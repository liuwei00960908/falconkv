#!/usr/bin/env python3
"""Single-client performance worker for FalconKV end-to-end perf tests.

Usage:
    python perf_client.py --config perf_config.json --client-id A

Architecture:
    - Client A: writer + local reader   (ACCESS_LOCAL_DIRECT)
    - Client B: same-node reader         (ACCESS_NODE_DIRECT)
    - Client C: cross-node reader         (ACCESS_REMOTE_RPC)

Key design:
    - Warmup phase uses prefix ``warmup_A_{batch_idx}_{i}``.
      Client A writes continuously during warmup to populate data.
    - Benchmark phase uses prefix ``bench_A_{batch_idx}_{i}``.
      Client A writes genuinely new keys (exercising the write + eviction
      path) and reads back its own newly written data.
    - Clients B/C only read warmup-phase data (``warmup_A_*``), which is
      guaranteed to exist — no synchronization needed between clients.
    - capacity_gb in the config controls when LRU eviction kicks in.
      Use a small value (e.g. 1 GB) to stress-test the eviction path.
"""

import argparse
import ctypes
import json
import math
import os
import sys
import time


# ---------------------------------------------------------------------------
# Buffer helpers (DirectIO-aligned)
# ---------------------------------------------------------------------------

DIRECTIO_ALIGNMENT = 4096  # Must match store page_size


def _align_up(value: int, alignment: int) -> int:
    """Round *value* up to the nearest multiple of *alignment*."""
    return (value + alignment - 1) & ~(alignment - 1)


class BufferGuard:
    """Holds a DirectIO-aligned ctypes buffer initialised with *data*.

    Both the exposed pointer and size are aligned to ``DIRECTIO_ALIGNMENT``
    so that the buffer can be used directly with O_DIRECT I/O.
    """

    def __init__(self, data: bytes, alignment: int = DIRECTIO_ALIGNMENT):
        self.size = _align_up(len(data), alignment)
        # Over-allocate so we can find an aligned start address.
        self._buf = (ctypes.c_ubyte * (self.size + alignment))()
        raw = ctypes.addressof(self._buf)
        self.ptr = _align_up(raw, alignment)
        ctypes.memmove(self.ptr, data, len(data))


class ZeroBuffer:
    """DirectIO-aligned, zeroed buffer of the requested size.

    Both the exposed pointer and size are aligned to ``DIRECTIO_ALIGNMENT``
    so that the buffer can be used directly with O_DIRECT I/O.
    """

    def __init__(self, size: int, alignment: int = DIRECTIO_ALIGNMENT):
        self.size = _align_up(size, alignment)
        self._buf = (ctypes.c_ubyte * (self.size + alignment))()
        raw = ctypes.addressof(self._buf)
        self.ptr = _align_up(raw, alignment)


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _find_client_config(config: dict, client_id: str) -> dict:
    for c in config["clients"]:
        if c["client_id"] == client_id:
            return c
    raise ValueError(f"Client '{client_id}' not found in config")


def _write_falconkv_config(client_cfg: dict, test_cfg: dict, config: dict,
                           ssd_path: str) -> str:
    """Generate a FalconKV JSON config file and return its path."""
    meta_addr = config["transfer"]["meta_addr"]
    scheduler_enabled = config["scheduler"].get("enabled", False)
    scheduler_uds = test_cfg.get("scheduler_uds_path",
                                  "/tmp/falconkv_perf_sched.sock")

    falconkv_cfg = {
        "common": {
            "meta_addr": meta_addr,
            "node_id": client_cfg["node_id"],
            "scheduler_enabled": scheduler_enabled,
            "scheduler_uds_path": scheduler_uds,
            "log_dir": ssd_path,
        },
        "store": {
            "ssd_path": ssd_path,
            "store_id": client_cfg["store_id"],
            "capacity_gb": client_cfg.get("capacity_gb", 8),
            "page_size": 4096,
            "io_threads": client_cfg.get("io_threads", 2),
            "store_rpc_host": client_cfg.get("store_rpc_host", "127.0.0.1"),
            "listen_port": client_cfg["listen_port"],
            "evict_grace_period_ms": client_cfg.get("evict_grace_period_ms", 5000),
            "evict_check_interval_sec": client_cfg.get("evict_check_interval_sec", 60),
            "evict_high_watermark": client_cfg.get("evict_high_watermark", 0.85),
            "evict_low_watermark": client_cfg.get("evict_low_watermark", 0.70),
            "io_uring_enabled": client_cfg.get("io_uring_enabled", False),
            "io_uring_queue_depth": client_cfg.get("io_uring_queue_depth", 128),
            "slot_size_bytes": client_cfg.get("slot_size_bytes", 0),
        },
        "client": {
            "cache_capacity": client_cfg.get("cache_capacity", 100000),
        },
    }
    os.makedirs(ssd_path, exist_ok=True)
    config_path = os.path.join(ssd_path, "falconkv_config.json")
    with open(config_path, "w") as f:
        json.dump(falconkv_cfg, f, indent=2)
    return config_path


# ---------------------------------------------------------------------------
# Key generation
# ---------------------------------------------------------------------------

def _warmup_keys(batch_idx: int, batch_size: int) -> list:
    """Generate warmup-phase keys (``warmup_A_{batch_idx}_{i}``)."""
    return [f"warmup_A_{batch_idx}_{i}" for i in range(batch_size)]


def _bench_write_keys(batch_idx: int, batch_size: int) -> list:
    """Generate benchmark-phase write keys (``bench_A_{batch_idx}_{i}``).

    Used by Client A to produce genuinely new keys that exercise the write
    + LRU eviction path.
    """
    return [f"bench_A_{batch_idx}_{i}" for i in range(batch_size)]


def _bench_read_keys(batch_idx: int, batch_size: int) -> list:
    """Generate benchmark-phase read keys.

    - Writer A: reads its own just-written bench keys (lag by 1 batch).
    - Readers B/C: cycle through warmup-phase data (always exists).
    """
    raise NotImplementedError("use role-specific methods instead")


def _writer_read_keys(batch_idx: int, batch_size: int) -> list:
    """Writer A reads its own bench keys from the *previous* batch."""
    read_batch = max(0, batch_idx - 1)
    return [f"bench_A_{read_batch}_{i}" for i in range(batch_size)]


def _reader_read_keys(batch_idx: int, total_warmup_batches: int,
                      batch_size: int) -> list:
    """Readers B/C cycle through warmup data (always exists)."""
    warmup_batch = batch_idx % total_warmup_batches
    return [f"warmup_A_{warmup_batch}_{i}" for i in range(batch_size)]


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def _percentile(sorted_data: list, pct: float) -> float:
    if not sorted_data:
        return 0.0
    k = (len(sorted_data) - 1) * pct / 100.0
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_data[int(k)]
    d0 = sorted_data[int(f)] * (c - k)
    d1 = sorted_data[int(c)] * (k - f)
    return d0 + d1


def _compute_stats(latencies_ms: list, elapsed_sec: float,
                   total_bytes: int) -> dict:
    if not latencies_ms:
        return {
            "total_ops": 0,
            "avg_ms": 0.0,
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "p99_ms": 0.0,
            "min_ms": 0.0,
            "max_ms": 0.0,
            "throughput_ops": 0.0,
            "throughput_mb": 0.0,
        }
    s = sorted(latencies_ms)
    return {
        "total_ops": len(s),
        "avg_ms": round(sum(s) / len(s), 3),
        "p50_ms": round(_percentile(s, 50), 3),
        "p95_ms": round(_percentile(s, 95), 3),
        "p99_ms": round(_percentile(s, 99), 3),
        "min_ms": round(s[0], 3),
        "max_ms": round(s[-1], 3),
        "throughput_ops": round(len(s) / elapsed_sec, 1)
        if elapsed_sec > 0 else 0.0,
        "throughput_mb": round(total_bytes / (1024 * 1024) / elapsed_sec, 2)
        if elapsed_sec > 0 else 0.0,
    }


# ---------------------------------------------------------------------------
# Determine client role
# ---------------------------------------------------------------------------

def _get_role(client_id: str, client_cfg: dict, config: dict) -> str:
    """Return the benchmark role for this client."""
    if client_id == "A":
        return "writer"
    writer_cfg = _find_client_config(config, "A")
    if client_cfg["node_id"] == writer_cfg["node_id"]:
        return "same_node_reader"
    return "cross_node_reader"


# ---------------------------------------------------------------------------
# Main benchmark
# ---------------------------------------------------------------------------

def run_benchmark(config: dict, client_id: str):
    from pyfalconkv.client import Client

    test_cfg = config["test"]
    client_cfg = _find_client_config(config, client_id)

    ssd_path = client_cfg["ssd_path"]
    os.makedirs(ssd_path, exist_ok=True)

    config_path = _write_falconkv_config(client_cfg, test_cfg, config, ssd_path)
    client = Client(config_path,
                    cache_capacity=client_cfg.get("cache_capacity", 100000))

    batch_size = test_cfg.get("batch_size", 16)
    value_size = test_cfg.get("value_size", 4096)
    warmup_sec = test_cfg.get("warmup_sec", 10)
    duration_sec = test_cfg.get("duration_sec", 30)
    capacity_gb = client_cfg.get("capacity_gb", 8)
    writer_warmup_only = test_cfg.get("writer_warmup_only", False)
    writer_hold_after_sec = test_cfg.get("writer_hold_after_sec", 0)

    role = _get_role(client_id, client_cfg, config)

    print(f"[{client_id}] role={role}  batch_size={batch_size}  "
          f"value_size={value_size}  capacity_gb={capacity_gb}")

    # ---- Warmup phase -------------------------------------------------------
    # Writer A publishes warmup_batch_count via a marker file so readers
    # know how many warmup batches are available for cycling.
    writer_cfg = _find_client_config(config, "A")
    writer_ssd_path = writer_cfg["ssd_path"]
    warmup_marker = os.path.join(writer_ssd_path, ".warmup_batch_count")

    warmup_batch_count = 0

    if role == "writer":
        # Client A writes continuously to populate data for B/C to read.
        # Pre-allocate write buffer once, reuse for entire warmup + bench.
        print(f"[A] Warmup: writing initial data for {warmup_sec}s ...")
        write_data = os.urandom(value_size)
        write_guard = BufferGuard(write_data)
        warmup_end = time.time() + warmup_sec

        while time.time() < warmup_end:
            keys = _warmup_keys(warmup_batch_count, batch_size)
            client.batch_put_sync(
                keys,
                [write_guard.ptr] * len(keys),
                [write_guard.size] * len(keys),
            )
            warmup_batch_count += 1

        # Publish warmup batch count for readers.
        with open(warmup_marker, "w") as f:
            f.write(str(warmup_batch_count))
        print(f"[A] Warmup done ({warmup_batch_count} batches).")
    else:
        # B / C wait for A to finish populating data and publish count.
        print(f"[{client_id}] Warmup: waiting {warmup_sec}s "
              f"for Client A to write data ...")
        time.sleep(warmup_sec)

        # Read the warmup batch count written by A.
        for _ in range(30):  # retry for up to 15s
            if os.path.exists(warmup_marker):
                try:
                    with open(warmup_marker) as f:
                        warmup_batch_count = int(f.read().strip())
                    break
                except (ValueError, OSError):
                    pass
            time.sleep(0.5)
        if warmup_batch_count == 0:
            print(f"[{client_id}] WARNING: failed to read warmup batch count, "
                  f"using 1 to avoid division by zero")
            warmup_batch_count = 1
        print(f"[{client_id}] Warmup done (A wrote {warmup_batch_count} batches).")

    # Allow meta sync to propagate across clients.
    print(f"[{client_id}] Waiting for meta sync ...")
    time.sleep(2)

    # ---- Main benchmark -----------------------------------------------------
    exist_latencies = []
    put_latencies = []
    get_latencies = []
    put_bytes = 0
    get_bytes = 0
    errors = 0
    put_skip_count = 0   # batches where all keys already existed
    put_exec_count = 0   # batches where put was actually called
    get_hit_count = 0    # keys successfully read
    get_miss_count = 0   # keys not found / failed

    batch_idx = 0

    # Pre-allocate read buffer pool — reuse across all iterations.
    read_buffer_pool = [ZeroBuffer(value_size) for _ in range(batch_size)]

    print(f"[{client_id}] Running benchmark for {duration_sec}s ...")
    t_start = time.time()
    deadline = t_start + duration_sec

    while time.time() < deadline:
        if role == "writer" and writer_warmup_only:
            # Writer only does warmup; idle during benchmark to avoid
            # evicting warmup data that remote readers depend on.
            time.sleep(1)
            batch_idx += 1
            continue

        if role == "writer":
            # --- Writer A: write new bench keys, then read previous bench keys
            write_keys = _bench_write_keys(batch_idx, batch_size)

            # 1) exist check on write keys
            t0 = time.monotonic()
            try:
                hit_count = client.batch_exist_sync(write_keys)
            except Exception:
                errors += 1
                hit_count = 0
            t1 = time.monotonic()
            exist_latencies.append((t1 - t0) * 1000)

            # 2) batch_put new keys (reuse pre-allocated write_guard)
            if hit_count < len(write_keys):
                t0 = time.monotonic()
                try:
                    client.batch_put_sync(
                        write_keys,
                        [write_guard.ptr] * len(write_keys),
                        [write_guard.size] * len(write_keys),
                    )
                    put_bytes += value_size * (len(write_keys) - hit_count)
                    put_exec_count += 1
                except Exception:
                    errors += 1
                t1 = time.monotonic()
                put_latencies.append((t1 - t0) * 1000)
            else:
                put_skip_count += 1

            # 3) batch_get — read the previous batch (guaranteed to exist
            #    after the first iteration).
            if batch_idx > 0:
                read_keys = _writer_read_keys(batch_idx, batch_size)

                t0 = time.monotonic()
                try:
                    results = client.batch_get_sync(
                        read_keys,
                        [b.ptr for b in read_buffer_pool],
                        [b.size for b in read_buffer_pool],
                    )
                    for r in results:
                        if r > 0:
                            get_bytes += r
                            get_hit_count += 1
                        else:
                            get_miss_count += 1
                except Exception:
                    errors += 1
                t1 = time.monotonic()
                get_latencies.append((t1 - t0) * 1000)

        else:
            # --- Readers B/C: cycle through warmup data (always exists)
            read_keys = _reader_read_keys(batch_idx, warmup_batch_count,
                                          batch_size)

            # 1) exist check
            t0 = time.monotonic()
            try:
                hit_count = client.batch_exist_sync(read_keys)
            except Exception:
                errors += 1
                hit_count = 0
            t1 = time.monotonic()
            exist_latencies.append((t1 - t0) * 1000)

            # 2) batch_get — only when keys exist (warmup data may have been
            #    evicted by LRU under small capacity)
            if hit_count > 0:
                t0 = time.monotonic()
                try:
                    results = client.batch_get_sync(
                        read_keys,
                        [b.ptr for b in read_buffer_pool],
                        [b.size for b in read_buffer_pool],
                    )
                    for r in results:
                        if r > 0:
                            get_bytes += r
                            get_hit_count += 1
                        else:
                            get_miss_count += 1
                except Exception:
                    errors += 1
                t1 = time.monotonic()
                get_latencies.append((t1 - t0) * 1000)

        batch_idx += 1

    elapsed = time.time() - t_start

    # ---- Compute statistics --------------------------------------------------
    result = {
        "client_id": client_id,
        "role": role,
        "node_id": client_cfg["node_id"],
        "store_id": client_cfg["store_id"],
        "elapsed_sec": round(elapsed, 2),
        "config": {
            "batch_size": batch_size,
            "value_size": value_size,
        },
        "exist": _compute_stats(exist_latencies, elapsed, 0),
        "put": _compute_stats(put_latencies, elapsed, put_bytes),
        "get": _compute_stats(get_latencies, elapsed, get_bytes),
        "put_exec_count": put_exec_count,
        "put_skip_count": put_skip_count,
        "get_hit_count": get_hit_count,
        "get_miss_count": get_miss_count,
        "errors": errors,
        "total_batches": batch_idx,
    }

    result_dir = test_cfg.get("result_dir", "/tmp/falconkv_perf_result")
    os.makedirs(result_dir, exist_ok=True)
    result_path = os.path.join(result_dir, f"result_{client_id}.json")
    with open(result_path, "w") as f:
        json.dump(result, f, indent=2)

    print(f"[{client_id}] Done. {result['total_batches']} batches, "
          f"{errors} errors. Results -> {result_path}")
    if role == "writer":
        print(f"[A] put_exec={put_exec_count}  put_skip={put_skip_count}")
    print(f"[{client_id}] get_hit={get_hit_count}  get_miss={get_miss_count}")

    if role == "writer" and writer_warmup_only and writer_hold_after_sec > 0:
        print(f"[A] Holding store alive for {writer_hold_after_sec}s ...")
        time.sleep(writer_hold_after_sec)

    client.close()
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="FalconKV perf test single-client worker")
    parser.add_argument("--config", required=True,
                        help="Path to perf_config.json")
    parser.add_argument("--client-id", required=True,
                        help="Client identifier (e.g. A, B, C)")
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    result = run_benchmark(config, args.client_id)

    for op in ("exist", "put", "get"):
        s = result[op]
        print(f"  {op:5s}: {s['total_ops']:5d} ops  "
              f"avg={s['avg_ms']:7.3f}ms  "
              f"p99={s['p99_ms']:7.3f}ms  "
              f"ops/s={s['throughput_ops']:8.1f}")


if __name__ == "__main__":
    main()
