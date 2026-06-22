#!/usr/bin/env bash
#
# Single-node HiXL remote-read validation for CANN 8.5.
#
# This script runs a writer/store process (A) and a remote-reader process (C)
# on one host. A and C use different FalconKV node IDs so C reads A's keys via
# ACCESS_REMOTE_RPC, then FalconKV uses HiXL READ for the data path.
#
# Usage:
#   HOST_IP=7.150.5.81 CANN_ROOT=/usr/local/Ascend/cann-8.5.0 \
#     bash tests/perf/run_perf_hixl_single_node.sh
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BUILD_DIR="${PROJECT_ROOT}/build"
FALCONKV_SCHED="${BUILD_DIR}/src/scheduler/falconkv_sched"

HOST_IP="${HOST_IP:-7.150.5.81}"
CANN_ROOT="${CANN_ROOT:-/usr/local/Ascend/cann-8.5.0}"
WORK_DIR="${WORK_DIR:-/tmp/falconkv_hixl_verify}"
META_PORT="${META_PORT:-19900}"

A_STORE_DEVICE="${A_STORE_DEVICE:-6}"
A_CLIENT_DEVICE="${A_CLIENT_DEVICE:-6}"
C_STORE_DEVICE="${C_STORE_DEVICE:-4}"
C_CLIENT_DEVICE="${C_CLIENT_DEVICE:-4}"

A_STORE_HIXL="${A_STORE_HIXL:-${HOST_IP}:16000}"
A_CLIENT_HIXL="${A_CLIENT_HIXL:-${HOST_IP}:17001}"
C_STORE_HIXL="${C_STORE_HIXL:-${HOST_IP}:16001}"
C_CLIENT_HIXL="${C_CLIENT_HIXL:-${HOST_IP}:17000}"

BATCH_SIZE="${BATCH_SIZE:-128}"
VALUE_SIZE="${VALUE_SIZE:-1048576}"
WARMUP_SEC="${WARMUP_SEC:-20}"
WARMUP_BATCHES="${WARMUP_BATCHES:-400}"
WARMUP_WAIT_SEC="${WARMUP_WAIT_SEC:-300}"
DURATION_SEC="${DURATION_SEC:-20}"
WRITER_HOLD_AFTER_SEC="${WRITER_HOLD_AFTER_SEC:-60}"
CAPACITY_GB="${CAPACITY_GB:-64}"
CACHE_CAPACITY="${CACHE_CAPACITY:-100000}"
IO_THREADS="${IO_THREADS:-8}"
IO_URING_QUEUE_DEPTH="${IO_URING_QUEUE_DEPTH:-256}"
STORE_POOL_SIZE="${STORE_POOL_SIZE:-4}"
MAX_BODY_SIZE_MB="${MAX_BODY_SIZE_MB:-128}"
REMOTE_READ_CHUNK_SIZE_MB="${REMOTE_READ_CHUNK_SIZE_MB:-16}"
REMOTE_READ_PREFETCH_CHUNKS="${REMOTE_READ_PREFETCH_CHUNKS:-4}"
REMOTE_READ_QUEUE_CHUNKS="${REMOTE_READ_QUEUE_CHUNKS:-4}"
SCHEDULER_ENABLED="${SCHEDULER_ENABLED:-true}"
SCHEDULE_POLICY="${SCHEDULE_POLICY:-passthrough}"
SCHEDULER_UDS="${SCHEDULER_UDS:-${WORK_DIR}/falconkv_perf_sched.sock}"
HIXL_STAGING_CHUNK_SIZE_MB="${HIXL_STAGING_CHUNK_SIZE_MB:-1}"
HIXL_STAGING_CHUNK_COUNT="${HIXL_STAGING_CHUNK_COUNT:-256}"
HIXL_RECEIVE_CHUNK_SIZE_MB="${HIXL_RECEIVE_CHUNK_SIZE_MB:-1}"
HIXL_RECEIVE_CHUNK_COUNT="${HIXL_RECEIVE_CHUNK_COUNT:-256}"
BUILD_IF_MISSING="${BUILD_IF_MISSING:-1}"
FORCE_BUILD="${FORCE_BUILD:-0}"

case "${SCHEDULER_ENABLED}" in
    true|True|1) SCHEDULER_ENABLED="true" ;;
    *) SCHEDULER_ENABLED="false" ;;
esac

META_PID=""
SCHED_PID=""
A_PID=""
C_PID=""
RUN_CLIENT_PID=""

log() {
    printf '[hixl-verify] %s\n' "$*"
}

die() {
    printf '[hixl-verify] ERROR: %s\n' "$*" >&2
    exit 1
}

cleanup() {
    set +e
    if [[ -n "${C_PID}" ]] && kill -0 "${C_PID}" 2>/dev/null; then
        kill "${C_PID}" 2>/dev/null || true
    fi
    if [[ -n "${A_PID}" ]] && kill -0 "${A_PID}" 2>/dev/null; then
        kill "${A_PID}" 2>/dev/null || true
    fi
    if [[ -n "${META_PID}" ]] && kill -0 "${META_PID}" 2>/dev/null; then
        kill "${META_PID}" 2>/dev/null || true
    fi
    if [[ -n "${SCHED_PID}" ]] && kill -0 "${SCHED_PID}" 2>/dev/null; then
        kill "${SCHED_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT

wait_for_port() {
    local host="$1"
    local port="$2"
    local timeout_sec="${3:-15}"
    local deadline=$(( $(date +%s) + timeout_sec ))
    while [[ "$(date +%s)" -lt "${deadline}" ]]; do
        if python3 - "${host}" "${port}" <<'PY' >/dev/null 2>&1
import socket
import sys
s = socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=1)
s.close()
PY
        then
            return 0
        fi
        sleep 0.3
    done
    return 1
}

wait_for_file() {
    local path="$1"
    local timeout_sec="${2:-30}"
    local deadline=$(( $(date +%s) + timeout_sec ))
    while [[ "$(date +%s)" -lt "${deadline}" ]]; do
        if [[ -e "${path}" ]]; then
            return 0
        fi
        if [[ -n "${A_PID}" ]] && ! kill -0 "${A_PID}" 2>/dev/null; then
            return 1
        fi
        sleep 1
    done
    return 1
}

pick_ascend_hal() {
    local candidates=(
        "${CANN_ROOT}/aarch64-linux/devlib/linux/aarch64/libascend_hal.so"
        "${CANN_ROOT}/aarch64-linux/devlib/libascend_hal.so"
        "${CANN_ROOT}/aarch64-linux/lib64/device/lib64/libascend_hal.so"
        "/usr/local/Ascend/driver/lib64/driver/libascend_hal.so"
    )
    local path
    for path in "${candidates[@]}"; do
        if [[ -f "${path}" ]]; then
            printf '%s\n' "${path}"
            return 0
        fi
    done
    return 1
}

build_if_needed() {
    local master_bin="${BUILD_DIR}/src/meta/falconkv_master"
    if [[ "${FORCE_BUILD}" != "1" && -x "${master_bin}" ]]; then
        log "Using existing build at ${BUILD_DIR}"
        return 0
    fi
    if [[ "${BUILD_IF_MISSING}" != "1" && "${FORCE_BUILD}" != "1" ]]; then
        die "${master_bin} not found. Set BUILD_IF_MISSING=1 or build manually."
    fi

    local ascend_hal
    ascend_hal="$(pick_ascend_hal)" || die "libascend_hal.so not found under ${CANN_ROOT} or driver path"

    log "Building FalconKV with HiXL support"
    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}"
    cmake -S "${PROJECT_ROOT}" -B "${BUILD_DIR}" -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DFALCONKV_WITH_HIXL=ON \
        -DFALCONKV_BUILD_PYTHON=ON \
        -DHIXL_INCLUDE_DIR="${CANN_ROOT}/aarch64-linux/include" \
        -DHIXL_LIBRARY="${CANN_ROOT}/aarch64-linux/lib64/libcann_hixl.so" \
        -DACL_RT_LIBRARY="${CANN_ROOT}/aarch64-linux/lib64/libacl_rt.so" \
        -DASCEND_HAL_LIBRARY="${ascend_hal}" \
        -DMETADEF_LIBRARY="${CANN_ROOT}/aarch64-linux/lib64/libmetadef.so"
    cmake --build "${BUILD_DIR}" -j"$(nproc)"
}

start_meta() {
    cat > "${WORK_DIR}/meta_config.json" <<EOF
{
  "common": {
    "meta_addr": "0.0.0.0:${META_PORT}",
    "log_dir": "${WORK_DIR}/logs"
  },
  "meta": {
    "shard_count": 16
  }
}
EOF

    log "Starting Meta on 0.0.0.0:${META_PORT}"
    "${BUILD_DIR}/src/meta/falconkv_master" "${WORK_DIR}/meta_config.json" \
        > "${WORK_DIR}/logs/meta_stdout.log" 2>&1 &
    META_PID=$!
    if ! wait_for_port "127.0.0.1" "${META_PORT}" 15; then
        cat "${WORK_DIR}/logs/meta_stdout.log" >&2 || true
        cat "${WORK_DIR}"/logs/falconkv_master_*.log >&2 2>/dev/null || true
        die "Meta did not start on port ${META_PORT}"
    fi
}

start_scheduler() {
    if [[ "${SCHEDULER_ENABLED}" != "true" ]]; then
        log "Scheduler disabled"
        return 0
    fi
    [[ -x "${FALCONKV_SCHED}" ]] || die "Scheduler binary not found: ${FALCONKV_SCHED}"
    rm -f "${SCHEDULER_UDS}"
    cat > "${WORK_DIR}/sched_config.json" <<EOF
{
  "common": {
    "log_dir": "${WORK_DIR}/logs",
    "scheduler_uds_path": "${SCHEDULER_UDS}"
  },
  "scheduler": {
    "schedule_policy": "${SCHEDULE_POLICY}",
    "stats_report_interval_sec": 2
  }
}
EOF

    log "Starting Scheduler on ${SCHEDULER_UDS}"
    "${FALCONKV_SCHED}" "${WORK_DIR}/sched_config.json" \
        > "${WORK_DIR}/logs/sched_stdout.log" 2>&1 &
    SCHED_PID=$!
    if ! wait_for_file "${SCHEDULER_UDS}" 15; then
        cat "${WORK_DIR}/logs/sched_stdout.log" >&2 || true
        cat "${WORK_DIR}"/logs/falconkv_sched_*.log >&2 2>/dev/null || true
        die "Scheduler did not start at ${SCHEDULER_UDS}"
    fi
}

write_perf_config() {
    cat > "${WORK_DIR}/perf_config_hixl.json" <<EOF
{
  "test": {
    "duration_sec": ${DURATION_SEC},
    "batch_size": ${BATCH_SIZE},
    "value_size": ${VALUE_SIZE},
    "warmup_sec": ${WARMUP_SEC},
    "warmup_batches": ${WARMUP_BATCHES},
    "meta_listen_port": ${META_PORT},
    "scheduler_uds_path": "${SCHEDULER_UDS}",
    "result_dir": "${WORK_DIR}/result",
    "writer_warmup_only": true,
    "writer_hold_after_sec": ${WRITER_HOLD_AFTER_SEC}
  },
  "meta": { "shard_count": 16 },
  "scheduler": { "schedule_policy": "${SCHEDULE_POLICY}", "enabled": ${SCHEDULER_ENABLED} },
  "clients": [
    {
      "client_id": "A",
      "node_id": 1,
      "store_id": 1,
      "listen_port": 18901,
      "ssd_path": "${WORK_DIR}/ssd/A",
      "capacity_gb": ${CAPACITY_GB},
      "cache_capacity": ${CACHE_CAPACITY},
      "io_threads": ${IO_THREADS},
      "store_rpc_host": "127.0.0.1",
      "slot_size_bytes": 0,
      "io_uring_enabled": false,
      "io_uring_queue_depth": ${IO_URING_QUEUE_DEPTH},
      "store_hixl_engine_addr": "${A_STORE_HIXL}",
      "store_hixl_device_id": ${A_STORE_DEVICE},
      "client_hixl_engine_addr": "${A_CLIENT_HIXL}",
      "client_hixl_device_id": ${A_CLIENT_DEVICE}
    },
    {
      "client_id": "C",
      "node_id": 2,
      "store_id": 3,
      "listen_port": 18923,
      "ssd_path": "${WORK_DIR}/ssd/C",
      "capacity_gb": ${CAPACITY_GB},
      "cache_capacity": ${CACHE_CAPACITY},
      "io_threads": ${IO_THREADS},
      "store_rpc_host": "127.0.0.1",
      "slot_size_bytes": 0,
      "io_uring_enabled": false,
      "io_uring_queue_depth": ${IO_URING_QUEUE_DEPTH},
      "store_hixl_engine_addr": "${C_STORE_HIXL}",
      "store_hixl_device_id": ${C_STORE_DEVICE},
      "client_hixl_engine_addr": "${C_CLIENT_HIXL}",
      "client_hixl_device_id": ${C_CLIENT_DEVICE}
    }
  ],
  "transfer": {
    "meta_addr": "127.0.0.1:${META_PORT}",
    "store_pool_size": ${STORE_POOL_SIZE},
    "rpc_timeout_ms": 5000,
    "connect_timeout_ms": 3000,
    "max_retry": 3,
    "max_body_size_mb": ${MAX_BODY_SIZE_MB},
    "remote_read_stream_enabled": true,
    "remote_read_chunk_size_mb": ${REMOTE_READ_CHUNK_SIZE_MB},
    "remote_read_prefetch_chunks": ${REMOTE_READ_PREFETCH_CHUNKS},
    "remote_read_queue_chunks": ${REMOTE_READ_QUEUE_CHUNKS},
    "remote_read_transport": "hixl",
    "hixl_protocol_desc": "",
    "hixl_local_comm_res": "",
    "hixl_mem_type": "host",
    "hixl_staging_chunk_size_mb": ${HIXL_STAGING_CHUNK_SIZE_MB},
    "hixl_staging_chunk_count": ${HIXL_STAGING_CHUNK_COUNT},
    "hixl_receive_chunk_size_mb": ${HIXL_RECEIVE_CHUNK_SIZE_MB},
    "hixl_receive_chunk_count": ${HIXL_RECEIVE_CHUNK_COUNT},
    "hixl_transfer_timeout_ms": 5000,
    "hixl_connect_timeout_ms": 5000,
    "hixl_min_read_size_bytes": 1,
    "hixl_fallback_to_brpc": false
  }
}
EOF
}

run_client() {
    local client_id="$1"
    local store_hixl="$2"
    local store_device="$3"
    local client_hixl="$4"
    local client_device="$5"
    local log_file="${WORK_DIR}/logs/client_${client_id}.log"

    env \
        PYTHONPATH="${PROJECT_ROOT}/python:${PYTHONPATH:-}" \
        FALCONKV_REMOTE_READ_TRANSPORT=hixl \
        FALCONKV_STORE_HIXL_ENGINE_ADDR="${store_hixl}" \
        FALCONKV_STORE_HIXL_DEVICE_ID="${store_device}" \
        FALCONKV_CLIENT_HIXL_ENGINE_ADDR="${client_hixl}" \
        FALCONKV_CLIENT_HIXL_DEVICE_ID="${client_device}" \
        FALCONKV_HIXL_PROTOCOL_DESC= \
        FALCONKV_HIXL_LOCAL_COMM_RES= \
        FALCONKV_HIXL_MEM_TYPE=host \
        FALCONKV_HIXL_STAGING_CHUNK_SIZE_MB="${HIXL_STAGING_CHUNK_SIZE_MB}" \
        FALCONKV_HIXL_STAGING_CHUNK_COUNT="${HIXL_STAGING_CHUNK_COUNT}" \
        FALCONKV_HIXL_RECEIVE_CHUNK_SIZE_MB="${HIXL_RECEIVE_CHUNK_SIZE_MB}" \
        FALCONKV_HIXL_RECEIVE_CHUNK_COUNT="${HIXL_RECEIVE_CHUNK_COUNT}" \
        FALCONKV_HIXL_MIN_READ_SIZE_BYTES=1 \
        FALCONKV_HIXL_FALLBACK_TO_BRPC=0 \
        FALCONKV_HIXL_CONNECT_TIMEOUT_MS=5000 \
        FALCONKV_HIXL_TRANSFER_TIMEOUT_MS=5000 \
        python3 -u "${SCRIPT_DIR}/perf_client.py" \
            --config "${WORK_DIR}/perf_config_hixl.json" \
            --client-id "${client_id}" \
            > "${log_file}" 2>&1 &
    RUN_CLIENT_PID=$!
}

grep_log_files() {
    local pattern="$1"
    local files=()
    local file
    for file in "${WORK_DIR}/logs"/*.log "${WORK_DIR}/ssd/A"/*.log "${WORK_DIR}/ssd/C"/*.log; do
        if [[ -e "${file}" ]]; then
            files+=("${file}")
        fi
    done
    if [[ "${#files[@]}" -eq 0 ]]; then
        return 1
    fi
    grep -E "${pattern}" "${files[@]}"
}

check_result() {
    local result_c="${WORK_DIR}/result/result_C.json"
    [[ -f "${result_c}" ]] || die "Missing ${result_c}"

    python3 - "${result_c}" <<'PY'
import json
import sys
path = sys.argv[1]
with open(path) as f:
    r = json.load(f)
errors = int(r.get("errors", -1))
hits = int(r.get("get_hit_count", 0))
gets = int(r.get("get", {}).get("total_ops", 0))
if errors != 0 or hits <= 0 or gets <= 0:
    print(json.dumps(r, indent=2))
    raise SystemExit(f"validation failed: errors={errors}, get_hit_count={hits}, get_ops={gets}")
print(f"validation result ok: errors={errors}, get_hit_count={hits}, get_ops={gets}")
PY

    if grep_log_files "falling back to brpc" >/dev/null 2>&1; then
        grep_log_files "falling back to brpc" || true
        die "HiXL fell back to brpc"
    fi
    if grep_log_files "HiXL remote read disabled|HiXL RegisterMem failed" >/dev/null 2>&1; then
        grep_log_files "HiXL remote read disabled|HiXL RegisterMem failed" || true
        die "HiXL remote read was disabled"
    fi

    if ! grep_log_files "HiXL|Hixl|PrepareHixl|TransferSync" >/dev/null 2>&1; then
        die "No HiXL-related log lines found"
    fi
    if ! grep_log_files "HiXL remote read enabled, engine=${A_STORE_HIXL}" >/dev/null 2>&1; then
        die "Writer store HiXL remote read was not enabled"
    fi
    if ! grep_log_files "Initialized local_engine=${C_CLIENT_HIXL}" >/dev/null 2>&1; then
        die "Reader client HiXL transport was not initialized"
    fi
    if ! grep_log_files "HiXL BatchRead succeeded|PrepareHixlBatchRead|HiXL TransferSync READ" >/dev/null 2>&1; then
        die "Reader did not execute a HiXL batch read"
    fi
}

print_summary() {
    log "Result C:"
    cat "${WORK_DIR}/result/result_C.json" || true
    log "HiXL-related log lines:"
    grep_log_files "HiXL|Hixl|fallback|PrepareHixl|TransferSync|ERROR|WARNING" 2>/dev/null || true
}

main() {
    [[ -f "${CANN_ROOT}/set_env.sh" ]] || die "CANN set_env.sh not found: ${CANN_ROOT}/set_env.sh"
    # shellcheck disable=SC1090
    source "${CANN_ROOT}/set_env.sh"

    log "Project: ${PROJECT_ROOT}"
    log "Work dir: ${WORK_DIR}"
    log "Host IP: ${HOST_IP}"
    log "CANN root: ${CANN_ROOT}"
    log "Perf params: batch=${BATCH_SIZE}, value=${VALUE_SIZE}, warmup_batches=${WARMUP_BATCHES}, warmup_wait=${WARMUP_WAIT_SEC}s, duration=${DURATION_SEC}s"
    log "HiXL pools: staging=${HIXL_STAGING_CHUNK_COUNT}x${HIXL_STAGING_CHUNK_SIZE_MB}MB, receive=${HIXL_RECEIVE_CHUNK_COUNT}x${HIXL_RECEIVE_CHUNK_SIZE_MB}MB"

    build_if_needed

    log "Cleaning old validation state"
    pkill -f "${BUILD_DIR}/src/meta/falconkv_master" 2>/dev/null || true
    pkill -f "${FALCONKV_SCHED}" 2>/dev/null || true
    pkill -f "${SCRIPT_DIR}/perf_client.py.*perf_config_hixl.json" 2>/dev/null || true
    rm -rf "${WORK_DIR}"
    mkdir -p "${WORK_DIR}/logs" "${WORK_DIR}/result" "${WORK_DIR}/ssd/A" "${WORK_DIR}/ssd/C"

    start_meta
    start_scheduler
    write_perf_config

    log "Starting writer A: store=${A_STORE_HIXL} device=${A_STORE_DEVICE}, client=${A_CLIENT_HIXL} device=${A_CLIENT_DEVICE}"
    run_client A "${A_STORE_HIXL}" "${A_STORE_DEVICE}" "${A_CLIENT_HIXL}" "${A_CLIENT_DEVICE}"
    A_PID="${RUN_CLIENT_PID}"

    local marker="${WORK_DIR}/ssd/A/.warmup_batch_count"
    if ! wait_for_file "${marker}" "${WARMUP_WAIT_SEC}"; then
        cat "${WORK_DIR}/logs/client_A.log" >&2 || true
        die "Writer A did not complete warmup"
    fi
    log "A warmup batches: $(cat "${marker}")"

    log "Starting remote reader C: store=${C_STORE_HIXL} device=${C_STORE_DEVICE}, client=${C_CLIENT_HIXL} device=${C_CLIENT_DEVICE}"
    run_client C "${C_STORE_HIXL}" "${C_STORE_DEVICE}" "${C_CLIENT_HIXL}" "${C_CLIENT_DEVICE}"
    C_PID="${RUN_CLIENT_PID}"

    if ! wait "${C_PID}"; then
        cat "${WORK_DIR}/logs/client_C.log" >&2 || true
        die "Client C failed"
    fi
    if ! wait "${A_PID}"; then
        cat "${WORK_DIR}/logs/client_A.log" >&2 || true
        die "Client A failed"
    fi

    check_result
    print_summary
    log "SUCCESS: FalconKV single-node HiXL remote read validation passed"
}

main "$@"
