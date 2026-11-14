#!/usr/bin/env bash
# 演示排程 API 与日报批次/发布 API（需先启动服务：docker compose up 或 uvicorn）。
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
DAY="${DAY:-2026-11-01}"

section() {
  echo "================================================================"
  echo "$1"
  echo "----------------------------------------------------------------"
}

post() {
  local title="$1" path="$2" file="$3"
  section "${title}"
  echo "POST ${BASE_URL}${path}  <- ${file}"
  local tmp
  tmp="$(mktemp)"
  local code
  code=$(curl -sS -o "${tmp}" -w '%{http_code}' -D "${tmp}.headers" \
    -X POST "${BASE_URL}${path}" \
    -H "Content-Type: application/json" \
    --data-binary "@${file}")
  grep -i '^x-idempotent-replay:' "${tmp}.headers" || true
  echo "HTTP ${code}"
  python3 -m json.tool --no-ensure-ascii < "${tmp}"
  rm -f "${tmp}" "${tmp}.headers"
  echo
}

# ---- 既有契约排程 API -----------------------------------------------------
section "排程样例一：先升级订阅端才能新增缺陷等级（有安全顺序）"
curl -sS -X POST "${BASE_URL}/api/v1/plan" \
  -H "Content-Type: application/json" \
  --data-binary "@samples/safe_subscriber_first.json" | python3 -m json.tool --no-ensure-ascii
echo

section "排程样例二：新增枚举值撞上订阅端新必填字段（无解）"
curl -sS -X POST "${BASE_URL}/api/v1/plan" \
  -H "Content-Type: application/json" \
  --data-binary "@samples/infeasible_circular.json" | python3 -m json.tool --no-ensure-ascii
echo

# ---- 批次补传 -------------------------------------------------------------
post "补传第一批 3 条裂纹事件（整批入库）" \
  "/api/v1/events/batches" "samples/backfill_batch.json"

post "重投同一批（同号同内容，只计一次：inserted=0）" \
  "/api/v1/events/batches" "samples/backfill_batch.json"

# ---- 日报发布 -------------------------------------------------------------
post "首次发布巡测日 ${DAY} 日报（版本 1）" \
  "/api/v1/reports/publish" "samples/publish_retry.json"

post "重试同一发布请求号（返回原版，X-Idempotent-Replay: true）" \
  "/api/v1/reports/publish" "samples/publish_retry.json"

# ---- 补传后重发（可选，需后到批次样例） ------------------------------------
if [[ -f samples/backfill_later.json ]]; then
  post "补传后到的一条事件（第二批）" \
    "/api/v1/events/batches" "samples/backfill_later.json"

  post "补传后重发：新请求号发布版本 2，旧版仍可查" \
    "/api/v1/reports/publish" "samples/republish_after_backfill.json"

  section "复核：版本 1 与版本 2 同时可查"
  curl -sS "${BASE_URL}/api/v1/reports/${DAY}" | python3 -m json.tool --no-ensure-ascii
  echo
fi

# ---- 同号异内容整批拒绝 ----------------------------------------------------
if [[ -f samples/reingest_conflict.json ]]; then
  post "同号异内容：整批 409，且不留下任何本批记录" \
    "/api/v1/events/batches" "samples/reingest_conflict.json"
fi
