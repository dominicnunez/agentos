#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: run-loopback-pilot.sh <agentos> <agentos-recovery> <new-work-directory>" >&2
  exit 2
fi

agentos_binary=$(realpath -e -- "$1")
recovery_binary=$(realpath -e -- "$2")
work_directory=$3
script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)

if [ ! -x "$agentos_binary" ] || [ ! -x "$recovery_binary" ]; then
  echo "pilot binaries must exist and be executable" >&2
  exit 2
fi
if [ -e "$work_directory" ]; then
  echo "pilot work directory must not already exist: $work_directory" >&2
  exit 2
fi
mkdir -p -- "$work_directory"
work_directory=$(realpath -e -- "$work_directory")

required_variables=(
  AGENTOS_PILOT_AGENT_TOKEN
  AGENTOS_PILOT_READER_TOKEN
  AGENTOS_PILOT_REVOKED_TOKEN
  AGENTOS_PILOT_EXPIRED_TOKEN
  AGENTOS_PILOT_HUMAN_TOKEN
  AGENTOS_PILOT_REVIEWER_TOKEN
  AGENTOS_PILOT_APPROVAL_TOKEN
)
for variable in "${required_variables[@]}"; do
  if [ -z "${!variable:-}" ]; then
    echo "$variable is required" >&2
    exit 2
  fi
done

agent_url=http://127.0.0.1:8081
control_url=http://127.0.0.1:8082
seed_registry="$script_directory/testdata/pilot-a2a-seed.json"
restart_registry="$script_directory/testdata/pilot-a2a-restart.json"
human_registry="$script_directory/testdata/pilot-human.json"
approval_registry="$script_directory/testdata/pilot-approval.json"
state_file="$work_directory/state.json"
server_log="$work_directory/server.log"
pilot_pid=""

stop_pilot() {
  if [ -n "$pilot_pid" ]; then
    kill -TERM "$pilot_pid" 2>/dev/null || true
    wait "$pilot_pid" 2>/dev/null || true
    pilot_pid=""
  fi
}

start_pilot() {
  local database=$1
  local a2a_registry=$2
  : >"$server_log"
  AGENTOS_DB="$database" \
  AGENTOS_A2A_ACTORS_FILE="$a2a_registry" \
  AGENTOS_HUMAN_ACTORS_FILE="$human_registry" \
  AGENTOS_APPROVAL_ACTORS_FILE="$approval_registry" \
  AGENTOS_MODEL_PROVIDER=fake-review \
  AGENTOS_LISTEN_ADDR=127.0.0.1:8081 \
  AGENTOS_PUBLIC_URL="$agent_url" \
  AGENTOS_CONTROL_LISTEN_ADDR=127.0.0.1:8082 \
  "$agentos_binary" >"$server_log" 2>&1 &
  pilot_pid=$!
  for attempt in $(seq 1 30); do
    if ! kill -0 "$pilot_pid" 2>/dev/null; then
      cat "$server_log" >&2
      echo "pilot process exited before becoming ready" >&2
      exit 1
    fi
    control_status=$(curl --silent --output /dev/null --write-out '%{http_code}' \
      --header "Authorization: Bearer $AGENTOS_PILOT_APPROVAL_TOKEN" \
      "$control_url/v1/control/approvals/missing" || true)
    if curl --fail --silent "$agent_url/.well-known/agent-card.json" >/dev/null && \
      [ "$control_status" = "404" ]; then
      return
    fi
    if [ "$attempt" -eq 30 ]; then
      cat "$server_log" >&2
      echo "pilot process did not become ready" >&2
      exit 1
    fi
    sleep 1
  done
}

run_verifier() {
  python3 "$script_directory/verify-loopback-pilot.py" "$1" \
    --url "$agent_url" \
    --control-url "$control_url" \
    --state "$state_file" \
    --agent-token "$AGENTOS_PILOT_AGENT_TOKEN" \
    --reader-token "$AGENTOS_PILOT_READER_TOKEN" \
    --revoked-token "$AGENTOS_PILOT_REVOKED_TOKEN" \
    --expired-token "$AGENTOS_PILOT_EXPIRED_TOKEN" \
    --human-token "$AGENTOS_PILOT_HUMAN_TOKEN" \
    --reviewer-token "$AGENTOS_PILOT_REVIEWER_TOKEN" \
    --approval-token "$AGENTOS_PILOT_APPROVAL_TOKEN"
}

trap stop_pilot EXIT
start_pilot "$work_directory/live.db" "$seed_registry"
run_verifier seed
"$recovery_binary" backup \
  --database "$work_directory/live.db" \
  --output "$work_directory/backup.db" >"$work_directory/backup.json"
"$recovery_binary" verify \
  --database "$work_directory/backup.db" >"$work_directory/verified.json"
stop_pilot
"$recovery_binary" restore \
  --backup "$work_directory/backup.db" \
  --output "$work_directory/restored.db" >"$work_directory/restored.json"
start_pilot "$work_directory/restored.db" "$restart_registry"
run_verifier recover
stop_pilot

# A credential reused across work intake and approval control must make the
# packaged runtime fail before either listener opens.
set +e
(
  export AGENTOS_DB="$work_directory/overlap.db"
  export AGENTOS_A2A_ACTORS_FILE="$seed_registry"
  export AGENTOS_HUMAN_ACTORS_FILE="$human_registry"
  export AGENTOS_APPROVAL_ACTORS_FILE="$approval_registry"
  export AGENTOS_PILOT_APPROVAL_TOKEN="$AGENTOS_PILOT_HUMAN_TOKEN"
  export AGENTOS_LISTEN_ADDR=127.0.0.1:0
  timeout 10s "$agentos_binary" >"$work_directory/overlap.log" 2>&1
)
overlap_status=$?
set -e
if [ "$overlap_status" -eq 0 ] || [ "$overlap_status" -eq 124 ]; then
  cat "$work_directory/overlap.log" >&2
  echo "runtime did not fail closed on cross-channel credential reuse" >&2
  exit 1
fi
if ! grep --fixed-strings --quiet \
  "identities and credentials must be distinct" \
  "$work_directory/overlap.log"; then
  cat "$work_directory/overlap.log" >&2
  echo "runtime failed for the wrong credential-overlap reason" >&2
  exit 1
fi

printf '%s\n' '{"status":"PASS","pilot":"packaged-compatible-loopback"}'
