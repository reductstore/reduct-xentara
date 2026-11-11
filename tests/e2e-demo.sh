#!/usr/bin/env sh
set -eu

cleanup() {
  docker compose -f examples/compose.yaml down -v
}

wait_for_reductstore() {
  attempt=0
  until curl --fail --silent --show-error -H 'Authorization: Bearer xentara-demo-token' http://127.0.0.1:8383/api/v1/info >/dev/null; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 200 ]; then
      exit 1
    fi
    sleep 0.1
  done
}

trap cleanup EXIT
docker compose -f examples/compose.yaml up -d
wait_for_reductstore
set +e
timeout --signal=INT 8s xentara --plugin build/reductstore-xentara.json --model examples/model.json
status=$?
set -e
if [ "$status" -ne 124 ] && [ "$status" -ne 130 ]; then
  exit "$status"
fi
info=$(curl --fail --silent --show-error -H 'Authorization: Bearer xentara-demo-token' http://127.0.0.1:8383/api/v1/b/xentara-demo)
timestamp=$(printf '%s' "$info" | jq -er '.info.latest_record')
payload=$(curl --fail --silent --show-error -H 'Authorization: Bearer xentara-demo-token' "http://127.0.0.1:8383/api/v1/b/xentara-demo/xentara-demo/telemetry/snapshot?ts=$timestamp")
printf '%s' "$payload" | jq -e '
  (.measurements | keys == ["invertedSawTooth", "noise", "pulse", "sawTooth", "sine", "triangle"])
  and (.quality | keys == ["invertedSawTooth", "noise", "pulse", "sawTooth", "sine", "triangle"])
  and (.measurements.pulse | type == "boolean")
  and ([.measurements.sine, .measurements.triangle, .measurements.sawTooth, .measurements.invertedSawTooth, .measurements.noise] | all(type == "number"))
  and ([.quality[]] | all(. == "good"))
' >/dev/null
