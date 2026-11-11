#!/usr/bin/env sh
set -eu

container="reductstore-xentara-e2e-$$"
model=$(mktemp)

cleanup() {
  docker rm --force "$container" >/dev/null 2>&1 || true
  rm -f "$model"
}

wait_for_reductstore() {
  attempt=0
  until curl --fail --silent --show-error -H 'Authorization: Bearer xentara-demo-token' "$server_url/api/v1/info" >/dev/null; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 200 ]; then
      exit 1
    fi
    sleep 0.1
  done
}
trap cleanup EXIT
docker run --detach --rm --name "$container" --publish 127.0.0.1::8383 --tmpfs /data:rw,size=256m,uid=10001,gid=10001,mode=770 \
  --env RS_API_TOKEN=xentara-demo-token reduct/store:v1.21.3 >/dev/null
port=$(docker inspect --format '{{(index (index .NetworkSettings.Ports "8383/tcp") 0).HostPort}}' "$container")
server_url="http://127.0.0.1:$port"
jq --arg server_url "$server_url" '(.children[] | select(has("@Skill.ReductStore.Client"))["@Skill.ReductStore.Client"].serverUrl) = $server_url' \
  examples/model.json > "$model"
wait_for_reductstore
set +e
timeout --signal=INT 8s xentara --plugin build/reductstore-xentara.json --model "$model"
status=$?
set -e
if [ "$status" -ne 124 ] && [ "$status" -ne 130 ]; then
  exit "$status"
fi
info=$(curl --fail --silent --show-error -H 'Authorization: Bearer xentara-demo-token' "$server_url/api/v1/b/xentara-demo")
timestamp=$(printf '%s' "$info" | jq -er '.info.latest_record')
payload=$(curl --fail --silent --show-error -H 'Authorization: Bearer xentara-demo-token' "$server_url/api/v1/b/xentara-demo/xentara-demo/telemetry/snapshot?ts=$timestamp")
printf '%s' "$payload" | jq -e '
  (.measurements | keys == ["invertedSawTooth", "noise", "pulse", "sawTooth", "sine", "triangle"])
  and (.quality | keys == ["invertedSawTooth", "noise", "pulse", "sawTooth", "sine", "triangle"])
  and (.measurements.pulse | type == "boolean")
  and ([.measurements.sine, .measurements.triangle, .measurements.sawTooth, .measurements.invertedSawTooth, .measurements.noise] | all(type == "number"))
  and ([.quality[]] | all(. == "good"))
' >/dev/null
