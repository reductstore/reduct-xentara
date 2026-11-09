#!/usr/bin/env sh
set -eu

wait_for_reductstore() {
  attempt=0
  until curl --fail --silent --show-error -H 'Authorization: Bearer xentara-demo-token' http://127.0.0.1:8383/api/v1/info >/dev/null; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 50 ]; then
      exit 1
    fi
    sleep 0.1
  done
}

docker compose -f examples/compose.yaml up -d
wait_for_reductstore
ctest --test-dir build --output-on-failure -R reductstore-xentara-integration-tests

docker compose -f examples/compose.yaml stop
if ctest --test-dir build --output-on-failure -R reductstore-xentara-integration-tests; then
  exit 1
fi
docker compose -f examples/compose.yaml start
wait_for_reductstore
ctest --test-dir build --output-on-failure -R reductstore-xentara-integration-tests
