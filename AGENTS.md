# Development

Use C++20. Keep the plugin API compatible with Xentara 2.1.1.

## Build

Install `cmake`, a C++20 compiler, OpenSSL development headers, and `xentara-dev` 2.1.1. Configure with:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

`CMakeLists.txt` pins ReductStore C++ SDK v1.21.3 by commit. Do not change it without updating the documentation and integration coverage.

## Local service

`examples/compose.yaml` starts the local ReductStore instance used by both the manual example and integration tests:

```sh
docker compose -f examples/compose.yaml up -d
```

## Manual example

`examples/model.json` is the user-facing Signal Generator example. Run it only when the installed Xentara licence has the `ReductStore` skill entitlement:

```sh
xentara --plugin build/reductstore-xentara.json --model examples/model.json
```

## Tests

Core tests require no server. Configure with `-DREDUCTSTORE_XENTARA_ENABLE_INTEGRATION_TESTS=ON` to build integration tests, then run them against the local compose service. Integration tests must verify the ReductStore records they write. Do not add tests that only inspect source text, forwarding, or mocks.

Stop the local service and remove its data with `docker compose -f examples/compose.yaml down -v`.
