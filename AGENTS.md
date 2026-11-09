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

## Demo

```sh
docker compose -f examples/compose.yaml up -d
xentara --plugin build/reductstore-xentara.json --model examples/model.json
```

Stop the service with `docker compose -f examples/compose.yaml down -v`.

## Tests

Core tests run without a server. Integration tests require the compose service and must verify the ReductStore records they write. Do not add tests that only inspect source text, forwarding, or mocks.
