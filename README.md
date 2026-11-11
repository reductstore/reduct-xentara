# ReductStore Skill for Xentara

C++20 plugin for Xentara 2.1.1. It writes scheduled scalar snapshots to [ReductStore](https://www.reduct.store/).

| Supported now | Not supported yet |
| --- | --- |
| Boolean, signed/unsigned integer, and floating-point measurements | Images, arbitrary binary data, and waveform buffers |
| JSON snapshots with quality and optional source timestamps | Image and waveform contracts are tracked separately |

Derived from `xentara/xentara-template-uplink-batch`. Original Boost Software License notices remain in the source.

## Build and run

| Requirement | Version or use |
| --- | --- |
| Xentara Runtime and development package | 2.1.1 |
| Build tools | CMake 3.25+, C++20 compiler, OpenSSL headers |
| Demo | Docker Compose and a signed `ReductStore` Xentara entitlement |

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DREDUCTSTORE_XENTARA_ENABLE_INTEGRATION_TESTS=ON
cmake --build build --parallel
ctest --test-dir build --output-on-failure
xentara --plugin build/reductstore-xentara.json --model examples/model.json
```

The manifest loads the plugin directly from `build`. The ReductStore C++ SDK is pinned to v1.21.3.

## Configuration

`@Skill.ReductStore.Client` owns one background uploader. `serverUrl` and `bucket` are required.

| Client field | Default | Purpose |
| --- | ---: | --- |
| `serverUrl` | required | ReductStore URL |
| `apiToken` | empty | ReductStore API token |
| `bucket` | required | Target bucket |
| `entryPrefix` | empty | Prefix for every entry. Use one per device. |
| `host` | `auto` | `host` label value |
| `createBucket` | `false` | Create the bucket if missing |
| `connectionTimeout` / `requestTimeout` / `shutdownTimeout` | 2s / 10s / 15s | HTTP and final-flush limits |
| `maxBatchRecords` | 64 | Records per request |
| `maxBatchPayloadBytes` / `maxBatchMetadataBytes` | 4 MiB / 6 KiB | Request size limits |
| `maxBatchAge` | 2s | Maximum time before a flush |
| `maxQueueBytes` | 16 MiB | Memory limit for pending records |
| `overflowPolicy` | `rejectNewest` | `rejectNewest` or `dropOldest` |
| `retry.maxAttempts` / `initialBackoff` / `maximumBackoff` | 3 / 100ms / 2s | Retry cycle and bounded backoff |

`@Skill.ReductStore.Writer` defines one or more `recordGroups`.

| Writer field | Required | Purpose |
| --- | --- | --- |
| `mode` | yes | `snapshot` |
| `entry` | yes | Relative ReductStore entry name |
| `timestampSource` | yes | `scheduled` |
| `measurements` | yes | JSON object from field name to Xentara data point |
| `labels` | no | Extra fixed labels. `host` and `quality` are reserved. |

See [`examples/model.json`](examples/model.json) for a complete model.

## How it works

| Part | Behavior |
| --- | --- |
| Collection | `collect` reads all fields in a snapshot. A missing or non-finite field skips the whole snapshot. |
| Record | Payload is compact JSON with `measurements`, `quality`, and optional `sourceTime` in Unix microseconds. Timestamp is the scheduled pipeline time. |
| Delivery | Collection only enqueues. A background worker batches records for ReductStore. |
| Identity | ReductStore identity is entry plus microsecond timestamp. A repeated timestamp is skipped before delivery. |
| Reliability | Transient HTTP failures retry with bounded backoff. Permanent failures, queue overflow, and shutdown expiry are counted and reported. |
| Events | `sent` reports new delivery. `sendError` reports new collection or delivery failure. Both may occur in one update. |
| Status | Client exposes connection, queue, and total delivery state. Writer exposes its own delivery, failure, and collection counters. |

## Local demo

The demo needs a [Xentara license with the ReductStore skill](https://xentara.io/licensing/).

`docker compose` starts only the local ReductStore server. Build the plugin first with the commands above. For this Ubuntu setup, Xentara reads its license from `/usr/local/share/xentara/licence/licences.json`; install the signed license there without adding it to this repository.

```sh
docker compose -f examples/compose.yaml up -d
xentara --plugin build/reductstore-xentara.json --model examples/model.json
```

Open <http://127.0.0.1:8383> to see the `xentara-demo` bucket and its snapshots. The demo token is `xentara-demo-token`.

```sh
docker compose -f examples/compose.yaml down -v
```

## Tests

| Command | What it checks |
| --- | --- |
| `ctest --test-dir build --output-on-failure` | Core behavior and ReductStore integration tests |
| `tests/integration-recovery.sh` | Queue recovery after a real ReductStore restart |
| `tests/e2e-demo.sh` | Licensed Xentara model and six-signal snapshot. Uses an isolated temporary container and port. |

## License

Project code is licensed under the Boost Software License 1.0. See [`LICENSE`](LICENSE). ReductStore C++ SDK and Xentara have their own licenses.
