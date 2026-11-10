# ReductStore Skill for Xentara

C++20 Xentara 2.1.1 plugin for writing scalar measurements and scheduled snapshots to [ReductStore](https://www.reduct.store/). Derived from `xentara/xentara-template-uplink-batch`; original embedded ocean copyright and Boost Software License 1.0 notices remain in the template-derived source.

## Build and load

Requirements: Ubuntu 24.04 AMD64, Xentara Runtime and `xentara-dev` 2.1.1, CMake 3.25+, C++20 compiler, OpenSSL development headers, and Docker Compose for the demo.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build --parallel
ctest --test-dir build --output-on-failure
xentara --plugin build/reductstore-xentara.json --model examples/model.json
```

The generated manifest loads the plugin from the build directory. To install it elsewhere, run `cmake --install build --prefix "$HOME/.local"` and pass `--plugin-dir "$HOME/.local/share/xentara/plugins"`.

`reduct-cpp` is pinned to v1.21.3 commit `6bddc6ac7d428f8c9313967ca87865b2a243ab02`. The demo uses `reduct/store:v1.21.3`.

## Configuration

`@Skill.ReductStore.Client` owns the asynchronous uploader. `serverUrl` and `bucket` are required.

| Field | Default | Meaning |
| --- | --- | --- |
| `apiToken` | empty | ReductStore API token |
| `entryPrefix` | empty | Optional relative prefix for every entry |
| `host` | `auto` | Automatic hostname or explicit `host` label |
| `connectionTimeout` / `requestTimeout` | `2s` / `10s` | HTTP timeouts |
| `createBucket` | `false` | Create a missing bucket |
| `maxBatchRecords` / `maxBatchPayloadBytes` / `maxBatchMetadataBytes` | `64` / `4194304` / `6144` | Batch limits |
| `maxBatchAge` / `maxQueueBytes` | `2s` / `16777216` | Flush age and bounded queue budget |
| `overflowPolicy` | `rejectNewest` | `rejectNewest` or `dropOldest` |
| `retry` | `3`, `100ms`, `2s` | Attempts, initial backoff, maximum backoff |

`@Skill.ReductStore.Writer` accepts `recordGroups`. This release supports `mode: "snapshot"` only. A snapshot requires an entry, `timestampSource: "scheduled"`, and a `measurements` map. It writes one `application/json` record:

```json
{"measurements":{"temperature":42.5,"pressure":1.03},"sourceTime":{"temperature":1794000000000000},"quality":{"temperature":"Good","pressure":"Good"}}
```

The record timestamp is the scheduled pipeline time. `sourceTime` is optional and contains Unix microseconds only for measurements whose Xentara data point supplies it. The writer adds `host`, stores standard Xentara quality per field, and writes the worst quality as the record `quality` label. It accepts Boolean, signed integer, unsigned integer, and floating-point values. An unreadable value produces no partial snapshot. `collectionFailures` and `duplicateSnapshots` are exposed as attributes.

## Delivery

Snapshot record timestamps are normalized to ReductStore microseconds before duplicate detection and delivery. Record identity is `entry + timestamp`. Per-measurement Xentara source times remain in the optional JSON `sourceTime` object.

Collection only reads, serializes, and enqueues. A worker performs HTTP I/O, batching up to 64 records, 4 MiB payload, 6 KiB metadata, or two seconds. Queue limits include queued and in-flight records. Transient failures use bounded exponential backoff. Permanent failures retain affected records and stop delivery until cleanup. Process crashes and cleanup after a bounded final flush can lose in-memory records; failures and overflow are exposed through writer attributes and events.

The public Xentara adapter supports scalar values only. Binary and image source contracts are tracked separately.

## Local demo

```sh
docker compose -f examples/compose.yaml up -d
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build --parallel
xentara --plugin build/reductstore-xentara.json --model examples/model.json
```

The model writes a 500 ms snapshot of Pulse, Sine, Triangle, Saw-Tooth, Inverted Saw-Tooth, and Noise to `xentara-demo/telemetry/snapshot`. ReductStore binds only to `127.0.0.1:8383`; `xentara-demo-token` is demo-only.

```sh
curl -H 'Authorization: Bearer xentara-demo-token' http://127.0.0.1:8383/api/v1/b/xentara-demo
curl -H 'Authorization: Bearer xentara-demo-token' 'http://127.0.0.1:8383/api/v1/b/xentara-demo/a/xentara-demo/telemetry/snapshot'
docker compose -f examples/compose.yaml down -v
```

## Tests and limitation
Enable integration tests with `-DREDUCTSTORE_XENTARA_ENABLE_INTEGRATION_TESTS=ON`, then run `ctest --test-dir build --output-on-failure` and `tests/integration-recovery.sh`. They cover multi-entry batches, payload metadata, immutable timestamp conflicts, unavailable-server handling, and recovery.

The installed runtime loads the plugin but rejects the model because its signed licence lacks the `ReductStore` skill entitlement. Direct SDK integration tests pass; six-signal Xentara execution remains blocked until embedded ocean issues that entitlement.

## License

Project code is licensed under the Boost Software License 1.0. `LICENSE` contains the original template notice. ReductStore C++ SDK and Xentara have their own licenses.
