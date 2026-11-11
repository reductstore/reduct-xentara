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
| `entryPrefix` | empty | Optional relative prefix for every entry. Required to distinguish devices writing the same logical stream. |
| `host` | `auto` | Automatic hostname or explicit `host` label |
| `connectionTimeout` / `requestTimeout` / `shutdownTimeout` | `2s` / `10s` / `15s` | HTTP connect, request, and final-flush bounds |
| `createBucket` | `false` | Create a missing bucket |
| `maxBatchRecords` / `maxBatchPayloadBytes` / `maxBatchMetadataBytes` | `64` / `4194304` / `6144` | Enforced plugin-side batch limits |
| `maxBatchAge` / `maxQueueBytes` | `2s` / `16777216` | Flush age and bounded memory budget |
| `overflowPolicy` | `rejectNewest` | `rejectNewest` or `dropOldest` |
| `retry` | `3`, `100ms`, `2s` | Attempts per retry cycle, initial backoff, maximum backoff |

`@Skill.ReductStore.Writer` accepts `recordGroups`. This release supports `mode: "snapshot"` only. A snapshot requires an entry, `timestampSource: "scheduled"`, and a `measurements` map. It writes one `application/json` record:

```json
{"measurements":{"temperature":42.5,"pressure":1.03},"sourceTime":{"temperature":1794000000000000},"quality":{"temperature":"Good","pressure":"Good"}}
```

The record timestamp is the scheduled pipeline time. `sourceTime` is optional and contains Unix microseconds only for measurements whose Xentara data point supplies it. The writer adds `host`, stores standard Xentara quality per field, and writes the worst quality as the record `quality` label. It accepts Boolean, signed integer, unsigned integer, and floating-point values. An unreadable or non-finite value produces no partial snapshot. `collectionFailures` and `duplicateSnapshots` are exposed as attributes.

Payload version 1 has `measurements` and `quality` objects, plus an optional `sourceTime` object. Quality values use Xentara's `data::toString` vocabulary. `sourceTime` values are Unix microseconds.

## Delivery

Snapshot record timestamps are normalized to ReductStore microseconds before duplicate detection and delivery. Record identity is `entry + timestamp`. Every snapshot either contains all configured values and qualities or is skipped. Source times are read after values and are best effort, so they are optional rather than a point-in-time atomic read. Record groups in one writer require distinct entries. If a submitted batch contains duplicate identities, the entire batch is rejected with 409 before any record is sent. Multi-device deployments must give each device a stable unique entry prefix because labels do not affect ReductStore record identity.

Collection reads, serializes, and enqueues. The `send` task only publishes delivery state; a worker performs HTTP I/O. The worker enforces batch limits before admission. A record larger than a configured batch limit is rejected and reported. With `dropOldest`, the queue first verifies that dropping queued records can free enough memory; otherwise the new record is rejected without discarding anything. `queuedRecords` counts records awaiting pickup; `inFlightRecords` counts records in the batch planner or current request; `queuedBytes` reserves memory for both. `rejectedRecords`, `droppedRecords`, `failedRecords`, and `lastErrorCode` are attributed to each writer. `sent` and `sendError` events use only that writer's own failures.

The worker preserves SDK error codes. It retries transport failures, 408, 429, and 5xx responses. `retry.maxAttempts` bounds one retry cycle; queued transient failures continue with bounded backoff until delivery, a permanent response, or shutdown. Per-record results are partitioned before retrying: confirmed records are released, transient per-record errors retry only their own records, and permanent per-record errors are released and counted without blocking later uploads. A retry that later reports an immutable-record conflict is never counted as delivered. Shutdown allows final requests only while the remaining `shutdownTimeout` can accommodate `requestTimeout`; it releases remaining records as failed and attributes those failures and error codes to their writers. Process crashes, overflow, and final-flush expiry can lose in-memory records and are exposed through attributes and events.

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

Enable integration tests with `-DREDUCTSTORE_XENTARA_ENABLE_INTEGRATION_TESTS=ON`, then run `ctest --test-dir build --output-on-failure` and `tests/integration-recovery.sh`. They cover batch admission limits, queue rejection and drop attribution, snapshot metadata, immutable timestamp conflicts, duplicate batch identities, per-record partial failures, unavailable-server retry, recovery of a queued record across a real server restart, and bounded shutdown.

The installed runtime loads the plugin but rejects the model because its signed licence lacks the `ReductStore` skill entitlement. Direct SDK integration tests pass; six-signal Xentara execution remains blocked until embedded ocean issues that entitlement.

## License

Project code is licensed under the Boost Software License 1.0. `LICENSE` contains the original template notice. ReductStore C++ SDK and Xentara have their own licenses.
