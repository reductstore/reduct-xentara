# ReductStore Skill for Xentara

A C++20 Xentara 2.1.1 plugin that writes scalar Xentara measurements to [ReductStore](https://www.reduct.store/). It is based on `xentara/xentara-template-uplink-batch`; original embedded ocean copyright and Boost Software License 1.0 notices are retained in the template-derived source.

## Requirements

- Ubuntu 24.04 AMD64
- Xentara Runtime and `xentara-dev` 2.1.1
- CMake 3.25 or newer, a C++20 compiler, OpenSSL development headers
- Docker Compose for the local demo

The build pins `reduct-cpp` v1.21.3 to commit `6bddc6ac7d428f8c9313967ca87865b2a243ab02`. ReductStore is pinned to `reduct/store:v1.21.3` in the demo.

## Build and load

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build --parallel
ctest --test-dir build --output-on-failure
xentara --plugin build/reductstore-xentara.json --model examples/model.json
```

`build/reductstore-xentara.json` is the generated manifest. To install outside the build tree, use `cmake --install build --prefix "$HOME/.local"` and pass `--plugin-dir "$HOME/.local/share/xentara/plugins"`.

## Configuration

`@Skill.ReductStore.Client` owns an asynchronous uploader. Required fields are `serverUrl` and `bucket`. `apiToken` is optional only for servers configured for anonymous access.

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `serverUrl` | string | required | `http://` or `https://` ReductStore URL |
| `apiToken` | string | empty | ReductStore API token |
| `bucket` | string | required | Destination bucket |
| `entryPrefix` | string | empty | Optional relative device path prepended to every entry; recommended for shared buckets |
| `host` | string | `auto` | `host` record label; `auto` reads the operating-system hostname |
| `connectionTimeout` | duration | `2s` | Connection timeout |
| `requestTimeout` | duration | `10s` | Request timeout |
| `createBucket` | Boolean | `false` | Create the destination bucket if missing |
| `maxBatchRecords` | integer | `64` | Small-record batch limit, below ReductStore's default 85-record server limit |
| `maxBatchPayloadBytes` | integer | `4194304` | Small-record batch payload limit |
| `maxBatchMetadataBytes` | integer | `6144` | Entry, content-type, and label budget per batch, below the default 8 KiB server limit |
| `maxBatchAge` | duration | `2s` | Maximum age before flush |
| `maxQueueBytes` | integer | `16777216` | Memory budget for queued and in-flight record payloads and metadata |
| `overflowPolicy` | string | `rejectNewest` | `rejectNewest` or `dropOldest` |
| `retry.maxAttempts` | integer | `3` | Bounded attempts per send cycle |
| `retry.initialBackoff` | duration | `100ms` | Initial retry delay |
| `retry.maximumBackoff` | duration | `2s` | Maximum retry delay |

`@Skill.ReductStore.Writer.records` maps one `dataPoint` to one relative `entry`. Point records support static `labels` and `timestampSource`: `scheduled`, `sourceTime`, or `updateTime`. If configured, the Client `entryPrefix` namespaces the final entry. Without it, `entry` is the complete relative ReductStore entry path.

`@Skill.ReductStore.Writer.recordGroups` supports `snapshot` groups. A group requires `mode: "snapshot"`, a relative `entry`, `timestampSource: "scheduled"`, and a non-empty `measurements` object mapping JSON field names to Xentara data points. One successful collection produces one `application/json` record:

```json
{"measurements":{"temperature":42.5,"pressure":1.03},"quality":{"temperature":"Good","pressure":"Acceptable"}}
```

Snapshot groups read every standard Xentara point quality, store each value under the `quality` JSON path, and add the worst group quality as the `quality` record label. They add the automatic `host` label plus static labels, but reject static `host` or `quality` labels. Use snapshots for values produced in one pipeline cycle. An unreadable value or quality emits no partial snapshot, increments `collectionFailures`, and raises `sendError`. Repeated scheduled timestamps emit no record and increment `duplicateSnapshots`.

The writer serializes Boolean, signed integer, unsigned integer, and floating-point values without string conversion. ReductSelect exposes nested JSON paths such as `measurements.temperature` and `quality.temperature`. Xentara 2.1.1 exposes numeric values and `Image` through public read handles. The skill intentionally does not claim native arbitrary binary or array ingestion. Its generic file streaming transport is independently tested and available for future source adapters that can provide a bounded stream.

## Timestamp and delivery policy

`scheduled` uses the pipeline scheduled time. `sourceTime` and `updateTime` use the corresponding Xentara time-point and suppress repeated timestamps per point mapping. Snapshot groups require `scheduled`: their one timestamp describes the pipeline cycle, not independently sampled hardware timestamps. Before duplicate detection and delivery, the skill normalizes every timestamp to microseconds because ReductStore identifies immutable records by `entry + timestamp` at microsecond precision. Use `updateTime` when unchanged point values must not produce duplicate ReductStore records.

The collection task only reads, serializes, and enqueues. A worker thread performs all network I/O. It flushes on 64 records, 4 MiB payload, 6 KiB metadata, or two seconds by default using multi-entry `IBucket::WriteBatch()`. The independently tested `FilePayload` transport uses `IBucket::Write()` with bounded callback chunks and a known total length. It is available to a future Xentara binary source adapter; the current public Xentara data-point adapter does not claim arbitrary binary or image ingestion.

The queue reserves memory for every queued and in-flight record, including entries, content types, and labels. Writer attributes expose `queuedBytes`, `queuedRecords`, `failedRecords`, `collectionFailures`, `duplicateSnapshots`, `rejectedRecords`, `droppedRecords`, and `lastSuccessfulSendTime`; rejected, dropped, and collection-failure records raise `sendError`. `rejectNewest` increments `rejectedRecords` and preserves already reserved data. `dropOldest` increments `droppedRecords`. Process crashes lose the in-memory queue. Transient failures retry with the configured bounded exponential backoff. A permanent write failure retains affected records and stops delivery until cleanup, avoiding repeated writes that cannot succeed. Cleanup has a bounded final flush, then releases undelivered reservations and reports their count. ReductStore records are immutable. A transport failure after the server accepts data is ambiguous: the skill never marks it delivered without an SDK success response, so an operator may need to resolve a duplicate-timestamp conflict before retrying.

## Local demo

```sh
docker compose -f examples/compose.yaml up -d
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build --parallel
xentara --plugin build/reductstore-xentara.json --model examples/model.json
```

The example copies only the required Signal Generator configuration and writes a 500 ms scheduled JSON snapshot containing Pulse, Sine, Triangle, Saw-Tooth, Inverted Saw-Tooth, and Noise to `xentara-demo/telemetry/snapshot`. ReductStore is bound only to `127.0.0.1:8383`; `xentara-demo-token` is demo-only.

Inspect entries with:

```sh
curl -H 'Authorization: Bearer xentara-demo-token' http://127.0.0.1:8383/api/v1/b/xentara-demo
curl -H 'Authorization: Bearer xentara-demo-token' 'http://127.0.0.1:8383/api/v1/b/xentara-demo/a/xentara-demo/telemetry/snapshot'
```

Stop and remove demo data with:

```sh
docker compose -f examples/compose.yaml down -v
```

## Validation limitation

The supplied runtime loads `libreductstore-xentara.so` from the generated manifest, then refuses to start the model because its signed licence does not include the `ReductStore` skill entitlement. Xentara reports `could not find a valid Xentara licence for skill "ReductStore"`. Direct SDK integration tests against the local server pass, but the six-signal Xentara execution and readback remain blocked until embedded ocean issues that signed skill entitlement.

## Integration tests

Start the demo service, configure with `-DREDUCTSTORE_XENTARA_ENABLE_INTEGRATION_TESTS=ON`, then run:

```sh
ctest --test-dir build --output-on-failure
tests/integration-recovery.sh
```

The integration suite verifies multi-entry batch writes, stored scalar payload metadata, a generated 20 MiB file through the chunked `Write()` callback, duplicate timestamp errors, unavailable-server reporting, and recovery after the compose service restarts.

## License

Project code is licensed under the Boost Software License 1.0. `LICENSE` contains the original template license notice. ReductStore C++ SDK and Xentara have their own licenses.
