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
| `host` | string | empty | `host` record label |
| `connectionTimeout` | duration | `2s` | Connection timeout |
| `requestTimeout` | duration | `10s` | Request timeout |
| `createBucket` | Boolean | `false` | Create the destination bucket if missing |
| `maxBatchRecords` | integer | `256` | Small-record batch limit |
| `maxBatchPayloadBytes` | integer | `4194304` | Small-record batch payload limit |
| `maxBatchAge` | duration | `2s` | Maximum age before flush |
| `maxQueueBytes` | integer | `16777216` | Memory budget for queued and in-flight record payloads and metadata |
| `overflowPolicy` | string | `rejectNewest` | `rejectNewest` or `dropOldest` |
| `retry.maxAttempts` | integer | `3` | Bounded attempts per send cycle |
| `retry.initialBackoff` | duration | `100ms` | Initial retry delay |
| `retry.maximumBackoff` | duration | `2s` | Maximum retry delay |

`@Skill.ReductStore.Writer.records` maps one `dataPoint` to one `entry`. Point records support static `labels` and `timestampSource`: `scheduled`, `sourceTime`, or `updateTime`.

`@Skill.ReductStore.Writer.recordGroups` supports `snapshot` groups. A group requires `mode: "snapshot"`, an `entry`, `timestampSource: "scheduled"`, and non-empty `fields`, where every field has a unique `name` and a `dataPoint`. One successful collection produces one `application/json` object such as `{"temperature":42.5,"pressure":1.03}`. Use snapshot groups for values produced in the same pipeline cycle. If any field cannot be read, the group produces no partial snapshot. Static group labels and the configured `host` label apply to the record; point-only `source` and `quality` labels do not represent individual snapshot fields.

The writer serializes Boolean, signed integer, unsigned integer, and floating-point values without string conversion. Point records use compact JSON scalar payloads. Snapshot groups use compact JSON objects, which ReductSelect exposes as SQL columns. Xentara 2.1.1 exposes numeric values and `Image` through public read handles. The skill intentionally does not claim native arbitrary binary or array ingestion. Its generic file streaming transport is independently tested and available for future source adapters that can provide a bounded stream.

## Timestamp and delivery policy

`scheduled` uses the pipeline scheduled time. `sourceTime` and `updateTime` preserve the Xentara time-point precision and suppress repeated timestamps per point mapping. Snapshot groups require `scheduled`: their one timestamp describes the pipeline cycle, not six independently sampled hardware timestamps. Use `updateTime` when unchanged point values must not produce duplicate ReductStore records.

The collection task only reads, serializes, and enqueues. A worker thread performs all network I/O. It flushes on 256 records, 4 MiB, or two seconds by default using multi-entry `IBucket::WriteBatch()`. File payloads above the batch threshold use `IBucket::Write()` with the SDK callback and bounded chunks. The total file length is determined before upload.

The queue reserves memory for every queued and in-flight record, including entries, content types, and labels. Writer attributes expose `queuedBytes`, `queuedRecords`, `failedRecords`, `rejectedRecords`, `droppedRecords`, and `lastSuccessfulSendTime`; rejected and dropped records also raise `sendError`. `rejectNewest` increments `rejectedRecords` and preserves already reserved data. `dropOldest` increments `droppedRecords`. Process crashes lose the in-memory queue. Failed batches remain reserved while the worker retries them. Shutdown uses the configured request timeout and bounded retry count, then releases reservations and reports undelivered data. ReductStore records are immutable. A transport failure after the server accepts data is ambiguous: the skill never marks it delivered without an SDK success response, so an operator may need to resolve a duplicate-timestamp conflict before retrying.

## Local demo

```sh
docker compose -f examples/compose.yaml up -d
cmake -S . -B build -DCMAKE_BUILD_TYPE=RelWithDebInfo
cmake --build build --parallel
xentara --plugin build/reductstore-xentara.json --model examples/model.json
```

The example copies only the required Signal Generator configuration and writes a 500 ms scheduled JSON snapshot containing Pulse, Sine, Triangle, Saw-Tooth, Inverted Saw-Tooth, and Noise to `xentara-demo/signals/snapshot`. ReductStore is bound only to `127.0.0.1:8383`; `xentara-demo-token` is demo-only.

Inspect entries with:

```sh
curl -H 'Authorization: Bearer xentara-demo-token' http://127.0.0.1:8383/api/v1/b/xentara-demo
curl -H 'Authorization: Bearer xentara-demo-token' 'http://127.0.0.1:8383/api/v1/b/xentara-demo/a/signals/snapshot'
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
