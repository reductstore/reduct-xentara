#include "reductstore_xentara/Batching.hpp"
#include "reductstore_xentara/Configuration.hpp"
#include "reductstore_xentara/Serialization.hpp"
#include "reductstore_xentara/Transmission.hpp"

#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

namespace
{

using namespace reductstore_xentara;

void check(bool value, const std::string& message)
{
	if (!value)
	{
		throw std::runtime_error(message);
	}
}

void rejectsInvalidConfiguration()
{
	ClientConfiguration config;
	config.serverUrl = "not-a-url";
	config.bucket = "demo";
	check(!validate(config).empty(), "an invalid server URL must be rejected");

	config.serverUrl = "http://127.0.0.1:8383";
	config.bucket.clear();
	check(!validate(config).empty(), "an empty bucket must be rejected");

	config.bucket = "demo";
	config.maxBatchRecords = 0;
	check(!validate(config).empty(), "a zero batch record limit must be rejected");

	config.maxBatchRecords = 64;
	config.maxBatchMetadataBytes = 0;
	check(!validate(config).empty(), "a zero batch metadata limit must be rejected");

	config.maxBatchMetadataBytes = 6U * 1024U;
	config.shutdownTimeout = std::chrono::milliseconds(1);
	config.requestTimeout = std::chrono::milliseconds(2);
	check(!validate(config).empty(), "a shutdown timeout shorter than a request must be rejected");

	ClientConfiguration shutdownConfiguration;
	check(shutdownConfiguration.shutdownTimeout > shutdownConfiguration.requestTimeout,
		"default shutdown time must allow at least one final HTTP request");
}

void namespacesEntriesWithoutMandatoryPrefix()
{
	check(entryName({}, "telemetry") == "telemetry", "an empty prefix must preserve a complete entry path");
	check(entryName("edge-a", "telemetry") == "edge-a/telemetry", "a configured prefix must namespace the entry");
}

void serializesNumericMeasurements()
{
	check(serializeScalar(std::intmax_t(-17)) == "-17", "signed integers must retain their value");
	check(serializeScalar(std::uintmax_t(42)) == "42", "unsigned integers must retain their value");
	check(serializeScalar(3.25) == "3.25", "floating point values must be JSON numbers");
	check(serializeScalar(true) == "true", "booleans must be JSON booleans");
}

void rejectsNonFiniteMeasurementsWithoutSerialization()
{
	check(!trySerializeScalar(std::numeric_limits<double>::quiet_NaN()), "NaN must be rejected before JSON serialization");
	check(!trySerializeScalar(std::numeric_limits<double>::infinity()), "infinity must be rejected before JSON serialization");
	check(trySerializeScalar(3.25) == "3.25", "finite floating-point values must remain JSON numbers");
}

void serializesScheduledSnapshot()
{
	const std::vector<std::pair<std::string, std::string>> fields {
		{ "pulse", "true" },
		{ "sine", "74.3" },
		{ "quoted\"field", "-2" },
	};
	check(serializeObject(fields) == "{\"pulse\":true,\"sine\":74.3,\"quoted\\\"field\":-2}", "a snapshot must serialize named JSON fields");
}

void serializesSnapshotSourceTime()
{
	const auto sourceTime = serializeObject({
		{ "temperature", serializeScalar(unixMicroseconds(TimePoint(std::chrono::nanoseconds(1'794'000'000'000'123'789LL)))) },
	});
	const auto payload = serializeObject({
		{ "measurements", serializeObject({ { "temperature", "42.5" }, { "pressure", "1.03" } }) },
		{ "sourceTime", sourceTime },
		{ "quality", serializeObject({ { "temperature", serializeString("Good") }, { "pressure", serializeString("Good") } }) },
	});
	check(payload == "{\"measurements\":{\"temperature\":42.5,\"pressure\":1.03},\"sourceTime\":{\"temperature\":1794000000000123},\"quality\":{\"temperature\":\"Good\",\"pressure\":\"Good\"}}",
		"a snapshot must preserve available per-measurement source times as Unix microseconds");
}

void serializesSnapshotQuality()
{
	const auto measurements = serializeObject({
		{ "pulse", "true" },
		{ "sine", "74.3" },
	});
	const auto quality = serializeObject({
		{ "pulse", serializeString("Good") },
		{ "sine", serializeString("Acceptable") },
	});
	const auto payload = serializeObject({
		{ "measurements", measurements },
		{ "quality", quality },
	});
	check(payload == "{\"measurements\":{\"pulse\":true,\"sine\":74.3},\"quality\":{\"pulse\":\"Good\",\"sine\":\"Acceptable\"}}",
		"a snapshot must retain values and per-measurement quality");
}

void preservesTimestampPrecision()
{
	const auto timestamp = TimePoint(std::chrono::nanoseconds(1'731'234'567'890'123'789LL));
	const auto normalized = normalizeTimestamp(timestamp);
	check(normalized == TimePoint(std::chrono::microseconds(1'731'234'567'890'123)), "record timestamps must truncate to ReductStore microseconds");
	const auto record = Record { "entry", normalized, "application/json", "42", { { "host", "edge-a" } } };
	check(record.timestamp == normalized, "records must retain normalized timestamps");
	check(record.labels.at("host") == "edge-a", "record labels must be retained");
}

void selectsBatchBoundaries()
{
	BatchLimits limits;
	limits.maxRecords = 2;
	limits.maxPayloadBytes = 5;
	limits.maxAge = std::chrono::seconds(2);
	BatchPlanner planner(limits);
	const auto now = std::chrono::steady_clock::now();
	check(planner.push(Record { "a", {}, "application/json", "12", {} }, now), "the first record must enter the planner");
	check(!planner.shouldFlush(now), "a non-full batch must remain queued");
	check(planner.push(Record { "b", {}, "application/json", "345", {} }, now), "the second record must enter the planner");
	check(planner.shouldFlush(now), "the payload threshold must flush a batch");
	check(planner.take().size() == 2, "the planner must keep multiple entries together");
}

void refusesRecordThatExceedsBatchLimit()
{
	BatchLimits limits;
	limits.maxRecords = 2;
	limits.maxPayloadBytes = 5;
	limits.maxMetadataBytes = 100;
	BatchPlanner planner(limits);
	const auto now = std::chrono::steady_clock::now();
	const Record first { "a", {}, "application/json", "123", {} };
	const Record second { "b", {}, "application/json", "456", {} };
	check(planner.accepts(first), "the first record must fit");
	check(planner.push(first, now), "the first record must enter the planner");
	check(!planner.accepts(second), "a record that exceeds a batch payload limit must not fit");
	check(!planner.push(second, now), "the planner must refuse a record that exceeds a batch limit");
}

void flushesAtMetadataBoundary()
{
	BatchLimits limits;
	limits.maxRecords = 3;
	limits.maxPayloadBytes = 100;
	limits.maxMetadataBytes = 2;
	limits.maxAge = std::chrono::seconds(2);
	BatchPlanner planner(limits);
	const auto now = std::chrono::steady_clock::now();
	check(planner.push(Record { "a", {}, "", "", {} }, now), "the first metadata record must enter the planner");
	check(!planner.shouldFlush(now), "one metadata record must remain queued");
	check(planner.push(Record { "b", {}, "", "", {} }, now), "the second metadata record must enter the planner");
	check(planner.shouldFlush(now), "the metadata threshold must flush a batch");
}

void accountsForMetadataAndInflightRecords()
{
	const Record record { "edge-01/telemetry/temperature", {}, "application/json", "1234", { { "host", "edge-01" }, { "source", "Sensors.Temperature" } } };
	BoundedQueue queue(record.storageBytes(), OverflowPolicy::RejectNewest);
	check(queue.push(record).accepted, "a record must fit within its exact queue budget");
	auto inflight = queue.tryPop();
	check(inflight.has_value(), "the queued record must become in-flight");
	check(queue.residentBytes() == record.storageBytes(), "in-flight records must remain in the memory budget");
	check(!queue.push(record).accepted, "a second record must be rejected while the first is in-flight");
	queue.release(*inflight);
	check(queue.residentBytes() == 0, "delivered records must release the complete memory reservation");
	check(queue.push(record).accepted, "released memory must accept a new record");
}

void preservesPartialBatchRecordIdentity()
{
	const auto timestamp = TimePoint(std::chrono::nanoseconds(123));
	const auto partial = UploadResult::partial({ { { "entry-a", timestamp }, 409, "already exists" } });
	check(!partial.delivered, "partial write errors must not be marked as delivered");
	check(partial.recordFailures.size() == 1, "partial failures must retain record identities");
	check(partial.recordFailures.front().identity.entry == "entry-a", "partial failures must retain the failed entry");
	check(partial.recordFailures.front().identity.timestamp == timestamp, "partial failures must retain the failed timestamp");
	check(partial.recordFailures.front().code == 409, "partial failures must retain the ReductStore error code");
}

void retainsTransientPartialFailuresForRetry()
{
	const auto partial = UploadResult::partial({ { { "entry-a", {}, }, 503, "unavailable" } });
	check(partial.retryable, "a transient per-record batch error must keep the failed record retryable");
}


void rejectsOverflowWithoutDiscardingQueuedRecords()
{
	const Record first { "a", {}, "application/json", "1234", {} };
	BoundedQueue queue(first.storageBytes(), OverflowPolicy::RejectNewest);
	check(queue.push(first).accepted, "the first record must fit");
	check(!queue.push(Record { "b", {}, "application/json", "1", {} }).accepted, "a full queue must reject the new record");
	check(queue.size() == 1, "overflow must preserve queued records");
	check(queue.rejectedRecords() == 1, "queue loss must be observable");
}

void attributesQueueRejectionToProducer()
{
	const Record record { "a", {}, "application/json", "1234", {}, 7 };
	ClientConfiguration configuration;
	configuration.maxQueueBytes = record.storageBytes();
	UplinkWorker worker(configuration);
	check(worker.enqueue(record), "the first producer record must fit");
	check(!worker.enqueue(Record { "b", {}, "application/json", "1", {}, 9 }),
		"a full queue must reject the next producer record");
	const auto status = worker.status();
	check(status.rejectedRecordsByProducer.at(9) == 1, "queue rejection must be attributed to its producer");
	check(status.failedRecordsByProducer.at(9) == 1, "queue rejection must be counted as a producer delivery failure");
}

void dropsOldestRecordWhenConfigured()
{
	const Record first { "a", {}, "application/json", "1234", {} };
	BoundedQueue queue(first.storageBytes(), OverflowPolicy::DropOldest);
	check(queue.push(first).accepted, "the first record must fit");
	const auto result = queue.push(Record { "b", {}, "application/json", "1234", {} });
	check(result.accepted, "drop-oldest overflow must accept the replacement record");
	check(result.droppedRecords.size() == 1 && result.droppedRecords.front().entry == "a",
		"drop-oldest overflow must identify the discarded record");
	const auto record = queue.tryPop();
	check(record && record->entry == "b", "drop-oldest overflow must retain the newest record");
	check(queue.droppedRecords() == 1, "dropped records must be observable");
}

void attributesQueueDropToProducer()
{
	const Record first { "a", {}, "application/json", "1234", {}, 7 };
	ClientConfiguration configuration;
	configuration.maxQueueBytes = first.storageBytes();
	configuration.overflowPolicy = OverflowPolicy::DropOldest;
	UplinkWorker worker(configuration);
	check(worker.enqueue(first), "the first producer record must fit");
	check(worker.enqueue(Record { "b", {}, "application/json", "1234", {}, 9 }),
		"drop-oldest policy must accept the replacement record");
	const auto status = worker.status();
	check(status.droppedRecordsByProducer.at(7) == 1, "queue drop must be attributed to the discarded producer");
	check(status.failedRecordsByProducer.at(7) == 1, "queue drop must count as a producer delivery failure");
}
void rejectsEntireBatchWhenDuplicateIdentitiesExist()
{
	ClientConfiguration configuration;
	configuration.serverUrl = "http://127.0.0.1:1";
	configuration.bucket = "test";
	ReductTransport transport(configuration);
	const auto timestamp = TimePoint::clock::now();
	const auto result = transport.writeBatch({
		{ "duplicate", timestamp, "application/json", "1", {} },
		{ "duplicate", timestamp, "application/json", "2", {} },
		{ "valid", timestamp, "application/json", "3", {} },
	});
	check(!result.delivered, "a batch containing duplicate identities must not be delivered");
	check(result.errorCode == 409, "a rejected duplicate batch must report the conflict status");
	check(result.recordFailures.empty(), "a rejected duplicate batch must not report individual partial failures");
}

void partitionsPartialBatchBeforeRetry()
{
	const auto timestamp = TimePoint::clock::now();
	std::vector<Record> records {
		{ "transient", timestamp, "application/json", "1", {}, 7 },
		{ "permanent", timestamp, "application/json", "2", {}, 8 },
		{ "success", timestamp, "application/json", "3", {}, 9 },
	};
	const auto result = UploadResult::partial({
		{ { "transient", timestamp }, 503, "unavailable" },
		{ { "permanent", timestamp }, 409, "already exists" },
	});
	const auto outcome = partitionPartialBatch(std::move(records), result);
	check(outcome.retry.size() == 1 && outcome.retry.front().entry == "transient", "only transient per-record failures may retry");
	check(outcome.succeeded.size() == 1 && outcome.succeeded.front().entry == "success", "successful records must be removed before retry");
	check(outcome.permanent.size() == 1 && outcome.permanent.front().second.code == 409, "permanent per-record failures must not retry");
}

void refusesDropWhenQueuedRecordsCannotFreeEnoughMemory()
{
	const auto unit = Record { "unit", {}, "application/json", std::string(1024, 'x'), {} }.storageBytes();
	BoundedQueue queue(10 * unit, OverflowPolicy::DropOldest);
	for (int index = 0; index < 10; ++index)
	{
		check(queue.push(Record { "queued", {}, "application/json", std::string(1024, 'x'), {} }).accepted, "queued fixture records must fit");
	}
	for (int index = 0; index < 9; ++index)
	{
		check(queue.tryPop().has_value(), "fixture records must become in-flight");
	}
	const auto result = queue.push(Record { "large", {}, "application/json", std::string(2048, 'x'), {} });
	check(!result.accepted, "drop-oldest must reject when in-flight reservations leave insufficient memory");
	check(result.droppedRecords.empty(), "drop-oldest must not drop queued records when they cannot free enough memory");
	check(queue.droppedRecords() == 0, "no queued record may be discarded for an impossible drop");
	check(queue.size() == 1, "the existing queued record must remain intact");
}
void releasesPendingRecordsOnShutdown()
{
	ClientConfiguration configuration;
	configuration.serverUrl = "http://127.0.0.1:1";
	configuration.bucket = "test";
	UplinkWorker worker(configuration);
	worker.start();
	check(worker.enqueue(Record { "entry", TimePoint::clock::now(), "application/json", "1", {}, 42 }), "the record must enter the worker queue");
	const auto started = std::chrono::steady_clock::now();
	worker.stop();
	check(std::chrono::steady_clock::now() - started < std::chrono::seconds(14),
		"default shutdown must release queued records before the shutdown deadline");
	check(worker.status().queuedBytes == 0, "shutdown must release undelivered record reservations");
	check(worker.status().failedRecords == 1, "shutdown must expose the undelivered record count");
	check(worker.status().failedRecordsByProducer.at(42) == 1, "shutdown must attribute undelivered records to their writer");
	check(worker.status().lastErrorCodeByProducer.at(42) == -1, "shutdown must attribute the timeout error code to their writer");
}

}

int main()
{
	const std::vector<std::pair<std::string, std::function<void()>>> tests {
		{ "rejects invalid configuration", rejectsInvalidConfiguration },
		{ "namespaces optional entry prefixes", namespacesEntriesWithoutMandatoryPrefix },
		{ "serializes numeric measurements", serializesNumericMeasurements },
		{ "rejects non-finite measurements", rejectsNonFiniteMeasurementsWithoutSerialization },
		{ "serializes scheduled snapshots", serializesScheduledSnapshot },
		{ "serializes snapshot quality", serializesSnapshotQuality },
		{ "serializes snapshot source time", serializesSnapshotSourceTime },
		{ "preserves timestamp precision", preservesTimestampPrecision },
		{ "selects batch boundaries", selectsBatchBoundaries },
		{ "flushes at metadata boundary", flushesAtMetadataBoundary },
		{ "accounts for metadata and in-flight records", accountsForMetadataAndInflightRecords },
		{ "rejects queue overflow", rejectsOverflowWithoutDiscardingQueuedRecords },
		{ "attributes queue rejection", attributesQueueRejectionToProducer },
		{ "attributes queue drop", attributesQueueDropToProducer },
		{ "drops oldest queue overflow", dropsOldestRecordWhenConfigured },
		{ "refuses unsafe drop-oldest", refusesDropWhenQueuedRecordsCannotFreeEnoughMemory },
		{ "retains transient partial failures", retainsTransientPartialFailuresForRetry },
		{ "partitions partial batch before retry", partitionsPartialBatchBeforeRetry },
		{ "rejects entire duplicate batch", rejectsEntireBatchWhenDuplicateIdentitiesExist },
		{ "preserves partial batch identities", preservesPartialBatchRecordIdentity },
		{ "refuses oversized batch records", refusesRecordThatExceedsBatchLimit },
		{ "releases pending records on shutdown", releasesPendingRecordsOnShutdown },
	};

	for (const auto& [name, test] : tests)
	{
		try
		{
			test();
		}
		catch (const std::exception& error)
		{
			std::cerr << name << ": " << error.what() << '\n';
			return 1;
		}
	}
	return 0;
}
