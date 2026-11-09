#include "reductstore_xentara/Batching.hpp"
#include "reductstore_xentara/Configuration.hpp"
#include "reductstore_xentara/Serialization.hpp"
#include "reductstore_xentara/Transmission.hpp"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
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
}

void serializesNumericMeasurements()
{
	check(serializeScalar(std::intmax_t(-17)) == "-17", "signed integers must retain their value");
	check(serializeScalar(std::uintmax_t(42)) == "42", "unsigned integers must retain their value");
	check(serializeScalar(3.25) == "3.25", "floating point values must be JSON numbers");
	check(serializeScalar(true) == "true", "booleans must be JSON booleans");
}

void preservesTimestampPrecision()
{
	const auto timestamp = TimePoint(std::chrono::nanoseconds(1'731'234'567'890'123'456LL));
	const auto record = Record { "entry", timestamp, "application/json", "42", { { "host", "edge-a" } } };
	check(record.timestamp == timestamp, "records must retain nanosecond timestamps");
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
	planner.push(Record { "a", {}, "application/json", "12", {} }, now);
	check(!planner.shouldFlush(now), "a non-full batch must remain queued");
	planner.push(Record { "b", {}, "application/json", "345", {} }, now);
	check(planner.shouldFlush(now), "the payload threshold must flush a batch");
	check(planner.take().size() == 2, "the planner must keep multiple entries together");
}

void accountsForMetadataAndInflightRecords()
{
	const Record record { "edge-01/telemetry/temperature", {}, "application/json", "1234", { { "host", "edge-01" }, { "source", "Sensors.Temperature" } } };
	BoundedQueue queue(record.storageBytes(), OverflowPolicy::RejectNewest);
	check(queue.push(record), "a record must fit within its exact queue budget");
	auto inflight = queue.tryPop();
	check(inflight.has_value(), "the queued record must become in-flight");
	check(queue.residentBytes() == record.storageBytes(), "in-flight records must remain in the memory budget");
	check(!queue.push(record), "a second record must be rejected while the first is in-flight");
	queue.release(*inflight);
	check(queue.residentBytes() == 0, "delivered records must release the complete memory reservation");
	check(queue.push(record), "released memory must accept a new record");
}

void preservesPartialBatchRecordIdentity()
{
	const auto timestamp = TimePoint(std::chrono::nanoseconds(123));
	const auto partial = UploadResult::partial({ { "entry-a", timestamp } });
	check(!partial.delivered, "partial write errors must not be marked as delivered");
	check(partial.failedRecords.size() == 1, "partial failures must retain record identities");
	check(partial.failedRecords.front().entry == "entry-a", "partial failures must retain the failed entry");
	check(partial.failedRecords.front().timestamp == timestamp, "partial failures must retain the failed timestamp");
}

void retriesTransientFailures()
{
	RetryPolicy retry { 3, std::chrono::milliseconds(0), std::chrono::milliseconds(0) };
	std::size_t attempts = 0;
	const auto result = retryTransient(retry, [&attempts] {
		++attempts;
		return attempts == 3 ? UploadResult::success() : UploadResult::transportFailure("unavailable");
	});
	check(result.delivered, "a retry succeeding within the bound must be delivered");
	check(attempts == 3, "transient failures must use the configured retry bound");
}

void rejectsOverflowWithoutDiscardingQueuedRecords()
{
	const Record first { "a", {}, "application/json", "1234", {} };
	BoundedQueue queue(first.storageBytes(), OverflowPolicy::RejectNewest);
	check(queue.push(first), "the first record must fit");
	check(!queue.push(Record { "b", {}, "application/json", "1", {} }), "a full queue must reject the new record");
	check(queue.size() == 1, "overflow must preserve queued records");
	check(queue.rejectedRecords() == 1, "queue loss must be observable");
}

void retriesTransientFailuresAndPreservesPartialFailures()
{
	retriesTransientFailures();
	preservesPartialBatchRecordIdentity();
}

void streamsFilesInBoundedChunks()
{
	const auto file = std::filesystem::temp_directory_path() / "reductstore-xentara-streaming-test.bin";
	{
		std::ofstream stream(file, std::ios::binary);
		std::string chunk(1024 * 1024, 'x');
		for (int index = 0; index < 20; ++index)
		{
			stream.write(chunk.data(), static_cast<std::streamsize>(chunk.size()));
		}
	}

	FilePayload payload(file);
	check(payload.size() == 20U * 1024U * 1024U, "the stream source must know the total length");
	std::size_t observed = 0;
	for (std::size_t offset = 0; offset < payload.size(); offset += 65536)
	{
		const auto chunk = payload.read(offset, 65536);
		check(chunk.size() <= 65536, "streaming reads must stay bounded");
		observed += chunk.size();
	}
	check(observed == payload.size(), "streaming reads must cover the complete file");
	std::filesystem::remove(file);
}

void releasesPendingRecordsOnShutdown()
{
	ClientConfiguration configuration;
	configuration.serverUrl = "http://127.0.0.1:1";
	configuration.bucket = "test";
	configuration.connectionTimeout = std::chrono::milliseconds(10);
	configuration.requestTimeout = std::chrono::milliseconds(10);
	configuration.retry.maxAttempts = 1;
	configuration.maxBatchAge = std::chrono::milliseconds(0);
	UplinkWorker worker(configuration);
	worker.start();
	check(worker.enqueue(Record { "entry", TimePoint::clock::now(), "application/json", "1", {} }), "the record must enter the worker queue");
	worker.stop();
	check(worker.status().queuedBytes == 0, "shutdown must release undelivered record reservations");
	check(worker.status().failedRecords == 1, "shutdown must expose the undelivered record count");
}

}

int main()
{
	const std::vector<std::pair<std::string, std::function<void()>>> tests {
		{ "rejects invalid configuration", rejectsInvalidConfiguration },
		{ "serializes numeric measurements", serializesNumericMeasurements },
		{ "preserves timestamp precision", preservesTimestampPrecision },
		{ "selects batch boundaries", selectsBatchBoundaries },
		{ "accounts for metadata and in-flight records", accountsForMetadataAndInflightRecords },
		{ "rejects queue overflow", rejectsOverflowWithoutDiscardingQueuedRecords },
		{ "retries transient failures", retriesTransientFailuresAndPreservesPartialFailures },
		{ "releases pending records on shutdown", releasesPendingRecordsOnShutdown },
		{ "streams files in bounded chunks", streamsFilesInBoundedChunks },
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
