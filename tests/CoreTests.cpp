#include "reductstore_xentara/Batching.hpp"
#include "reductstore_xentara/Configuration.hpp"
#include "reductstore_xentara/Serialization.hpp"

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

void rejectsOverflowWithoutDiscardingQueuedRecords()
{
	BoundedQueue queue(4, OverflowPolicy::RejectNewest);
	check(queue.push(Record { "a", {}, "application/json", "1234", {} }), "the first record must fit");
	check(!queue.push(Record { "b", {}, "application/json", "1", {} }), "a full queue must reject the new record");
	check(queue.size() == 1, "overflow must preserve queued records");
	check(queue.rejectedRecords() == 1, "queue loss must be observable");
}

void retriesTransientFailuresAndPreservesPartialFailures()
{
	RetryPolicy retry { 3, std::chrono::milliseconds(0), std::chrono::milliseconds(0) };
	std::size_t attempts = 0;
	const auto result = retryTransient(retry, [&attempts] {
		++attempts;
		return attempts == 3 ? UploadResult::success() : UploadResult::transportFailure("unavailable");
	});
	check(result.delivered, "a retry succeeding within the bound must be delivered");
	check(attempts == 3, "transient failures must use the configured retry bound");

	const auto partial = UploadResult::partial({ "entry-a" });
	check(!partial.delivered, "partial write errors must not be marked as delivered");
	check(partial.failedEntries.size() == 1, "partial failures must retain failed entries");
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

}

int main()
{
	const std::vector<std::pair<std::string, std::function<void()>>> tests {
		{ "rejects invalid configuration", rejectsInvalidConfiguration },
		{ "serializes numeric measurements", serializesNumericMeasurements },
		{ "preserves timestamp precision", preservesTimestampPrecision },
		{ "selects batch boundaries", selectsBatchBoundaries },
		{ "rejects queue overflow", rejectsOverflowWithoutDiscardingQueuedRecords },
		{ "retries transient failures", retriesTransientFailuresAndPreservesPartialFailures },
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
