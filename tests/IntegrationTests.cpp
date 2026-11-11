#include "reductstore_xentara/Serialization.hpp"
#include "reductstore_xentara/Transmission.hpp"

#include <reduct/client.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <thread>
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

auto environment(const char* name, const char* fallback) -> std::string
{
	if (const auto* value = std::getenv(name))
	{
		return value;
	}
	return fallback;
}


auto waitFor(auto condition, std::chrono::milliseconds timeout) -> bool
{
	const auto deadline = std::chrono::steady_clock::now() + timeout;
	while (std::chrono::steady_clock::now() < deadline)
	{
		if (condition())
		{
			return true;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
	return condition();
}
auto configuration() -> ClientConfiguration
{
	ClientConfiguration config;
	config.serverUrl = environment("REDUCTSTORE_URL", "http://127.0.0.1:8383");
	config.apiToken = environment("REDUCTSTORE_TOKEN", "xentara-demo-token");
	config.bucket = "xentara-integration";
	config.createBucket = true;
	config.maxBatchRecords = 64;
	config.maxBatchPayloadBytes = 4U * 1024U * 1024U;
	config.maxBatchMetadataBytes = 6U * 1024U;
	config.maxBatchAge = std::chrono::seconds(2);
	return config;
}

void writesAndReadsMultipleEntries()
{
	const auto config = configuration();
	ReductTransport transport(config);
	const auto timestamp = TimePoint::clock::now();
	const std::vector<Record> records {
		{ "signals/a", timestamp, "application/json", "1", { { "host", "integration" }, { "quality", "Good" } } },
		{ "signals/b", timestamp, "application/json", "true", { { "source", "test" } } },
	};
	check(transport.writeBatch(records).delivered, "multi-entry batch write must succeed");

	reduct::HttpOptions options {};
	options.api_token = config.apiToken;
	options.ssl_verification = true;
	auto client = reduct::IClient::Build(config.serverUrl, options);
	auto bucketResult = client->GetBucket(config.bucket);
	check(!bucketResult.error, "integration bucket must be readable");
	std::string body;
	std::string contentType;
	reduct::IBucket::LabelMap labels;
	const auto error = bucketResult.result->Read("signals/a", timestamp, [&](const reduct::IBucket::ReadableRecord& record) {
		contentType = record.content_type;
		labels = record.labels;
		return !record.Read([&body](std::string_view chunk) { body.append(chunk); return true; });
	});
	check(!error, "stored scalar must be readable");
	check(body == "1", "stored scalar payload must match");
	check(contentType == "application/json", "stored content type must match");
	check(labels.at("host") == "integration", "stored labels must match");
}

void writesAndReadsProductionSnapshot()
{
	const auto config = configuration();
	const auto timestamp = TimePoint::clock::now();
	const auto payload = serializeObject({
		{ "measurements", serializeObject({ { "pulse", "true" }, { "sine", "74.3" } }) },
		{ "sourceTime", serializeObject({ { "pulse", "1794000000000000" } }) },
		{ "quality", serializeObject({ { "pulse", serializeString("Good") }, { "sine", serializeString("Acceptable") } }) },
	});
	ReductTransport transport(config);
	const std::vector<Record> records {
		{ "edge-a/telemetry", timestamp, "application/json", payload, { { "host", "edge-a" }, { "quality", "Acceptable" } } },
	};
	check(transport.writeBatch(records).delivered, "production snapshot write must succeed");

	reduct::HttpOptions options {};
	options.api_token = config.apiToken;
	options.ssl_verification = true;
	auto client = reduct::IClient::Build(config.serverUrl, options);
	auto bucketResult = client->GetBucket(config.bucket);
	check(!bucketResult.error, "integration bucket must be readable");
	std::string body;
	reduct::IBucket::LabelMap labels;
	const auto error = bucketResult.result->Read("edge-a/telemetry", timestamp, [&](const reduct::IBucket::ReadableRecord& record) {
		labels = record.labels;
		return !record.Read([&body](std::string_view chunk) { body.append(chunk); return true; });
	});
	check(!error, "stored snapshot must be readable");
	check(body == payload, "stored snapshot payload must match");
	check(labels.at("host") == "edge-a", "snapshot host label must match");
	check(labels.at("quality") == "Acceptable", "snapshot quality label must match");
}

void preservesConnectivityAfterDuplicateRecordFailure()
{
	auto config = configuration();
	config.maxBatchRecords = 2;
	config.maxBatchPayloadBytes = 8;
	config.maxBatchAge = std::chrono::milliseconds(10);
	UplinkWorker worker(config);
	worker.start();
	const auto deliveredTimestamp = TimePoint::clock::now();
	check(worker.enqueue(Record { "connectivity/success", deliveredTimestamp, "application/json", "1", {} }),
		"successful connectivity fixture must enter the worker queue");
	check(waitFor([&worker] {
		const auto status = worker.status();
		return status.connected && status.deliveredRecords == 1;
	}, std::chrono::seconds(2)), "a successful write must establish worker connectivity");

	const auto duplicateTimestamp = deliveredTimestamp + std::chrono::microseconds(1);
	check(worker.enqueue(Record { "connectivity/duplicate", duplicateTimestamp, "application/json", "1", {} }),
		"first duplicate fixture must enter the worker queue");
	check(worker.enqueue(Record { "connectivity/duplicate", duplicateTimestamp, "application/json", "2", {} }),
		"second duplicate fixture must enter the worker queue");
	check(waitFor([&worker] {
		const auto status = worker.status();
		return status.failedRecords == 2 && status.lastErrorCode == 409;
	}, std::chrono::seconds(2)), "duplicate batch must fail permanently");
	check(worker.status().connected, "a record-level 409 must not mark an established client disconnected");

	check(worker.enqueue(Record { "connectivity/oversize", duplicateTimestamp + std::chrono::microseconds(1), "application/json", "payload-too-large", {} }),
		"oversized fixture must enter the worker queue");
	check(waitFor([&worker] {
		const auto status = worker.status();
		return status.failedRecords == 3 && status.lastErrorCode == 413;
	}, std::chrono::seconds(2)), "oversized record must fail permanently");
	check(worker.status().connected, "a record-level 413 must not mark an established client disconnected");
	worker.stop();
}

void workerCompletesSuccessfulRecordsFromPartialBatch()
{
	auto config = configuration();
	config.maxBatchRecords = 2;
	config.maxBatchAge = std::chrono::milliseconds(10);
	config.retry.maxAttempts = 1;
	config.retry.maximumBackoff = std::chrono::milliseconds(10);
	const auto timestamp = TimePoint::clock::now();
	ReductTransport transport(config);
	const Record duplicate { "partial/duplicate", timestamp, "application/json", "1", {} };
	check(transport.writeBatch({ duplicate }).delivered, "partial failure fixture must create a record");

	UplinkWorker worker(config);
	worker.start();
	check(worker.enqueue(duplicate), "duplicate record must enter the worker queue");
	check(worker.enqueue(Record { "partial/success", timestamp, "application/json", "2", {} }), "successful record must enter the worker queue");
	check(waitFor([&worker] {
		const auto status = worker.status();
		return status.deliveredRecords == 1 && status.failedRecords == 1;
	}, std::chrono::seconds(2)), "a partial failure must deliver unaffected records");
	check(worker.status().lastErrorCode == 409, "the partial failure HTTP status must be retained");

	check(worker.enqueue(Record { "partial/after", timestamp, "application/json", "3", {} }), "subsequent record must enter the worker queue");
	check(waitFor([&worker] { return worker.status().deliveredRecords == 2; }, std::chrono::seconds(2)),
		"a permanent per-record failure must not block later uploads");
	worker.stop();
}


void detectsDuplicateTimestamp()
{
	ReductTransport transport(configuration());
	const auto timestamp = TimePoint::clock::now();
	const std::vector<Record> record { { "duplicates/value", timestamp, "application/json", "42", {} } };
	check(transport.writeBatch(record).delivered, "first immutable record write must succeed");
	check(!transport.writeBatch(record).delivered, "duplicate immutable timestamps must not be marked delivered");
}

void reportsUnavailableServer()
{
	auto config = configuration();
	config.serverUrl = "http://127.0.0.1:8384";
	ReductTransport transport(config);
	const std::vector<Record> record { { "unavailable/value", TimePoint::clock::now(), "application/json", "1", {} } };
	const auto result = transport.writeBatch(record);
	check(!result.delivered && result.retryable, "an unavailable server must be retryable and undelivered");
}

void workerStopsWithinShutdownTimeout()
{
	auto config = configuration();
	config.serverUrl = "http://127.0.0.1:8384";
	config.connectionTimeout = std::chrono::milliseconds(50);
	config.requestTimeout = std::chrono::milliseconds(100);
	config.shutdownTimeout = std::chrono::milliseconds(100);
	config.maxBatchRecords = 1;
	config.maxBatchAge = std::chrono::milliseconds(1);
	config.retry.maximumBackoff = std::chrono::seconds(1);
	UplinkWorker worker(config);
	worker.start();
	check(worker.enqueue(Record { "shutdown/value", TimePoint::clock::now(), "application/json", "1", {} }),
		"shutdown record must enter the queue");
	check(waitFor([&worker] { return !worker.status().error.empty(); }, std::chrono::seconds(2)),
		"worker must attempt the unavailable server");
	const auto started = std::chrono::steady_clock::now();
	worker.stop();
	check(std::chrono::steady_clock::now() - started < std::chrono::milliseconds(250),
		"worker shutdown must not wait for a retry backoff");
}

void workerRecoversQueuedRecordAfterServerRestart()
{
	auto config = configuration();
	config.maxBatchRecords = 1;
	config.maxBatchAge = std::chrono::milliseconds(1);
	config.retry.maximumBackoff = std::chrono::milliseconds(100);
	config.shutdownTimeout = std::chrono::seconds(2);
	UplinkWorker worker(config);
	worker.start();
	check(worker.enqueue(Record { "recovery/value", TimePoint::clock::now(), "application/json", "1", {} }),
		"recovery record must enter the queue");
	check(waitFor([&worker] { return !worker.status().error.empty(); }, std::chrono::seconds(3)),
		"worker must report the initial outage");
	check(waitFor([&worker] { return worker.status().deliveredRecords == 1; }, std::chrono::seconds(15)),
		"worker must deliver the queued record after server recovery");
	worker.stop();
}

}

int main()
{
	try
	{
		if (std::getenv("REDUCTSTORE_RECOVERY_TEST"))
		{
			workerRecoversQueuedRecordAfterServerRestart();
			return 0;
		}
		writesAndReadsMultipleEntries();
		writesAndReadsProductionSnapshot();
		detectsDuplicateTimestamp();
		reportsUnavailableServer();
		preservesConnectivityAfterDuplicateRecordFailure();
		workerCompletesSuccessfulRecordsFromPartialBatch();
	}
	catch (const std::exception& error)
	{
		std::cerr << error.what() << '\n';
		return 1;
	}
	return 0;
}
