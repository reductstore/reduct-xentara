#include "reductstore_xentara/Transmission.hpp"

#include <reduct/client.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
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

auto environment(const char* name, const char* fallback) -> std::string
{
	if (const auto* value = std::getenv(name))
	{
		return value;
	}
	return fallback;
}

auto configuration() -> ClientConfiguration
{
	ClientConfiguration config;
	config.serverUrl = environment("REDUCTSTORE_URL", "http://127.0.0.1:8383");
	config.apiToken = environment("REDUCTSTORE_TOKEN", "xentara-demo-token");
	config.bucket = "xentara-integration";
	config.createBucket = true;
	config.maxBatchRecords = 256;
	config.maxBatchPayloadBytes = 4U * 1024U * 1024U;
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

void streamsTwentyMiBFile()
{
	const auto file = std::filesystem::temp_directory_path() / "reductstore-xentara-integration.bin";
	{
		std::ofstream output(file, std::ios::binary);
		std::string chunk(1024U * 1024U, 'z');
		for (int index = 0; index < 20; ++index)
		{
			output.write(chunk.data(), static_cast<std::streamsize>(chunk.size()));
		}
	}
	FilePayload payload(file);
	ReductTransport transport(configuration());
	const auto timestamp = TimePoint::clock::now();
	const Record metadata { "binary/large", timestamp, "application/octet-stream", {}, { { "source", "integration" } } };
	check(transport.writeFile(metadata, payload).delivered, "chunked twenty MiB write must succeed");
	std::filesystem::remove(file);
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

}

int main()
{
	try
	{
		writesAndReadsMultipleEntries();
		streamsTwentyMiBFile();
		detectsDuplicateTimestamp();
		reportsUnavailableServer();
	}
	catch (const std::exception& error)
	{
		std::cerr << error.what() << '\n';
		return 1;
	}
	return 0;
}
