#pragma once

#include <chrono>
#include <cstddef>
#include <string>
#include <string_view>

namespace reductstore_xentara
{

enum class OverflowPolicy
{
	RejectNewest,
	DropOldest,
};

struct RetryPolicy
{
	std::size_t maxAttempts { 3 };
	std::chrono::milliseconds initialBackoff { 100 };
	std::chrono::milliseconds maximumBackoff { 2000 };
};

struct ClientConfiguration
{
	std::string serverUrl;
	std::string apiToken;
	std::string bucket;
	std::chrono::milliseconds connectionTimeout { 2000 };
	std::chrono::milliseconds requestTimeout { 10000 };
	bool createBucket { false };
	std::size_t maxBatchRecords { 64 };
	std::size_t maxBatchPayloadBytes { 4U * 1024U * 1024U };
	std::size_t maxBatchMetadataBytes { 6U * 1024U };
	std::chrono::milliseconds maxBatchAge { 2000 };
	std::size_t maxQueueBytes { 16U * 1024U * 1024U };
	OverflowPolicy overflowPolicy { OverflowPolicy::RejectNewest };
	RetryPolicy retry {};
};

[[nodiscard]] auto validate(const ClientConfiguration& configuration) -> std::string;
[[nodiscard]] auto entryName(std::string_view prefix, std::string_view entry) -> std::string;

}
