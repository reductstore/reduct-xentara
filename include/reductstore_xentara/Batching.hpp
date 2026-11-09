#pragma once

#include "reductstore_xentara/Configuration.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace reductstore_xentara
{

using TimePoint = std::chrono::time_point<std::chrono::system_clock>;
using Labels = std::map<std::string, std::string>;

struct Record
{
	std::string entry;
	TimePoint timestamp;
	std::string contentType;
	std::string payload;
	Labels labels;

	[[nodiscard]] auto storageBytes() const noexcept -> std::size_t;
};

struct RecordIdentity
{
	std::string entry;
	TimePoint timestamp;

	auto operator==(const RecordIdentity&) const -> bool = default;
};

struct BatchLimits
{
	std::size_t maxRecords { 256 };
	std::size_t maxPayloadBytes { 4U * 1024U * 1024U };
	std::chrono::milliseconds maxAge { 2000 };
};

class BatchPlanner final
{
public:
	explicit BatchPlanner(BatchLimits limits);
	auto push(Record record, std::chrono::steady_clock::time_point collectedAt) -> void;
	[[nodiscard]] auto shouldFlush(std::chrono::steady_clock::time_point now) const -> bool;
	[[nodiscard]] auto take() -> std::vector<Record>;
	[[nodiscard]] auto empty() const noexcept -> bool;

private:
	BatchLimits _limits;
	std::vector<Record> _records;
	std::size_t _payloadBytes { 0 };
	std::optional<std::chrono::steady_clock::time_point> _firstCollectedAt;
};

class BoundedQueue final
{
public:
	explicit BoundedQueue(std::size_t maxPayloadBytes, OverflowPolicy policy);
	auto push(Record record) -> bool;
	[[nodiscard]] auto tryPop() -> std::optional<Record>;
	[[nodiscard]] auto waitPop(std::chrono::milliseconds timeout) -> std::optional<Record>;
	auto release(const Record& record) -> void;
	auto close() -> void;
	[[nodiscard]] auto size() const -> std::size_t;
	[[nodiscard]] auto residentBytes() const -> std::size_t;
	[[nodiscard]] auto rejectedRecords() const -> std::size_t;
	[[nodiscard]] auto droppedRecords() const -> std::size_t;

private:
	[[nodiscard]] static auto storageBytes(const Record& record) -> std::size_t;

	std::size_t _maxPayloadBytes;
	OverflowPolicy _policy;
	mutable std::mutex _mutex;
	std::condition_variable _changed;
	std::queue<Record> _records;
	std::size_t _residentBytes { 0 };
	std::size_t _rejectedRecords { 0 };
	std::size_t _droppedRecords { 0 };
	bool _closed { false };
};

struct UploadResult
{
	bool delivered { false };
	bool retryable { false };
	std::string error;
	std::vector<RecordIdentity> failedRecords;

	[[nodiscard]] static auto success() -> UploadResult;
	[[nodiscard]] static auto transportFailure(std::string error) -> UploadResult;
	[[nodiscard]] static auto partial(std::vector<RecordIdentity> failedRecords) -> UploadResult;
};

template <typename Upload>
auto retryTransient(const RetryPolicy& policy, Upload&& upload) -> UploadResult
{
	UploadResult result;
	for (std::size_t attempt = 0; attempt < policy.maxAttempts; ++attempt)
	{
		result = std::invoke(upload);
		if (result.delivered || !result.retryable || attempt + 1 == policy.maxAttempts)
		{
			return result;
		}
		const auto multiplier = std::int64_t { 1 } << attempt;
		const auto delay = std::min(policy.maximumBackoff, policy.initialBackoff * multiplier);
		if (delay.count() > 0)
		{
			std::this_thread::sleep_for(delay);
		}
	}
	return result;
}

class FilePayload final
{
public:
	explicit FilePayload(std::filesystem::path path);
	[[nodiscard]] auto size() const noexcept -> std::size_t;
	[[nodiscard]] auto read(std::size_t offset, std::size_t maximumBytes) const -> std::string;

private:
	std::filesystem::path _path;
	std::size_t _size;
};

}
