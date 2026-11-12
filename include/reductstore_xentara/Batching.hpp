#pragma once

#include "reductstore_xentara/Configuration.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <vector>

namespace reductstore_xentara
{

using TimePoint = std::chrono::time_point<std::chrono::system_clock>;
using Labels = std::map<std::string, std::string>;

[[nodiscard]] auto normalizeTimestamp(TimePoint timestamp) -> TimePoint;
[[nodiscard]] auto unixMicroseconds(TimePoint timestamp) -> std::int64_t;

struct Record
{
	std::string entry;
	TimePoint timestamp;
	std::string contentType;
	std::string payload;
	Labels labels;
	std::uint64_t producerId { 0 };
	[[nodiscard]] auto storageBytes() const noexcept -> std::size_t;
	[[nodiscard]] auto metadataBytes() const noexcept -> std::size_t;
};

struct RecordIdentity
{
	std::string entry;
	TimePoint timestamp;

	auto operator==(const RecordIdentity&) const -> bool = default;
};

struct RecordFailure
{
	RecordIdentity identity;
	int code;
	std::string message;
};

struct BatchLimits
{
	std::size_t maxRecords { 64 };
	std::size_t maxPayloadBytes { 4U * 1024U * 1024U };
	std::size_t maxMetadataBytes { 6U * 1024U };
	std::chrono::milliseconds maxAge { 2000 };
};

class BatchPlanner final
{
public:
	explicit BatchPlanner(BatchLimits limits);
	[[nodiscard]] auto accepts(const Record& record) const noexcept -> bool;
	[[nodiscard]] auto push(Record record, std::chrono::steady_clock::time_point collectedAt) -> bool;
	[[nodiscard]] auto shouldFlush(std::chrono::steady_clock::time_point now) const -> bool;
	[[nodiscard]] auto take() -> std::vector<Record>;
	[[nodiscard]] auto empty() const noexcept -> bool;
	[[nodiscard]] auto size() const noexcept -> std::size_t;
private:
	BatchLimits _limits;
	std::vector<Record> _records;
	std::size_t _payloadBytes { 0 };
	std::size_t _metadataBytes { 0 };
	std::optional<std::chrono::steady_clock::time_point> _firstCollectedAt;
};

struct QueuePushResult
{
	bool accepted { false };
	std::vector<Record> droppedRecords;
};

class BoundedQueue final
{
public:
	explicit BoundedQueue(std::size_t maxPayloadBytes, OverflowPolicy policy);
	[[nodiscard]] auto push(Record record) -> QueuePushResult;
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
	std::size_t _queuedBytes { 0 };
	std::size_t _rejectedRecords { 0 };
	std::size_t _droppedRecords { 0 };
	bool _closed { false };
};

[[nodiscard]] inline auto isRetryableError(int code) -> bool
{
	return code == -1 || code == 408 || code == 429 || code >= 500;
}

struct UploadResult
{
	bool delivered { false };
	bool retryable { false };
	int errorCode { 0 };
	std::string error;
	std::vector<RecordFailure> recordFailures;

	[[nodiscard]] static auto success() -> UploadResult;
	[[nodiscard]] static auto transportFailure(int errorCode, std::string error) -> UploadResult;
	[[nodiscard]] static auto partial(std::vector<RecordFailure> recordFailures) -> UploadResult;
};



}
