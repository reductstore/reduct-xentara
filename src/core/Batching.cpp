#include "reductstore_xentara/Batching.hpp"

#include <algorithm>
#include <fstream>
#include <stdexcept>
#include <thread>
#include <utility>

namespace reductstore_xentara
{

auto Record::storageBytes() const noexcept -> std::size_t
{
	auto bytes = sizeof(Record) + entry.capacity() + contentType.capacity() + payload.capacity();
	for (const auto& [key, value] : labels)
	{
		bytes += sizeof(Labels::value_type) + key.capacity() + value.capacity();
	}
	return bytes;
}

BatchPlanner::BatchPlanner(BatchLimits limits) :
	_limits(std::move(limits))
{
}

auto BatchPlanner::push(Record record, std::chrono::steady_clock::time_point collectedAt) -> void
{
	if (_records.empty())
	{
		_firstCollectedAt = collectedAt;
	}
	_payloadBytes += record.payload.size();
	_records.push_back(std::move(record));
}

auto BatchPlanner::shouldFlush(std::chrono::steady_clock::time_point now) const -> bool
{
	return !_records.empty() &&
		(_records.size() >= _limits.maxRecords || _payloadBytes >= _limits.maxPayloadBytes || now - *_firstCollectedAt >= _limits.maxAge);
}

auto BatchPlanner::take() -> std::vector<Record>
{
	_payloadBytes = 0;
	_firstCollectedAt.reset();
	return std::exchange(_records, {});
}

auto BatchPlanner::empty() const noexcept -> bool
{
	return _records.empty();
}

BoundedQueue::BoundedQueue(std::size_t maxPayloadBytes, OverflowPolicy policy) :
	_maxPayloadBytes(maxPayloadBytes),
	_policy(policy)
{
}

auto BoundedQueue::push(Record record) -> bool
{
	const auto bytes = storageBytes(record);
	std::lock_guard lock(_mutex);
	if (_closed || bytes > _maxPayloadBytes)
	{
		++_rejectedRecords;
		return false;
	}
	if (_residentBytes + bytes > _maxPayloadBytes)
	{
		if (_policy == OverflowPolicy::RejectNewest)
		{
			++_rejectedRecords;
			return false;
		}
		while (!_records.empty() && _residentBytes + bytes > _maxPayloadBytes)
		{
			_residentBytes -= storageBytes(_records.front());
			_records.pop();
			++_droppedRecords;
		}
		if (_residentBytes + bytes > _maxPayloadBytes)
		{
			++_rejectedRecords;
			return false;
		}
	}
	_residentBytes += bytes;
	_records.push(std::move(record));
	_changed.notify_one();
	return true;
}

auto BoundedQueue::tryPop() -> std::optional<Record>
{
	std::lock_guard lock(_mutex);
	if (_records.empty())
	{
		return std::nullopt;
	}
	auto record = std::move(_records.front());
	_records.pop();
	return record;
}

auto BoundedQueue::waitPop(std::chrono::milliseconds timeout) -> std::optional<Record>
{
	std::unique_lock lock(_mutex);
	_changed.wait_for(lock, timeout, [this] { return _closed || !_records.empty(); });
	if (_records.empty())
	{
		return std::nullopt;
	}
	auto record = std::move(_records.front());
	_records.pop();
	return record;
}

auto BoundedQueue::release(const Record& record) -> void
{
	const auto bytes = storageBytes(record);
	std::lock_guard lock(_mutex);
	if (bytes > _residentBytes)
	{
		throw std::logic_error("released record exceeds reserved queue memory");
	}
	_residentBytes -= bytes;
}

auto BoundedQueue::close() -> void
{
	std::lock_guard lock(_mutex);
	_closed = true;
	_changed.notify_all();
}

auto BoundedQueue::size() const -> std::size_t
{
	std::lock_guard lock(_mutex);
	return _records.size();
}

auto BoundedQueue::residentBytes() const -> std::size_t
{
	std::lock_guard lock(_mutex);
	return _residentBytes;
}

auto BoundedQueue::rejectedRecords() const -> std::size_t
{
	std::lock_guard lock(_mutex);
	return _rejectedRecords;
}

auto BoundedQueue::droppedRecords() const -> std::size_t
{
	std::lock_guard lock(_mutex);
	return _droppedRecords;
}

auto BoundedQueue::storageBytes(const Record& record) -> std::size_t
{
	return record.storageBytes();
}

auto UploadResult::success() -> UploadResult
{
	return { true, false, {}, {} };
}

auto UploadResult::transportFailure(std::string error) -> UploadResult
{
	return { false, true, std::move(error), {} };
}

auto UploadResult::partial(std::vector<RecordIdentity> failedRecords) -> UploadResult
{
	return { false, false, "partial batch failure", std::move(failedRecords) };
}

FilePayload::FilePayload(std::filesystem::path path) :
	_path(std::move(path)),
	_size(std::filesystem::file_size(_path))
{
}

auto FilePayload::size() const noexcept -> std::size_t
{
	return _size;
}

auto FilePayload::read(std::size_t offset, std::size_t maximumBytes) const -> std::string
{
	if (offset >= _size)
	{
		return {};
	}
	const auto bytes = std::min(maximumBytes, _size - offset);
	std::ifstream file(_path, std::ios::binary);
	if (!file)
	{
		throw std::runtime_error("could not open streaming payload");
	}
	file.seekg(static_cast<std::streamoff>(offset));
	std::string chunk(bytes, '\0');
	file.read(chunk.data(), static_cast<std::streamsize>(chunk.size()));
	chunk.resize(static_cast<std::size_t>(file.gcount()));
	return chunk;
}

}
