#pragma once

#include "reductstore_xentara/Batching.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace reductstore_xentara
{

struct EnqueueResult
{
	bool accepted { false };
	std::optional<std::uint64_t> droppedProducerId;
};

struct DeliveryStatus
{
	bool connected { false };
	std::size_t deliveredRecords { 0 };
	std::map<std::uint64_t, std::size_t> deliveredRecordsByProducer;
	std::size_t failedRecords { 0 };
	std::map<std::uint64_t, std::size_t> failedRecordsByProducer;
	std::map<std::uint64_t, std::size_t> rejectedRecordsByProducer;
	std::map<std::uint64_t, std::size_t> droppedRecordsByProducer;
	std::map<std::uint64_t, int> lastErrorCodeByProducer;
	std::size_t queuedRecords { 0 };
	std::size_t inFlightRecords { 0 };
	std::size_t queuedBytes { 0 };
	TimePoint lastSuccessfulSendTime { TimePoint::min() };
	std::string error;
	int lastErrorCode { 0 };
};

class ReductTransport final
{
public:
	explicit ReductTransport(ClientConfiguration configuration);
	~ReductTransport();
	ReductTransport(ReductTransport&&) noexcept;
	auto operator=(ReductTransport&&) noexcept -> ReductTransport&;
	ReductTransport(const ReductTransport&) = delete;
	auto operator=(const ReductTransport&) -> ReductTransport& = delete;

	[[nodiscard]] auto writeBatch(const std::vector<Record>& records) -> UploadResult;

private:
	class Implementation;
	std::unique_ptr<Implementation> _implementation;
};

class UplinkWorker final
{
public:
	explicit UplinkWorker(ClientConfiguration configuration);
	~UplinkWorker();
	UplinkWorker(const UplinkWorker&) = delete;
	auto operator=(const UplinkWorker&) -> UplinkWorker& = delete;

	auto start() -> void;
	auto stop() -> void;
	[[nodiscard]] auto enqueue(Record record) -> EnqueueResult;
	[[nodiscard]] auto status() const -> DeliveryStatus;
	[[nodiscard]] auto rejectedRecords() const -> std::size_t;
	[[nodiscard]] auto droppedRecords() const -> std::size_t;

private:
	auto run() -> void;
	auto setFailure(int code, std::string error) -> void;

	ClientConfiguration _configuration;
	BoundedQueue _queue;
	std::atomic<bool> _running { false };
	std::atomic<std::size_t> _inFlightRecords { 0 };
	std::thread _thread;
	mutable std::mutex _statusMutex;
	DeliveryStatus _status;
};

}
