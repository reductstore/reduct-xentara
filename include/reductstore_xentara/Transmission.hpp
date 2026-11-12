#pragma once

#include "reductstore_xentara/Batching.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace reductstore_xentara
{

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

struct PartialBatchOutcome
{
	std::vector<Record> retry;
	std::vector<Record> succeeded;
	std::vector<std::pair<Record, RecordFailure>> permanent;
};

[[nodiscard]] auto partitionPartialBatch(std::vector<Record> records, const UploadResult& result) -> PartialBatchOutcome;

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
	[[nodiscard]] auto enqueue(Record record) -> bool;
	[[nodiscard]] auto status() const -> DeliveryStatus;

private:
	struct RunState
	{
		explicit RunState(const ClientConfiguration& configuration);

		BatchPlanner planner;
		std::vector<Record> pending;
		std::optional<Record> deferred;
		std::optional<std::chrono::steady_clock::time_point> shutdownDeadline;
	};

	auto run() -> void;
	auto setFailure(int code, std::string error) -> void;
	auto waitWhileRunning(std::chrono::milliseconds delay) -> void;
	[[nodiscard]] auto shutdownExpired(const RunState& state) const -> bool;
	[[nodiscard]] auto requestFitsShutdown(const RunState& state) const -> bool;
	auto releaseUndelivered(RunState& state) -> void;
	[[nodiscard]] auto admitRecord(RunState& state, Record record) -> bool;
	[[nodiscard]] auto applyPartialResult(RunState& state, const UploadResult& result, int transientCode) -> bool;
	[[nodiscard]] auto sendWithRetry(ReductTransport& transport, RunState& state, bool& partialResolved)
		-> std::optional<UploadResult>;
	auto finishBatch(RunState& state, const UploadResult& result, bool partialResolved) -> void;
	auto accountSuccess(std::vector<Record>& records) -> void;
	auto accountFailure(std::vector<Record>& records, int code, std::string message) -> void;

	ClientConfiguration _configuration;
	BoundedQueue _queue;
	std::atomic<bool> _running { false };
	std::atomic<std::size_t> _inFlightRecords { 0 };
	std::thread _thread;
	mutable std::mutex _statusMutex;
	DeliveryStatus _status;
};

}
