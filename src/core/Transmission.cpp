#include "reductstore_xentara/Transmission.hpp"

#include <reduct/bucket.h>
#include <reduct/client.h>

#include <algorithm>
#include <chrono>
#include <iterator>
#include <map>
#include <stdexcept>
#include <utility>

namespace reductstore_xentara
{

namespace
{
class TransportException final : public std::runtime_error
{
public:
	TransportException(int code, std::string message) : std::runtime_error(std::move(message)), code(code) {}
	int code;
};

auto message(const reduct::Error& error) -> std::string
{
	return error.ToString();
}

}

class ReductTransport::Implementation final
{
public:
	explicit Implementation(ClientConfiguration configuration) :
		configuration(std::move(configuration))
	{
	}

	auto bucket() -> reduct::IBucket*
	{
		if (!_client)
		{
			reduct::HttpOptions options {};
			options.api_token = configuration.apiToken;
			options.ssl_verification = true;
			options.connection_timeout = configuration.connectionTimeout;
			options.request_timeout = configuration.requestTimeout;
			_client = reduct::IClient::Build(configuration.serverUrl, options);
			if (!_client)
			{
				throw TransportException(-1, "could not create ReductStore client");
			}
		}
		if (!_bucket)
		{
			auto result = configuration.createBucket ? _client->GetOrCreateBucket(configuration.bucket) : _client->GetBucket(configuration.bucket);
			if (result.error)
			{
				throw TransportException(result.error.code, message(result.error));
			}
			_bucket = std::move(result.result);
		}
		return _bucket.get();
	}

	ClientConfiguration configuration;
	std::unique_ptr<reduct::IClient> _client;
	std::unique_ptr<reduct::IBucket> _bucket;
};

ReductTransport::ReductTransport(ClientConfiguration configuration) :
	_implementation(std::make_unique<Implementation>(std::move(configuration)))
{
}

ReductTransport::~ReductTransport() = default;
ReductTransport::ReductTransport(ReductTransport&&) noexcept = default;
auto ReductTransport::operator=(ReductTransport&&) noexcept -> ReductTransport& = default;

auto ReductTransport::writeBatch(const std::vector<Record>& records) -> UploadResult
{
	std::map<std::pair<std::string, std::int64_t>, std::size_t> identityCounts;
	for (const auto& record : records)
	{
		++identityCounts[{ record.entry, unixMicroseconds(normalizeTimestamp(record.timestamp)) }];
	}
	if (std::ranges::any_of(identityCounts, [](const auto& item) { return item.second > 1; }))
	{
		return { false, false, 409, "duplicate record identity in batch", {} };
	}
	try
	{
		auto result = _implementation->bucket()->WriteBatch([&records](reduct::IBucket::Batch* batch) {
			for (const auto& record : records)
			{
				batch->AddRecord(record.entry, record.timestamp, record.payload, record.contentType, record.labels);
			}
		});
		if (result.error)
		{
			return { false, isRetryableError(result.error.code), result.error.code, message(result.error), {} };
		}
		if (!result.result.empty())
		{
			std::vector<RecordFailure> failures;
			for (const auto& [entry, errors] : result.result)
			{
				for (const auto& [timestamp, error] : errors)
				{
					failures.push_back({ { entry, timestamp }, error.code, message(error) });
				}
			}
			return UploadResult::partial(std::move(failures));
		}
		return UploadResult::success();
	}
	catch (const TransportException& error)
	{
		return { false, isRetryableError(error.code), error.code, error.what(), {} };
	}
	catch (const std::exception& error)
	{
		return UploadResult::transportFailure(-1, error.what());
	}
}

auto partitionPartialBatch(std::vector<Record> records, const UploadResult& result) -> PartialBatchOutcome
{
	PartialBatchOutcome outcome;
	for (auto& record : records)
	{
		const RecordIdentity identity { record.entry, normalizeTimestamp(record.timestamp) };
		const auto failure = std::ranges::find_if(result.recordFailures,
			[&identity](const RecordFailure& item) {
				return item.identity.entry == identity.entry && normalizeTimestamp(item.identity.timestamp) == identity.timestamp;
			});
		if (failure == result.recordFailures.end())
		{
			outcome.succeeded.push_back(std::move(record));
		}
		else if (isRetryableError(failure->code))
		{
			outcome.retry.push_back(std::move(record));
		}
		else
		{
			outcome.permanent.emplace_back(std::move(record), *failure);
		}
	}
	return outcome;
}

UplinkWorker::RunState::RunState(const ClientConfiguration& configuration) :
	planner({
		configuration.maxBatchRecords,
		configuration.maxBatchPayloadBytes,
		configuration.maxBatchMetadataBytes,
		configuration.maxBatchAge,
	})
{
}

UplinkWorker::UplinkWorker(ClientConfiguration configuration) :
	_configuration(std::move(configuration)),
	_queue(_configuration.maxQueueBytes, _configuration.overflowPolicy)
{
}

UplinkWorker::~UplinkWorker()
{
	stop();
}

auto UplinkWorker::start() -> void
{
	if (_running.exchange(true))
	{
		return;
	}
	_thread = std::thread(&UplinkWorker::run, this);
}

auto UplinkWorker::stop() -> void
{
	if (!_running.exchange(false))
	{
		return;
	}
	_queue.close();
	if (_thread.joinable())
	{
		_thread.join();
	}
}

auto UplinkWorker::enqueue(Record record) -> bool
{
	const auto producerId = record.producerId;
	auto result = _queue.push(std::move(record));
	std::lock_guard lock(_statusMutex);
	if (!result.accepted)
	{
		++_status.failedRecords;
		++_status.failedRecordsByProducer[producerId];
		++_status.rejectedRecordsByProducer[producerId];
		_status.lastErrorCode = 507;
		_status.lastErrorCodeByProducer[producerId] = 507;
		_status.error = "record rejected because the delivery queue is full";
		return false;
	}
	for (const auto& dropped : result.droppedRecords)
	{
		++_status.failedRecords;
		++_status.failedRecordsByProducer[dropped.producerId];
		++_status.droppedRecordsByProducer[dropped.producerId];
		_status.lastErrorCode = 507;
		_status.lastErrorCodeByProducer[dropped.producerId] = 507;
		_status.error = "record dropped because the delivery queue is full";
	}
	return true;
}

auto UplinkWorker::status() const -> DeliveryStatus
{
	std::lock_guard lock(_statusMutex);
	auto status = _status;
	status.queuedRecords = _queue.size();
	status.inFlightRecords = _inFlightRecords.load();
	status.queuedBytes = _queue.residentBytes();
	return status;
}

auto UplinkWorker::setFailure(int code, std::string error) -> void
{
	std::lock_guard lock(_statusMutex);
	_status.connected = false;
	_status.lastErrorCode = code;
	_status.error = std::move(error);
}

auto UplinkWorker::waitWhileRunning(std::chrono::milliseconds delay) -> void
{
	const auto until = std::chrono::steady_clock::now() + delay;
	while (_running && std::chrono::steady_clock::now() < until)
	{
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
}

auto UplinkWorker::shutdownExpired(const RunState& state) const -> bool
{
	return state.shutdownDeadline && std::chrono::steady_clock::now() >= *state.shutdownDeadline;
}

auto UplinkWorker::requestFitsShutdown(const RunState& state) const -> bool
{
	return !state.shutdownDeadline ||
		std::chrono::steady_clock::now() + _configuration.requestTimeout <= *state.shutdownDeadline;
}

auto UplinkWorker::accountSuccess(std::vector<Record>& records) -> void
{
	if (records.empty())
	{
		return;
	}
	for (const auto& record : records)
	{
		_queue.release(record);
	}
	std::lock_guard lock(_statusMutex);
	_status.connected = true;
	_status.lastSuccessfulSendTime = TimePoint::clock::now();
	for (const auto& record : records)
	{
		++_status.deliveredRecordsByProducer[record.producerId];
	}
	_status.deliveredRecords += records.size();
}

auto UplinkWorker::accountFailure(std::vector<Record>& records, int code, std::string message) -> void
{
	if (records.empty())
	{
		return;
	}
	for (const auto& record : records)
	{
		_queue.release(record);
	}
	std::lock_guard lock(_statusMutex);
	_status.lastErrorCode = code;
	_status.error = std::move(message);
	_status.failedRecords += records.size();
	for (const auto& record : records)
	{
		++_status.failedRecordsByProducer[record.producerId];
		_status.lastErrorCodeByProducer[record.producerId] = code;
	}
}

auto UplinkWorker::releaseUndelivered(RunState& state) -> void
{
	std::vector<Record> undelivered = std::move(state.pending);
	state.pending.clear();
	if (state.deferred)
	{
		undelivered.push_back(std::move(*state.deferred));
		state.deferred.reset();
	}
	auto planned = state.planner.take();
	undelivered.insert(undelivered.end(), std::make_move_iterator(planned.begin()), std::make_move_iterator(planned.end()));
	while (auto record = _queue.tryPop())
	{
		undelivered.push_back(std::move(*record));
	}
	_inFlightRecords = 0;
	accountFailure(undelivered, -1, "shutdown final flush released undelivered records");
}

auto UplinkWorker::admitRecord(RunState& state, Record record) -> bool
{
	if (state.planner.accepts(record))
	{
		if (!state.planner.push(std::move(record), std::chrono::steady_clock::now()))
		{
			throw std::logic_error("accepted record could not enter the batch planner");
		}
	}
	else if (state.planner.empty())
	{
		std::vector<Record> oversized;
		oversized.push_back(std::move(record));
		accountFailure(oversized, 413, "record exceeds the configured batch limits");
		setFailure(413, "record exceeds the configured batch limits");
		_inFlightRecords = state.deferred ? 1 : 0;
		return false;
	}
	else
	{
		state.deferred = std::move(record);
	}
	_inFlightRecords = state.planner.size() + (state.deferred ? 1 : 0);
	return true;
}

auto UplinkWorker::applyPartialResult(RunState& state, const UploadResult& result, int transientCode) -> bool
{
	{
		std::lock_guard lock(_statusMutex);
		_status.connected = true;
	}
	auto outcome = partitionPartialBatch(std::move(state.pending), result);
	accountSuccess(outcome.succeeded);
	for (auto& [record, failure] : outcome.permanent)
	{
		std::vector<Record> failed;
		failed.push_back(std::move(record));
		accountFailure(failed, failure.code, failure.message);
	}
	state.pending = std::move(outcome.retry);
	{
		std::lock_guard lock(_statusMutex);
		for (const auto& record : state.pending)
		{
			_status.lastErrorCodeByProducer[record.producerId] = transientCode;
		}
	}
	_inFlightRecords = state.pending.size() + (state.deferred ? 1 : 0);
	return state.pending.empty();
}

auto UplinkWorker::sendWithRetry(ReductTransport& transport, RunState& state, bool& partialResolved)
	-> std::optional<UploadResult>
{
	UploadResult result;
	for (std::size_t attempt = 0; attempt < _configuration.retry.maxAttempts; ++attempt)
	{
		if (!requestFitsShutdown(state))
		{
			releaseUndelivered(state);
			setFailure(-1, "shutdown final flush timed out before another HTTP request");
			return std::nullopt;
		}
		result = transport.writeBatch(state.pending);
		if (!result.delivered && !result.recordFailures.empty())
		{
			int transientCode = 0;
			for (const auto& failure : result.recordFailures)
			{
				if (isRetryableError(failure.code))
				{
					transientCode = failure.code;
				}
			}
			partialResolved = applyPartialResult(state, result, transientCode);
			if (partialResolved)
			{
				return result;
			}
		}
		if (result.delivered || !result.retryable || attempt + 1 == _configuration.retry.maxAttempts || !_running)
		{
			break;
		}
		const auto multiplier = std::int64_t { 1 } << attempt;
		const auto delay = std::min(_configuration.retry.maximumBackoff, _configuration.retry.initialBackoff * multiplier);
		waitWhileRunning(delay);
	}
	return result;
}

auto UplinkWorker::finishBatch(RunState& state, const UploadResult& result, bool partialResolved) -> void
{
	if (result.delivered)
	{
		accountSuccess(state.pending);
		{
			std::lock_guard lock(_statusMutex);
			_status.error.clear();
			_status.lastErrorCode = 0;
		}
		state.pending.clear();
	}
	else if (!partialResolved)
	{
		if (!result.retryable)
		{
			accountFailure(state.pending, result.errorCode, result.error);
			setFailure(result.errorCode, result.error);
			state.pending.clear();
		}
		else
		{
			{
				std::lock_guard lock(_statusMutex);
				for (const auto& record : state.pending)
				{
					_status.lastErrorCodeByProducer[record.producerId] = result.errorCode;
				}
			}
			setFailure(result.errorCode, result.error);
		}
	}
	_inFlightRecords = state.pending.size() + (state.deferred ? 1 : 0);
}

auto UplinkWorker::run() -> void
{
	ReductTransport transport(_configuration);
	RunState state(_configuration);
	while (_running || !state.planner.empty() || _queue.size() != 0 || !state.pending.empty() || state.deferred)
	{
		if (!_running && !state.shutdownDeadline)
		{
			state.shutdownDeadline = std::chrono::steady_clock::now() + _configuration.shutdownTimeout;
		}
		if (shutdownExpired(state))
		{
			releaseUndelivered(state);
			setFailure(-1, "shutdown final flush timed out with undelivered records");
			return;
		}
		if (state.pending.empty())
		{
			std::optional<Record> record;
			if (state.deferred)
			{
				record = std::move(*state.deferred);
				state.deferred.reset();
			}
			else
			{
				record = _queue.waitPop(std::chrono::milliseconds(50));
			}
			if (record && !admitRecord(state, std::move(*record)))
			{
				continue;
			}
			if (state.planner.empty())
			{
				continue;
			}
			if (!state.deferred && !state.planner.shouldFlush(std::chrono::steady_clock::now()) && _running)
			{
				continue;
			}
			state.pending = state.planner.take();
			_inFlightRecords = state.pending.size() + (state.deferred ? 1 : 0);
		}
		bool partialResolved = false;
		auto result = sendWithRetry(transport, state, partialResolved);
		if (!result)
		{
			return;
		}
		finishBatch(state, *result, partialResolved);
		if (result->retryable && !state.pending.empty())
		{
			waitWhileRunning(_configuration.retry.maximumBackoff);
		}
	}
}

}
