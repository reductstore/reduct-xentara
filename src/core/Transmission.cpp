#include "reductstore_xentara/Transmission.hpp"

#include <reduct/bucket.h>
#include <reduct/client.h>

#include <algorithm>
#include <chrono>
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

auto retryable(int code) -> bool
{
	return code == -1 || code == 408 || code == 429 || code >= 500;
}

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
	std::vector<RecordFailure> duplicates;
	for (const auto& record : records)
	{
		const auto key = std::pair { record.entry, unixMicroseconds(normalizeTimestamp(record.timestamp)) };
		if (identityCounts.at(key) > 1)
		{
			duplicates.push_back({ { record.entry, normalizeTimestamp(record.timestamp) }, 409, "duplicate record identity in batch" });
		}
	}
	if (!duplicates.empty())
	{
		return UploadResult::partial(std::move(duplicates));
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
			return { false, retryable(result.error.code), result.error.code, message(result.error), {} };
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
		return { false, retryable(error.code), error.code, error.what(), {} };
	}
	catch (const std::exception& error)
	{
		return UploadResult::transportFailure(-1, error.what());
	}
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

auto UplinkWorker::enqueue(Record record) -> EnqueueResult
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
		return {};
	}
	EnqueueResult enqueueResult { true, std::nullopt };
	for (const auto& dropped : result.droppedRecords)
	{
		++_status.failedRecords;
		++_status.failedRecordsByProducer[dropped.producerId];
		++_status.droppedRecordsByProducer[dropped.producerId];
		_status.lastErrorCode = 507;
		_status.lastErrorCodeByProducer[dropped.producerId] = 507;
		_status.error = "record dropped because the delivery queue is full";
		if (!enqueueResult.droppedProducerId)
		{
			enqueueResult.droppedProducerId = dropped.producerId;
		}
	}
	return enqueueResult;
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

auto UplinkWorker::rejectedRecords() const -> std::size_t
{
	return _queue.rejectedRecords();
}

auto UplinkWorker::droppedRecords() const -> std::size_t
{
	return _queue.droppedRecords();
}

auto UplinkWorker::setFailure(int code, std::string error) -> void
{
	std::lock_guard lock(_statusMutex);
	_status.connected = false;
	_status.lastErrorCode = code;
	_status.error = std::move(error);
}

auto UplinkWorker::run() -> void
{
	ReductTransport transport(_configuration);
	BatchPlanner planner({
		_configuration.maxBatchRecords,
		_configuration.maxBatchPayloadBytes,
		_configuration.maxBatchMetadataBytes,
		_configuration.maxBatchAge,
	});
	std::vector<Record> pending;
	std::optional<Record> deferred;
	std::optional<std::chrono::steady_clock::time_point> shutdownDeadline;
	auto releaseUndelivered = [this, &planner, &pending, &deferred] {
		auto undelivered = pending.size();
		for (const auto& record : pending)
		{
			_queue.release(record);
		}
		pending.clear();
		if (deferred)
		{
			_queue.release(*deferred);
			deferred.reset();
			++undelivered;
		}
		for (const auto& record : planner.take())
		{
			_queue.release(record);
			++undelivered;
		}
		while (auto record = _queue.tryPop())
		{
			_queue.release(*record);
			++undelivered;
		}
		_inFlightRecords = 0;
		return undelivered;
	};
	while (_running || !planner.empty() || _queue.size() != 0 || !pending.empty() || deferred)
	{
		if (!_running && !shutdownDeadline)
		{
			shutdownDeadline = std::chrono::steady_clock::now() + _configuration.shutdownTimeout;
		}
		if (shutdownDeadline && std::chrono::steady_clock::now() >= *shutdownDeadline)
		{
			const auto undelivered = releaseUndelivered();
			{
				std::lock_guard lock(_statusMutex);
				_status.failedRecords += undelivered;
			}
			setFailure(-1, "shutdown final flush timed out with undelivered records");
			return;
		}
		if (pending.empty())
		{
			std::optional<Record> record;
			if (deferred)
			{
				record = std::move(deferred);
				deferred.reset();
			}
			else
			{
				record = _queue.waitPop(std::chrono::milliseconds(50));
			}
			if (record)
			{
				if (planner.accepts(*record))
				{
					if (!planner.push(std::move(*record), std::chrono::steady_clock::now()))
					{
						throw std::logic_error("accepted record could not enter the batch planner");
					}
				}
				else if (planner.empty())
				{
					_queue.release(*record);
					{
						std::lock_guard lock(_statusMutex);
						++_status.failedRecords;
						++_status.failedRecordsByProducer[record->producerId];
						_status.lastErrorCode = 413;
						_status.lastErrorCodeByProducer[record->producerId] = 413;
					}
					setFailure(413, "record exceeds the configured batch limits");
					continue;
				}
				else
				{
					deferred = std::move(record);
				}
				_inFlightRecords = planner.size() + (deferred ? 1 : 0);
			}
			if (planner.empty())
			{
				continue;
			}
			if (!deferred && !planner.shouldFlush(std::chrono::steady_clock::now()) && _running)
			{
				continue;
			}
			pending = planner.take();
			_inFlightRecords = pending.size() + (deferred ? 1 : 0);
		}
		UploadResult result;
		for (std::size_t attempt = 0; attempt < _configuration.retry.maxAttempts; ++attempt)
		{
			if (shutdownDeadline && std::chrono::steady_clock::now() + _configuration.requestTimeout > *shutdownDeadline)
			{
				const auto undelivered = releaseUndelivered();
				{
					std::lock_guard lock(_statusMutex);
					_status.failedRecords += undelivered;
				}
				setFailure(-1, "shutdown final flush timed out before another HTTP request");
				return;
			}
			result = transport.writeBatch(pending);
			if (result.delivered || !result.retryable || attempt + 1 == _configuration.retry.maxAttempts || !_running)
			{
				break;
			}
			const auto multiplier = std::int64_t { 1 } << attempt;
			const auto delay = std::min(_configuration.retry.maximumBackoff, _configuration.retry.initialBackoff * multiplier);
			const auto retryAt = std::chrono::steady_clock::now() + delay;
			while (_running && std::chrono::steady_clock::now() < retryAt)
			{
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			}
		}
		if (result.delivered)
		{
			for (const auto& record : pending)
			{
				_queue.release(record);
			}
			std::lock_guard lock(_statusMutex);
			_status.connected = true;
			_status.error.clear();
			_status.lastErrorCode = 0;
			for (const auto& record : pending)
			{
				++_status.deliveredRecordsByProducer[record.producerId];
			}
			_status.deliveredRecords += pending.size();
			_status.lastSuccessfulSendTime = TimePoint::clock::now();
			pending.clear();
			_inFlightRecords = deferred ? 1 : 0;
			continue;
		}
		if (!result.recordFailures.empty())
		{
			int transientCode = 0;
			std::string transientError;
			{
				std::lock_guard lock(_statusMutex);
				_status.connected = true;
			}
			std::vector<Record> retry;
			for (auto& record : pending)
			{
				const RecordIdentity identity { record.entry, normalizeTimestamp(record.timestamp) };
				const auto failure = std::ranges::find_if(result.recordFailures,
					[&identity](const RecordFailure& item) { return item.identity == identity; });
				if (failure == result.recordFailures.end())
				{
					_queue.release(record);
					std::lock_guard lock(_statusMutex);
					++_status.deliveredRecords;
					++_status.deliveredRecordsByProducer[record.producerId];
					_status.lastSuccessfulSendTime = TimePoint::clock::now();
					continue;
				}
				if (retryable(failure->code))
				{
					transientCode = failure->code;
					transientError = failure->message;
					retry.push_back(std::move(record));
					continue;
				}
				_queue.release(record);
				std::lock_guard lock(_statusMutex);
				++_status.failedRecords;
				++_status.failedRecordsByProducer[record.producerId];
				_status.lastErrorCode = failure->code;
				_status.lastErrorCodeByProducer[record.producerId] = failure->code;
				_status.error = failure->message;
			}
			pending = std::move(retry);
			_inFlightRecords = pending.size() + (deferred ? 1 : 0);
			if (pending.empty())
			{
				continue;
			}
			{
				std::lock_guard lock(_statusMutex);
				_status.lastErrorCode = transientCode;
				_status.error = std::move(transientError);
				for (const auto& record : pending)
				{
					_status.lastErrorCodeByProducer[record.producerId] = transientCode;
				}
			}
			const auto retryAt = std::chrono::steady_clock::now() + _configuration.retry.maximumBackoff;
			while (_running && std::chrono::steady_clock::now() < retryAt)
			{
				std::this_thread::sleep_for(std::chrono::milliseconds(10));
			}
			continue;
		}
		if (!result.retryable)
		{
			for (const auto& record : pending)
			{
				_queue.release(record);
			}
			{
				std::lock_guard lock(_statusMutex);
				_status.failedRecords += pending.size();
				for (const auto& record : pending)
				{
					++_status.failedRecordsByProducer[record.producerId];
					_status.lastErrorCodeByProducer[record.producerId] = result.errorCode;
				}
			}
			pending.clear();
			_inFlightRecords = deferred ? 1 : 0;
			setFailure(result.errorCode, result.error);
			continue;
		}
		{
			std::lock_guard lock(_statusMutex);
			for (const auto& record : pending)
			{
				_status.lastErrorCodeByProducer[record.producerId] = result.errorCode;
			}
		}
		setFailure(result.errorCode, result.error);
		const auto retryAt = std::chrono::steady_clock::now() + _configuration.retry.maximumBackoff;
		while (_running && std::chrono::steady_clock::now() < retryAt)
		{
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	}
}

}
