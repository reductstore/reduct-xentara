#include "reductstore_xentara/Transmission.hpp"

#include <reduct/bucket.h>
#include <reduct/client.h>

#include <algorithm>
#include <chrono>
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

auto UplinkWorker::enqueue(Record record) -> bool
{
	return _queue.push(std::move(record));
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
	std::optional<std::chrono::steady_clock::time_point> shutdownDeadline;
	auto releaseUndelivered = [this, &planner, &pending] {
		auto undelivered = pending.size();
		for (const auto& record : pending)
		{
			_queue.release(record);
		}
		pending.clear();
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
	while (_running || !planner.empty() || _queue.size() != 0 || !pending.empty())
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
			if (auto record = _queue.waitPop(std::chrono::milliseconds(50)))
			{
				planner.push(std::move(*record), std::chrono::steady_clock::now());
				_inFlightRecords = planner.size();
			}
			if (planner.empty())
			{
				continue;
			}
			if (!planner.shouldFlush(std::chrono::steady_clock::now()) && _running)
			{
				continue;
			}
			pending = planner.take();
			_inFlightRecords = pending.size();
		}
		UploadResult result;
		for (std::size_t attempt = 0; attempt < _configuration.retry.maxAttempts; ++attempt)
		{
			if (shutdownDeadline && std::chrono::steady_clock::now() + _configuration.requestTimeout > *shutdownDeadline)
			{
				result = UploadResult::transportFailure(-1, "shutdown final flush timed out before another HTTP request");
				break;
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
			_inFlightRecords = 0;
			continue;
		}
		if (!result.recordFailures.empty())
		{
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
					retry.push_back(std::move(record));
					continue;
				}
				_queue.release(record);
				std::lock_guard lock(_statusMutex);
				++_status.failedRecords;
				_status.lastErrorCode = failure->code;
				_status.error = failure->message;
			}
			pending = std::move(retry);
			_inFlightRecords = pending.size();
			if (pending.empty())
			{
				continue;
			}
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
			}
			pending.clear();
			_inFlightRecords = 0;
			setFailure(result.errorCode, result.error);
			continue;
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
