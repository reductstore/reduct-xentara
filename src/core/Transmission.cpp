#include "reductstore_xentara/Transmission.hpp"

#include <reduct/bucket.h>
#include <reduct/client.h>

#include <algorithm>
#include <chrono>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace reductstore_xentara
{

namespace
{

auto retryable(const reduct::Error& error) -> bool
{
	return error.code == -1 || error.code == 408 || error.code == 429 || error.code >= 500;
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
				throw std::runtime_error("could not create ReductStore client");
			}
		}
		if (!_bucket)
		{
			auto result = configuration.createBucket ? _client->GetOrCreateBucket(configuration.bucket) : _client->GetBucket(configuration.bucket);
			if (result.error)
			{
				throw std::runtime_error(message(result.error));
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
			return { false, retryable(result.error), message(result.error), {} };
		}
		if (!result.result.empty())
		{
			std::vector<RecordIdentity> failedRecords;
			for (const auto& [entry, errors] : result.result)
			{
				for (const auto& [timestamp, error] : errors)
				{
					failedRecords.push_back({ entry, timestamp });
				}
			}
			return UploadResult::partial(std::move(failedRecords));
		}
		return UploadResult::success();
	}
	catch (const std::exception& error)
	{
		return UploadResult::transportFailure(error.what());
	}
}

auto ReductTransport::writeFile(const Record& metadata, const FilePayload& payload) -> UploadResult
{
	try
	{
		reduct::IBucket::WriteOptions options {};
		options.timestamp = metadata.timestamp;
		options.labels = metadata.labels;
		options.content_type = metadata.contentType;
		const auto error = _implementation->bucket()->Write(metadata.entry, options, [&payload](reduct::IBucket::WritableRecord* record) {
			record->Write(payload.size(), [&payload](std::size_t offset, std::size_t bytes) {
				auto chunk = payload.read(offset, bytes);
				return std::pair { true, std::move(chunk) };
			});
		});
		if (error)
		{
			return { false, retryable(error), message(error), {} };
		}
		return UploadResult::success();
	}
	catch (const std::exception& error)
	{
		return UploadResult::transportFailure(error.what());
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

auto UplinkWorker::setFailure(std::string error) -> void
{
	std::lock_guard lock(_statusMutex);
	_status.connected = false;
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
		return undelivered;
	};
	while (_running || !planner.empty() || _queue.size() != 0 || !pending.empty())
	{
		if (!_running && !shutdownDeadline)
		{
			shutdownDeadline = std::chrono::steady_clock::now() + _configuration.requestTimeout;
		}
		if (shutdownDeadline && std::chrono::steady_clock::now() >= *shutdownDeadline)
		{
			const auto undelivered = releaseUndelivered();
			{
				std::lock_guard lock(_statusMutex);
				_status.failedRecords = undelivered;
			}
			setFailure("shutdown final flush timed out with undelivered records");
			return;
		}
		if (pending.empty())
		{
			if (auto record = _queue.waitPop(std::chrono::milliseconds(50)))
			{
				planner.push(std::move(*record), std::chrono::steady_clock::now());
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
		}
		auto result = retryTransient(_configuration.retry, [&transport, &pending] { return transport.writeBatch(pending); });
		if (result.delivered)
		{
			for (const auto& record : pending)
			{
				_queue.release(record);
			}
			std::lock_guard lock(_statusMutex);
			_status.connected = true;
			_status.error.clear();
			_status.deliveredRecords += pending.size();
			_status.failedRecords = 0;
			_status.lastSuccessfulSendTime = TimePoint::clock::now();
			pending.clear();
			continue;
		}
		if (!result.failedRecords.empty())
		{
			std::vector<Record> failed;
			failed.reserve(result.failedRecords.size());
			for (auto& record : pending)
			{
				const RecordIdentity identity { record.entry, record.timestamp };
				if (std::ranges::find(result.failedRecords, identity) != result.failedRecords.end())
				{
					failed.push_back(std::move(record));
				}
				else
				{
					_queue.release(record);
				}
			}
			pending = std::move(failed);
		}
		{
			std::lock_guard lock(_statusMutex);
			_status.failedRecords = pending.size();
		}
		setFailure(result.error);
		if (!result.retryable && !pending.empty())
		{
			while (_running)
			{
				std::this_thread::sleep_for(std::chrono::milliseconds(50));
			}
			const auto undelivered = releaseUndelivered();
			{
				std::lock_guard lock(_statusMutex);
				_status.failedRecords = undelivered;
			}
			return;
		}
		std::this_thread::sleep_for(_configuration.retry.maximumBackoff);
	}
}

}
