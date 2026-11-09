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
			std::vector<std::string> failedEntries;
			for (const auto& [entry, errors] : result.result)
			{
				if (!errors.empty())
				{
					failedEntries.push_back(entry);
				}
			}
			return UploadResult::partial(std::move(failedEntries));
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

auto UplinkWorker::requestFlush() -> void
{
	_flushRequested = true;
}

auto UplinkWorker::status() const -> DeliveryStatus
{
	std::lock_guard lock(_statusMutex);
	auto status = _status;
	status.queuedRecords = _queue.size();
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
	BatchPlanner planner({ _configuration.maxBatchRecords, _configuration.maxBatchPayloadBytes, _configuration.maxBatchAge });
	ReductTransport transport(_configuration);
	while (_running || !planner.empty() || _queue.size() != 0)
	{
		if (auto record = _queue.waitPop(std::chrono::milliseconds(50)))
		{
			planner.push(std::move(*record), std::chrono::steady_clock::now());
		}
		if (planner.empty())
		{
			continue;
		}
		if (!_flushRequested.exchange(false) && !planner.shouldFlush(std::chrono::steady_clock::now()) && _running)
		{
			continue;
		}
		auto batch = planner.take();
		auto result = retryTransient(_configuration.retry, [&transport, &batch] { return transport.writeBatch(batch); });
		if (result.delivered)
		{
			std::lock_guard lock(_statusMutex);
			_status.connected = true;
			_status.error.clear();
			_status.deliveredRecords += batch.size();
			continue;
		}
		{
			std::lock_guard lock(_statusMutex);
			_status.failedRecords += batch.size();
		}
		setFailure(result.error);
		for (auto& record : batch)
		{
			if (!_queue.push(std::move(record)))
			{
				setFailure("failed records could not be retained because the queue is full");
			}
		}
		std::this_thread::sleep_for(_configuration.retry.maximumBackoff);
	}
}

}
