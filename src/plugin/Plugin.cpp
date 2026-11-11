// Copyright (c) embedded ocean GmbH
#include "Plugin.hpp"

#include "reductstore_xentara/Serialization.hpp"

#include <xentara/config/Context.hpp>
#include <xentara/config/Errors.hpp>
#include <xentara/data/DataType.hpp>
#include <xentara/data/NativeType.hpp>
#include <xentara/data/Quality.hpp>
#include <xentara/memory/WriteSentinel.hpp>
#include <xentara/memory/memoryResources.hpp>
#include <xentara/model/Attribute.hpp>
#include <xentara/model/Element.hpp>
#include <xentara/model/ForEachAttributeFunction.hpp>
#include <xentara/model/ForEachEventFunction.hpp>
#include <xentara/model/ForEachTaskFunction.hpp>
#include <xentara/process/ExecutionContext.hpp>
#include <xentara/skill/ElementFactory.hpp>
#include <xentara/utils/json/decoder/Errors.hpp>
#include <xentara/utils/json/decoder/Object.hpp>

#include <algorithm>
#include <stdexcept>
#include <string_view>

#include <unistd.h>

namespace xentara::plugins::reductstore
{

namespace
{

using namespace std::literals;
using namespace xentara::literals;

std::atomic_uint64_t nextProducerId { 1 };

const model::Attribute kHealthy { "98915e48-f6d9-4954-9d33-e91ffca468f9"_uuid, "healthy"sv, model::Attribute::Access::ReadOnly, data::DataType::kBoolean };
const model::Attribute kQueuedRecords { "fe3d61a2-8720-4ce7-948a-bf22ad5dbb1f"_uuid, "queuedRecords"sv, model::Attribute::Access::ReadOnly, data::DataType::kInteger };
const model::Attribute kInFlightRecords { "d6c7c05a-9a7e-48b2-9aa8-faf9a339dbb1"_uuid, "inFlightRecords"sv, model::Attribute::Access::ReadOnly, data::DataType::kInteger };
const model::Attribute kQueuedBytes { "b6350cac-b1ae-44a4-b5ef-5b4d2fb5c2d5"_uuid, "queuedBytes"sv, model::Attribute::Access::ReadOnly, data::DataType::kInteger };
const model::Attribute kRejectedRecords { "a065417d-5e59-4db7-9707-7d03e1f9af17"_uuid, "rejectedRecords"sv, model::Attribute::Access::ReadOnly, data::DataType::kInteger };
const model::Attribute kDroppedRecords { "a55ee685-69e2-4e9d-b889-64c64eeb6024"_uuid, "droppedRecords"sv, model::Attribute::Access::ReadOnly, data::DataType::kInteger };
const model::Attribute kFailedRecords { "55c99df4-78e5-4c6a-9d93-870d71a67b5a"_uuid, "failedRecords"sv, model::Attribute::Access::ReadOnly, data::DataType::kInteger };
const model::Attribute kCollectionFailures { "461a7d30-ec81-41d9-b630-6187eeef6d3d"_uuid, "collectionFailures"sv, model::Attribute::Access::ReadOnly, data::DataType::kInteger };
const model::Attribute kDuplicateSnapshots { "bbdb44e0-f343-4fd1-b1f9-9e5ab4c62209"_uuid, "duplicateSnapshots"sv, model::Attribute::Access::ReadOnly, data::DataType::kInteger };
const model::Attribute kLastErrorCode { "3471b8be-11a7-4c0a-9e7e-cbbde003ab19"_uuid, "lastErrorCode"sv, model::Attribute::Access::ReadOnly, data::DataType::kInteger };
const model::Attribute kLastSuccessfulSendTime { "b33cac6d-19b0-4fc5-b8b9-1c8abcf57cf0"_uuid, "lastSuccessfulSendTime"sv, model::Attribute::Access::ReadOnly, data::DataType::kTimeStamp };
const model::Attribute kError { model::Attribute::kError, model::Attribute::Access::ReadOnly, data::DataType::kErrorCode };
const process::Event::Role kSent { "c0d111f6-2f1b-497a-bcaf-837c6f56add6"_uuid, "sent"sv };
const process::Event::Role kSendError { "d0bee531-3595-466f-96bd-6f7dcc0ad09f"_uuid, "sendError"sv };
const process::Task::Role kCollect { "6c1c2987-3b97-450e-8614-ca00a9a15587"_uuid, "collect"sv };
const process::Task::Role kSend { "b590c276-adf7-4d28-875f-0946b0c93a26"_uuid, "send"sv };

auto parseOverflowPolicy(utils::json::decoder::Value& value) -> reductstore_xentara::OverflowPolicy
{
	const auto policy = value.asString<std::string>();
	if (policy == "rejectNewest")
	{
		return reductstore_xentara::OverflowPolicy::RejectNewest;
	}
	if (policy == "dropOldest")
	{
		return reductstore_xentara::OverflowPolicy::DropOldest;
	}
	throw std::runtime_error("overflowPolicy must be rejectNewest or dropOldest");
}

auto hostName() -> std::string
{
	char buffer[256] {};
	if (gethostname(buffer, sizeof(buffer) - 1) != 0)
	{
		throw std::runtime_error("could not determine host name");
	}
	return buffer;
}

auto isRelativeEntry(std::string_view entry) -> bool
{
	return !entry.empty() && entry.front() != '/' && entry.back() != '/' && entry.find("//") == std::string_view::npos;
}

}

Skill::Class Skill::_class;

auto Skill::createElement(const skill::Element::Class& elementClass, skill::ElementFactory& factory) -> std::shared_ptr<skill::Element>
{
	if (&elementClass == &ReductClient::Class::instance())
	{
		return factory.makeShared<ReductClient>();
	}
	return nullptr;
}

auto ReductClient::createChildElement(const skill::Element::Class& elementClass, skill::ElementFactory& factory) -> std::shared_ptr<skill::Element>
{
	if (&elementClass == &ReductWriter::Class::instance())
	{
		return factory.makeShared<ReductWriter>(*this);
	}
	return nullptr;
}

auto ReductClient::category() const noexcept -> model::ElementCategory
{
	return model::ElementCategory::DataStorage;
}

auto ReductClient::worker() -> reductstore_xentara::UplinkWorker&
{
	return *_worker;
}

auto ReductClient::host() const -> const std::string&
{
	return _host;
}

auto ReductClient::entryPrefix() const -> const std::string&
{
	return _entryPrefix;
}

auto ReductClient::load(utils::json::decoder::Object& object, config::Context&) -> void
{
	for (auto&& [name, value] : object)
	{
		if (name == "serverUrl"sv) _configuration.serverUrl = value.asString<std::string>();
		else if (name == "apiToken"sv) _configuration.apiToken = value.asString<std::string>();
		else if (name == "bucket"sv) _configuration.bucket = value.asString<std::string>();
		else if (name == "entryPrefix"sv) _entryPrefix = value.asString<std::string>();
		else if (name == "host"sv) _host = value.asString<std::string>();
		else if (name == "connectionTimeout"sv) _configuration.connectionTimeout = value.toTimeDuration<std::chrono::milliseconds>();
		else if (name == "requestTimeout"sv) _configuration.requestTimeout = value.toTimeDuration<std::chrono::milliseconds>();
		else if (name == "shutdownTimeout"sv) _configuration.shutdownTimeout = value.toTimeDuration<std::chrono::milliseconds>();
		else if (name == "createBucket"sv) _configuration.createBucket = value.asBoolean();
		else if (name == "maxBatchRecords"sv) _configuration.maxBatchRecords = value.asNumber<std::size_t>();
		else if (name == "maxBatchPayloadBytes"sv) _configuration.maxBatchPayloadBytes = value.asNumber<std::size_t>();
		else if (name == "maxBatchMetadataBytes"sv) _configuration.maxBatchMetadataBytes = value.asNumber<std::size_t>();
		else if (name == "maxBatchAge"sv) _configuration.maxBatchAge = value.toTimeDuration<std::chrono::milliseconds>();
		else if (name == "maxQueueBytes"sv) _configuration.maxQueueBytes = value.asNumber<std::size_t>();
		else if (name == "overflowPolicy"sv) _configuration.overflowPolicy = parseOverflowPolicy(value);
		else if (name == "retry"sv)
		{
			for (auto&& [retryName, retryValue] : value.asObject())
			{
				if (retryName == "maxAttempts"sv) _configuration.retry.maxAttempts = retryValue.asNumber<std::size_t>();
				else if (retryName == "initialBackoff"sv) _configuration.retry.initialBackoff = retryValue.toTimeDuration<std::chrono::milliseconds>();
				else if (retryName == "maximumBackoff"sv) _configuration.retry.maximumBackoff = retryValue.toTimeDuration<std::chrono::milliseconds>();
				else config::throwUnknownParameterError(retryName);
			}
		}
		else config::throwUnknownParameterError(name);
	}
	if (const auto error = reductstore_xentara::validate(_configuration); !error.empty())
	{
		utils::json::decoder::throwWithLocation(object, std::runtime_error(error));
	}
}

auto ReductClient::prepare() -> void
{
	if (!_entryPrefix.empty() && !isRelativeEntry(_entryPrefix))
	{
		throw std::runtime_error("entryPrefix must be a relative entry path");
	}
	if (_host.empty() || _host == "auto")
	{
		_host = hostName();
	}
	_worker = std::make_unique<reductstore_xentara::UplinkWorker>(_configuration);
	_worker->start();
}

auto ReductClient::cleanup() -> void
{
	if (_worker)
	{
		_worker->stop();
	}
}

ReductWriter::ReductWriter(std::reference_wrapper<ReductClient> client) :
	_client(client),
	_producerId(nextProducerId.fetch_add(1)),
	_collectTask(*this),
	_sendTask(*this)
{
}

auto ReductWriter::forEachAttribute(const model::ForEachAttributeFunction& function) const -> bool
{
	return function(kHealthy) || function(kQueuedRecords) || function(kInFlightRecords) || function(kQueuedBytes) || function(kRejectedRecords) ||
		function(kDroppedRecords) || function(kFailedRecords) || function(kCollectionFailures) ||
		function(kDuplicateSnapshots) || function(kLastErrorCode) || function(kLastSuccessfulSendTime) || function(kError);
}

auto ReductWriter::forEachEvent(const model::ForEachEventFunction& function) -> bool
{
	return function(kSent, sharedFromThis(&_sent)) || function(kSendError, sharedFromThis(&_error));
}

auto ReductWriter::forEachTask(const model::ForEachTaskFunction& function) -> bool
{
	return function(kCollect, sharedFromThis(&_collectTask)) || function(kSend, sharedFromThis(&_sendTask));
}

auto ReductWriter::makeReadHandle(const model::Attribute& attribute) const noexcept -> std::optional<data::ReadHandle>
{
	if (attribute == kHealthy) return _state.member(&State::healthy);
	if (attribute == kQueuedRecords) return _state.member(&State::queuedRecords);
	if (attribute == kInFlightRecords) return _state.member(&State::inFlightRecords);
	if (attribute == kQueuedBytes) return _state.member(&State::queuedBytes);
	if (attribute == kRejectedRecords) return _state.member(&State::rejectedRecords);
	if (attribute == kDroppedRecords) return _state.member(&State::droppedRecords);
	if (attribute == kFailedRecords) return _state.member(&State::failedRecords);
	if (attribute == kCollectionFailures) return _state.member(&State::collectionFailures);
	if (attribute == kDuplicateSnapshots) return _state.member(&State::duplicateSnapshots);
	if (attribute == kLastErrorCode) return _state.member(&State::lastErrorCode);
	if (attribute == kLastSuccessfulSendTime) return _state.member(&State::lastSuccessfulSendTime);
	if (attribute == kError) return _state.member(&State::error);
	return std::nullopt;
}

auto ReductWriter::category() const noexcept -> model::ElementCategory
{
	return model::ElementCategory::Transaction;
}

auto ReductWriter::Measurement::resolve() -> void
{
	auto point = dataPoint.lock();
	if (!point)
	{
		throw std::runtime_error("writer dataPoint could not be resolved");
	}
	value = point->attributeReadHandle(model::Attribute::kValue);
	quality = point->attributeReadHandle(model::Attribute::kQuality);
	sourceTimestamp = point->attributeReadHandle(model::Attribute::kSourceTime);
	if (value.hardError() || quality.hardError())
	{
		throw std::runtime_error("writer could not create required data point read handles");
	}
	switch (value.nativeType())
	{
	case data::NativeType::Boolean:
	case data::NativeType::Integer:
	case data::NativeType::UnsignedInteger:
	case data::NativeType::FloatingPoint:
		break;
	default:
		throw std::runtime_error("writer dataPoint value must be Boolean, Integer, UnsignedInteger, or FloatingPoint");
	}
}

auto ReductWriter::Measurement::valuePayload() const -> std::optional<std::string>
{
	switch (value.nativeType())
	{
	case data::NativeType::Boolean: { auto item = value.read<bool>(); if (item) return reductstore_xentara::serializeScalar(*item); break; }
	case data::NativeType::Integer: { auto item = value.read<std::intmax_t>(); if (item) return reductstore_xentara::serializeScalar(*item); break; }
	case data::NativeType::UnsignedInteger: { auto item = value.read<std::uintmax_t>(); if (item) return reductstore_xentara::serializeScalar(*item); break; }
	case data::NativeType::FloatingPoint: { auto item = value.read<double>(); if (item) return reductstore_xentara::trySerializeScalar(*item); break; }
	default: break;
	}
	return std::nullopt;
}

auto ReductWriter::Snapshot::resolve() -> void
{
	for (auto& measurement : measurements) measurement.resolve();
}

auto ReductWriter::Snapshot::collect(std::chrono::system_clock::time_point scheduledTime, const std::string& host, const std::string& entryPrefix) -> CollectionResult
{
	const auto recordTimestamp = reductstore_xentara::normalizeTimestamp(scheduledTime);
	if (lastTimestamp == recordTimestamp)
	{
		return { CollectionStatus::Duplicate, std::nullopt };
	}
	std::vector<std::pair<std::string, std::string>> values;
	std::vector<std::pair<std::string, std::string>> sourceTimes;
	std::vector<std::pair<std::string, std::string>> qualities;
	values.reserve(measurements.size());
	sourceTimes.reserve(measurements.size());
	qualities.reserve(measurements.size());
	std::optional<data::Quality> aggregateQuality;
	for (const auto& measurement : measurements)
	{
		auto value = measurement.valuePayload();
		auto quality = measurement.quality.read<data::Quality>();
		if (!value || !quality)
		{
			return { CollectionStatus::Incomplete, std::nullopt };
		}
		values.emplace_back(measurement.name, std::move(*value));
		if (!measurement.sourceTimestamp.hardError())
		{
			if (auto sourceTime = measurement.sourceTimestamp.read<std::chrono::system_clock::time_point>())
			{
				sourceTimes.emplace_back(measurement.name,
					reductstore_xentara::serializeScalar(static_cast<std::intmax_t>(reductstore_xentara::unixMicroseconds(*sourceTime))));
			}
		}
		qualities.emplace_back(measurement.name, reductstore_xentara::serializeString(data::toString(*quality)));
		aggregateQuality = aggregateQuality ? data::combine(*aggregateQuality, *quality) : *quality;
	}
	std::vector<std::pair<std::string, std::string>> payload;
	payload.reserve(3);
	payload.emplace_back("measurements", reductstore_xentara::serializeObject(values));
	if (!sourceTimes.empty())
	{
		payload.emplace_back("sourceTime", reductstore_xentara::serializeObject(sourceTimes));
	}
	payload.emplace_back("quality", reductstore_xentara::serializeObject(qualities));
	auto recordLabels = labels;
	recordLabels["host"] = host;
	recordLabels["quality"] = data::toString(*aggregateQuality);
	return {
		CollectionStatus::Collected,
		reductstore_xentara::Record {
			reductstore_xentara::entryName(entryPrefix, entry),
			recordTimestamp,
			"application/json",
			reductstore_xentara::serializeObject(payload),
			std::move(recordLabels),
		},
	};
}

auto ReductWriter::load(utils::json::decoder::Object& object, config::Context& context) -> void
{
	for (auto&& [name, value] : object)
	{
		if (name == "recordGroups"sv)
		{
			for (auto&& item : value.asArray())
			{
				Snapshot snapshot;
				bool modeLoaded = false;
				bool entryLoaded = false;
				bool timestampLoaded = false;
				bool measurementsLoaded = false;
				for (auto&& [groupName, groupValue] : item.asObject())
				{
					if (groupName == "mode"sv)
					{
						modeLoaded = true;
						if (groupValue.asString<std::string>() != "snapshot") throw std::runtime_error("record group mode must be snapshot");
					}
					else if (groupName == "entry"sv)
					{
						snapshot.entry = groupValue.asString<std::string>();
						entryLoaded = isRelativeEntry(snapshot.entry);
					}
					else if (groupName == "timestampSource"sv)
					{
						timestampLoaded = true;
						if (groupValue.asString<std::string>() != "scheduled") throw std::runtime_error("snapshot record groups require timestampSource scheduled");
					}
					else if (groupName == "labels"sv)
					{
						for (auto&& [label, labelValue] : groupValue.asObject())
						{
							if (label == "host"sv || label == "quality"sv) throw std::runtime_error("snapshot labels must not override host or quality");
							snapshot.labels.emplace(std::string(label), labelValue.asString<std::string>());
						}
					}
					else if (groupName == "measurements"sv)
					{
						measurementsLoaded = true;
						for (auto&& [measurementName, dataPoint] : groupValue.asObject())
						{
							if (measurementName.empty()) throw std::runtime_error("snapshot measurement names must be non-empty");
							Measurement measurement;
							measurement.name = std::string(measurementName);
							context.resolve<model::Element>(dataPoint, std::ref(measurement.dataPoint));
							snapshot.measurements.push_back(std::move(measurement));
						}
					}
					else config::throwUnknownParameterError(groupName);
				}
				if (!modeLoaded || !entryLoaded || !timestampLoaded || !measurementsLoaded || snapshot.measurements.empty())
				{
					throw std::runtime_error("each snapshot record group requires mode, relative entry, scheduled timestampSource, and measurements");
				}
				if (std::ranges::any_of(_snapshots, [&snapshot](const Snapshot& item) { return item.entry == snapshot.entry; }))
				{
					throw std::runtime_error("snapshot record groups in one writer must use distinct entries");
				}
				_snapshots.push_back(std::move(snapshot));
			}
		}
		else
		{
			config::throwUnknownParameterError(name);
		}
	}
	if (_snapshots.empty()) throw std::runtime_error("writer requires at least one record group");
}

auto ReductWriter::realize() -> void
{
	_state.create(memory::memoryResources::data());
}

auto ReductWriter::prepare() -> void
{
	for (auto& snapshot : _snapshots) snapshot.resolve();
}

auto ReductWriter::collect(const process::ExecutionContext& context) -> void
{
	{
		std::lock_guard lock(_mutex);
		for (auto& snapshot : _snapshots)
		{
			try
			{
				auto result = snapshot.collect(context.scheduledTime(), _client.get().host(), _client.get().entryPrefix());
				if (result.status == Snapshot::CollectionStatus::Incomplete)
				{
					++_collectionFailures;
					continue;
				}
				if (result.status == Snapshot::CollectionStatus::Duplicate)
				{
					++_duplicateSnapshots;
					continue;
				}
				result.record->producerId = _producerId;
				const auto recordTimestamp = result.record->timestamp;
				const auto enqueue = _client.get().worker().enqueue(std::move(*result.record));
				if (!enqueue.accepted)
				{
					++_collectionFailures;
					continue;
				}
				snapshot.lastTimestamp = recordTimestamp;
			}
			catch (const std::exception&)
			{
				++_collectionFailures;
			}
		}
	}
	updateState(context.scheduledTime());
}


auto ReductWriter::updateState(std::chrono::system_clock::time_point timestamp) -> void
{
	std::lock_guard lock(_mutex);
	const auto status = _client.get().worker().status();
	const auto delivered = status.deliveredRecordsByProducer.find(_producerId);
	const auto deliveredRecords = delivered == status.deliveredRecordsByProducer.end() ? 0 : delivered->second;
	const auto failed = status.failedRecordsByProducer.find(_producerId);
	const auto failedRecords = failed == status.failedRecordsByProducer.end() ? 0 : failed->second;
	const auto rejected = status.rejectedRecordsByProducer.find(_producerId);
	const auto rejectedRecords = rejected == status.rejectedRecordsByProducer.end() ? 0 : rejected->second;
	const auto dropped = status.droppedRecordsByProducer.find(_producerId);
	const auto droppedRecords = dropped == status.droppedRecordsByProducer.end() ? 0 : dropped->second;
	const auto lastError = status.lastErrorCodeByProducer.find(_producerId);
	const auto lastErrorCode = lastError == status.lastErrorCodeByProducer.end() ? 0 : lastError->second;
	const auto collectionFailed = _collectionFailures != _observedCollectionFailures;
	memory::WriteSentinel sentinel { _state };
	auto& state = *sentinel;
	state.healthy = status.connected && status.error.empty() && !collectionFailed;
	state.queuedRecords = status.queuedRecords;
	state.inFlightRecords = status.inFlightRecords;
	state.queuedBytes = status.queuedBytes;
	state.rejectedRecords = rejectedRecords;
	state.droppedRecords = droppedRecords;
	state.failedRecords = failedRecords;
	state.collectionFailures = _collectionFailures;
	state.duplicateSnapshots = _duplicateSnapshots;
	state.lastErrorCode = lastErrorCode;
	state.error = status.error.empty() && !collectionFailed ? std::error_code {} : std::make_error_code(std::errc::io_error);
	state.lastSuccessfulSendTime = status.lastSuccessfulSendTime;
	if (collectionFailed)
	{
		_observedCollectionFailures = _collectionFailures;
		sentinel.commit(timestamp, _error);
	}
	else if (deliveredRecords > _observedDelivered)
	{
		_observedDelivered = deliveredRecords;
		sentinel.commit(timestamp, _sent);
	}
	else if (!status.error.empty())
	{
		sentinel.commit(timestamp, _error);
	}
	else
	{
		sentinel.commit(timestamp);
	}
}

ReductWriter::CollectTask::CollectTask(ReductWriter& target) : _target(target) {}
auto ReductWriter::CollectTask::operational(const process::ExecutionContext& context) -> void { _target.get().collect(context); }
ReductWriter::SendTask::SendTask(ReductWriter& target) : _target(target) {}
auto ReductWriter::SendTask::stages() const -> Stages { return Stage::Operational | Stage::PostOperational; }
auto ReductWriter::SendTask::operational(const process::ExecutionContext& context) -> void { _target.get().updateState(context.scheduledTime()); }
auto ReductWriter::SendTask::preparePostOperational(const process::ExecutionContext& context) -> Status { _target.get().updateState(context.scheduledTime()); return Status::Completed; }

}
