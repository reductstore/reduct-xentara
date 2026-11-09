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

#include <format>
#include <stdexcept>
#include <string_view>

namespace xentara::plugins::reductstore
{

namespace
{

using namespace std::literals;
using namespace xentara::literals;

const model::Attribute kHealthy { "98915e48-f6d9-4954-9d33-e91ffca468f9"_uuid, "healthy"sv, model::Attribute::Access::ReadOnly, data::DataType::kBoolean };
const model::Attribute kQueuedRecords { "fe3d61a2-8720-4ce7-948a-bf22ad5dbb1f"_uuid, "queuedRecords"sv, model::Attribute::Access::ReadOnly, data::DataType::kInteger };
const model::Attribute kQueuedBytes { "b6350cac-b1ae-44a4-b5ef-5b4d2fb5c2d5"_uuid, "queuedBytes"sv, model::Attribute::Access::ReadOnly, data::DataType::kInteger };
const model::Attribute kRejectedRecords { "a065417d-5e59-4db7-9707-7d03e1f9af17"_uuid, "rejectedRecords"sv, model::Attribute::Access::ReadOnly, data::DataType::kInteger };
const model::Attribute kDroppedRecords { "a55ee685-69e2-4e9d-b889-64c64eeb6024"_uuid, "droppedRecords"sv, model::Attribute::Access::ReadOnly, data::DataType::kInteger };
const model::Attribute kFailedRecords { "55c99df4-78e5-4c6a-9d93-870d71a67b5a"_uuid, "failedRecords"sv, model::Attribute::Access::ReadOnly, data::DataType::kInteger };
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

auto ReductClient::load(utils::json::decoder::Object& object, config::Context&) -> void
{
	for (auto&& [name, value] : object)
	{
		if (name == "serverUrl"sv) _configuration.serverUrl = value.asString<std::string>();
		else if (name == "apiToken"sv) _configuration.apiToken = value.asString<std::string>();
		else if (name == "bucket"sv) _configuration.bucket = value.asString<std::string>();
		else if (name == "host"sv) _host = value.asString<std::string>();
		else if (name == "connectionTimeout"sv) _configuration.connectionTimeout = value.toTimeDuration<std::chrono::milliseconds>();
		else if (name == "requestTimeout"sv) _configuration.requestTimeout = value.toTimeDuration<std::chrono::milliseconds>();
		else if (name == "createBucket"sv) _configuration.createBucket = value.asBoolean();
		else if (name == "maxBatchRecords"sv) _configuration.maxBatchRecords = value.asNumber<std::size_t>();
		else if (name == "maxBatchPayloadBytes"sv) _configuration.maxBatchPayloadBytes = value.asNumber<std::size_t>();
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
	_collectTask(*this),
	_sendTask(*this)
{
}

auto ReductWriter::forEachAttribute(const model::ForEachAttributeFunction& function) const -> bool
{
	return function(kHealthy) || function(kQueuedRecords) || function(kQueuedBytes) || function(kRejectedRecords) ||
		function(kDroppedRecords) || function(kFailedRecords) || function(kLastSuccessfulSendTime) || function(kError);
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
	if (attribute == kQueuedBytes) return _state.member(&State::queuedBytes);
	if (attribute == kRejectedRecords) return _state.member(&State::rejectedRecords);
	if (attribute == kDroppedRecords) return _state.member(&State::droppedRecords);
	if (attribute == kFailedRecords) return _state.member(&State::failedRecords);
	if (attribute == kLastSuccessfulSendTime) return _state.member(&State::lastSuccessfulSendTime);
	return std::nullopt;
}

auto ReductWriter::category() const noexcept -> model::ElementCategory
{
	return model::ElementCategory::Transaction;
}

auto ReductWriter::Mapping::resolve() -> void
{
	auto point = dataPoint.lock();
	if (!point)
	{
		throw std::runtime_error("writer dataPoint could not be resolved");
	}
	value = point->attributeReadHandle(model::Attribute::kValue);
	quality = point->attributeReadHandle(model::Attribute::kQuality);
	if (timestampSource == TimestampSource::Source) timestamp = point->attributeReadHandle(model::Attribute::kSourceTime);
	if (timestampSource == TimestampSource::Update) timestamp = point->attributeReadHandle(model::Attribute::kUpdateTime);
	if (value.hardError() || quality.hardError() || (timestampSource != TimestampSource::Scheduled && timestamp.hardError()))
	{
		throw std::runtime_error("writer could not create required data point read handles");
	}
	source = std::string(point->primaryKey());
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

auto ReductWriter::Mapping::collect(std::chrono::system_clock::time_point scheduledTime, const std::string& host) -> std::optional<reductstore_xentara::Record>
{
	auto recordTimestamp = scheduledTime;
	if (timestampSource != TimestampSource::Scheduled)
	{
		auto valueTimestamp = timestamp.read<std::chrono::system_clock::time_point>();
		if (!valueTimestamp) return std::nullopt;
		recordTimestamp = *valueTimestamp;
		if (lastTimestamp == recordTimestamp) return std::nullopt;
	}
	std::string payload;
	switch (value.nativeType())
	{
	case data::NativeType::Boolean: { auto item = value.read<bool>(); if (!item) return std::nullopt; payload = reductstore_xentara::serializeScalar(*item); break; }
	case data::NativeType::Integer: { auto item = value.read<std::intmax_t>(); if (!item) return std::nullopt; payload = reductstore_xentara::serializeScalar(*item); break; }
	case data::NativeType::UnsignedInteger: { auto item = value.read<std::uintmax_t>(); if (!item) return std::nullopt; payload = reductstore_xentara::serializeScalar(*item); break; }
	case data::NativeType::FloatingPoint: { auto item = value.read<double>(); if (!item) return std::nullopt; payload = reductstore_xentara::serializeScalar(*item); break; }
	default: return std::nullopt;
	}
	auto qualityValue = quality.read<data::Quality>();
	if (!qualityValue) return std::nullopt;
	auto recordLabels = labels;
	recordLabels["quality"] = data::toString(*qualityValue);
	recordLabels["source"] = source;
	if (!host.empty()) recordLabels["host"] = host;
	lastTimestamp = recordTimestamp;
	return reductstore_xentara::Record { entry, recordTimestamp, "application/json", std::move(payload), std::move(recordLabels) };
}

auto ReductWriter::load(utils::json::decoder::Object& object, config::Context& context) -> void
{
	for (auto&& [name, value] : object)
	{
		if (name != "records"sv)
		{
			config::throwUnknownParameterError(name);
		}
		for (auto&& item : value.asArray())
		{
			Mapping mapping;
			bool dataPointLoaded = false;
			bool entryLoaded = false;
			for (auto&& [recordName, recordValue] : item.asObject())
			{
				if (recordName == "dataPoint"sv) { context.resolve<model::Element>(recordValue, std::ref(mapping.dataPoint)); dataPointLoaded = true; }
				else if (recordName == "entry"sv) { mapping.entry = recordValue.asString<std::string>(); entryLoaded = !mapping.entry.empty(); }
				else if (recordName == "timestampSource"sv)
				{
					const auto source = recordValue.asString<std::string>();
					if (source == "scheduled") mapping.timestampSource = Mapping::TimestampSource::Scheduled;
					else if (source == "sourceTime") mapping.timestampSource = Mapping::TimestampSource::Source;
					else if (source == "updateTime") mapping.timestampSource = Mapping::TimestampSource::Update;
					else throw std::runtime_error("timestampSource must be scheduled, sourceTime, or updateTime");
				}
				else if (recordName == "labels"sv)
				{
					for (auto&& [label, labelValue] : recordValue.asObject()) mapping.labels.emplace(std::string(label), labelValue.asString<std::string>());
				}
				else config::throwUnknownParameterError(recordName);
			}
			if (!dataPointLoaded || !entryLoaded) throw std::runtime_error("each writer record requires dataPoint and non-empty entry");
			_mappings.push_back(std::move(mapping));
		}
	}
	if (_mappings.empty()) throw std::runtime_error("writer requires at least one record");
}

auto ReductWriter::realize() -> void
{
	_state.create(memory::memoryResources::data());
}

auto ReductWriter::prepare() -> void
{
	for (auto& mapping : _mappings) mapping.resolve();
}

auto ReductWriter::collect(const process::ExecutionContext& context) -> void
{
	for (auto& mapping : _mappings)
	{
		if (auto record = mapping.collect(context.scheduledTime(), _client.get().host()))
		{
			(void)_client.get().worker().enqueue(std::move(*record));
		}
	}
	updateState(context.scheduledTime());
}

auto ReductWriter::flush(const process::ExecutionContext& context) -> void
{
	_client.get().worker().requestFlush();
	updateState(context.scheduledTime());
}

auto ReductWriter::updateState(std::chrono::system_clock::time_point timestamp) -> void
{
	const auto status = _client.get().worker().status();
	memory::WriteSentinel sentinel { _state };
	auto& state = *sentinel;
	state.healthy = status.connected && status.error.empty();
	state.queuedRecords = status.queuedRecords;
	state.queuedBytes = status.queuedBytes;
	state.rejectedRecords = _client.get().worker().rejectedRecords();
	state.droppedRecords = _client.get().worker().droppedRecords();
	state.failedRecords = status.failedRecords;
	state.error = status.error.empty() ? std::error_code {} : std::make_error_code(std::errc::io_error);
	state.lastSuccessfulSendTime = status.lastSuccessfulSendTime;
	if (status.deliveredRecords > _observedDelivered)
	{
		_observedDelivered = status.deliveredRecords;
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
auto ReductWriter::SendTask::operational(const process::ExecutionContext& context) -> void { _target.get().flush(context); }
auto ReductWriter::SendTask::preparePostOperational(const process::ExecutionContext& context) -> Status { _target.get().flush(context); return Status::Completed; }

}
