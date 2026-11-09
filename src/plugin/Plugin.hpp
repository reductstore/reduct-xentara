// Copyright (c) embedded ocean GmbH
#pragma once

#include "reductstore_xentara/Configuration.hpp"
#include "reductstore_xentara/Transmission.hpp"

#include <xentara/data/ReadHandle.hpp>
#include <xentara/memory/Array.hpp>
#include <xentara/model/ElementCategory.hpp>
#include <xentara/process/Event.hpp>
#include <xentara/process/Task.hpp>
#include <xentara/skill/Element.hpp>
#include <xentara/skill/EnableSharedFromThis.hpp>
#include <xentara/skill/Skill.hpp>
#include <xentara/utils/core/Uuid.hpp>

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace xentara::plugins::reductstore
{

class ReductClient;
class ReductWriter;


class ReductClient final : public skill::Element, public skill::EnableSharedFromThis<ReductClient>
{
public:
	using Class = ConcreteClass<"Client", "2f11c968-2f66-4a3a-9c31-0e2d4f3e1fb5"_uuid, "ReductStore client">;

	auto createChildElement(const skill::Element::Class& elementClass, skill::ElementFactory& factory) -> std::shared_ptr<skill::Element> final;
	auto category() const noexcept -> model::ElementCategory final;
	[[nodiscard]] auto worker() -> reductstore_xentara::UplinkWorker&;
	[[nodiscard]] auto host() const -> const std::string&;

private:
	auto load(utils::json::decoder::Object& object, config::Context& context) -> void final;
	auto prepare() -> void final;
	auto cleanup() -> void final;

	reductstore_xentara::ClientConfiguration _configuration;
	std::string _host;
	std::unique_ptr<reductstore_xentara::UplinkWorker> _worker;
};

class ReductWriter final : public skill::Element, public skill::EnableSharedFromThis<ReductWriter>
{
public:
	using Class = ConcreteClass<"Writer", "78cd6480-c2d5-4ad4-a001-98a613d923fa"_uuid, "ReductStore writer">;

	explicit ReductWriter(std::reference_wrapper<ReductClient> client);
	auto forEachAttribute(const model::ForEachAttributeFunction& function) const -> bool final;
	auto forEachEvent(const model::ForEachEventFunction& function) -> bool final;
	auto forEachTask(const model::ForEachTaskFunction& function) -> bool final;
	auto makeReadHandle(const model::Attribute& attribute) const noexcept -> std::optional<data::ReadHandle> final;
	auto category() const noexcept -> model::ElementCategory final;

private:
	struct State
	{
		bool healthy { false };
		std::uint64_t queuedRecords { 0 };
		std::uint64_t rejectedRecords { 0 };
		std::error_code error;
		std::chrono::system_clock::time_point sendTime { std::chrono::system_clock::time_point::min() };
	};

	struct Mapping
	{
		enum class TimestampSource { Scheduled, Source, Update };
		std::weak_ptr<const model::Element> dataPoint;
		std::string entry;
		reductstore_xentara::Labels labels;
		TimestampSource timestampSource { TimestampSource::Scheduled };
		data::ReadHandle value;
		data::ReadHandle quality;
		data::ReadHandle timestamp;
		std::optional<std::chrono::system_clock::time_point> lastTimestamp;
		std::string source;

		auto resolve() -> void;
		[[nodiscard]] auto collect(std::chrono::system_clock::time_point scheduledTime, const std::string& host) -> std::optional<reductstore_xentara::Record>;
	};

	class CollectTask final : public process::Task
	{
	public:
		explicit CollectTask(ReductWriter& target);
		auto operational(const process::ExecutionContext& context) -> void final;
	private:
		std::reference_wrapper<ReductWriter> _target;
	};

	class SendTask final : public process::Task
	{
	public:
		explicit SendTask(ReductWriter& target);
		auto stages() const -> Stages final;
		auto operational(const process::ExecutionContext& context) -> void final;
		auto preparePostOperational(const process::ExecutionContext& context) -> Status final;
	private:
		std::reference_wrapper<ReductWriter> _target;
	};

	auto load(utils::json::decoder::Object& object, config::Context& context) -> void final;
	auto realize() -> void final;
	auto prepare() -> void final;
	auto collect(const process::ExecutionContext& context) -> void;
	auto flush(const process::ExecutionContext& context) -> void;
	auto updateState(std::chrono::system_clock::time_point timestamp) -> void;

	std::reference_wrapper<ReductClient> _client;
	std::vector<Mapping> _mappings;
	process::Event _sent;
	process::Event _error;
	CollectTask _collectTask;
	SendTask _sendTask;
	memory::ObjectBlock<State> _state;
	std::size_t _observedDelivered { 0 };
};
class Skill final : public skill::Skill
{
public:
	auto createElement(const skill::Element::Class& elementClass, skill::ElementFactory& factory) -> std::shared_ptr<skill::Element> final;

private:
	using Class = ConcreteClass<Skill,
		"ReductStore",
		"6c7a07de-c6e4-4bc6-8b2b-840d809e7f1d"_uuid,
		ReductClient::Class>;
	static Class _class;
};

}
