// Copyright (c) embedded ocean GmbH
#pragma once

#include "TemplateRecord.hpp"
#include "CustomError.hpp"
#include "Attributes.hpp"

#include <xentara/memory/Array.hpp>
#include <xentara/model/ElementCategory.hpp>
#include <xentara/process/Event.hpp>
#include <xentara/process/Task.hpp>
#include <xentara/skill/Element.hpp>
#include <xentara/skill/EnableSharedFromThis.hpp>
#include <xentara/utils/core/RawDataBlock.hpp>
#include <xentara/utils/core/Uuid.hpp>

#include <functional>
#include <string_view>
#include <forward_list>

namespace xentara::plugins::templateUplink
{

using namespace std::literals;

class TemplateClient;

/// @brief A class representing a data transaction for writing data to a client.
/// @todo rename this class to something more descriptive
class TemplateTransaction final : public skill::Element, public skill::EnableSharedFromThis<TemplateTransaction>
{
public:
	/// @brief The class object containing meta-information about this element type
	/// @todo change class name
	/// @todo assign a unique UUID
	/// @todo change display name
	using Class = ConcreteClass<"TemplateTransaction",
		"deadbeef-dead-beef-dead-beefdeadbeef"_uuid,
		"template uplink transaction">;

	/// @brief This constructor attaches the output to its client
	TemplateTransaction(std::reference_wrapper<TemplateClient> client) :
		_client(client)
	{
	}

	/// @name Virtual Overrides for skill::Element
	/// @{

	auto forEachAttribute(const model::ForEachAttributeFunction &function) const -> bool final;
	
	auto forEachEvent(const model::ForEachEventFunction &function) -> bool final;

	auto forEachTask(const model::ForEachTaskFunction &function) -> bool final;

	auto makeReadHandle(const model::Attribute &attribute) const noexcept -> std::optional<data::ReadHandle> final;

	auto category() const noexcept -> model::ElementCategory final
	{
		return model::ElementCategory::Transaction;
	}
	
	/// @}

private:
	/// @brief This structure represents the current state of the transaction
	struct State final
	{
		/// @brief Whether the transaction is being transmitted correctly
		bool _transactionState { false };
		/// @brief The last time the records were sent (successfully or not)
		std::chrono::system_clock::time_point _sendTime { std::chrono::system_clock::time_point::min() };
		/// @brief The last error code when sending the records, or a default constructed std::error_code object for none.
		std::error_code _error { CustomError::Pending };
	};

	/// @brief This class providing callbacks for the Xentara scheduler for the "collect" task
	class CollectTask final : public process::Task
	{
	public:
		/// @brief This constuctor attached the task to its target
		CollectTask(std::reference_wrapper<TemplateTransaction> target) : _target(target)
		{
		}

		/// @name Virtual Overrides for process::Task
		/// @{

		auto operational(const process::ExecutionContext &context) -> void final;

		/// @}

	private:
		/// @brief A reference to the target element
		std::reference_wrapper<TemplateTransaction> _target;
	};

	/// @brief This class providing callbacks for the Xentara scheduler for the "send" task
	class SendTask final : public process::Task
	{
	public:
		/// @brief This constuctor attached the task to its target
		SendTask(std::reference_wrapper<TemplateTransaction> target) : _target(target)
		{
		}

		/// @name Virtual Overrides for process::Task
		/// @{

		auto stages() const -> Stages final
		{
			return Stage::Operational | Stage::PostOperational;
		}

		auto operational(const process::ExecutionContext &context) -> void final;

		auto preparePostOperational(const process::ExecutionContext &context) -> Status final;

		/// @}

	private:
		/// @brief A reference to the target element
		std::reference_wrapper<TemplateTransaction> _target;
	};

	/// @brief This function is called by the "collect" task.
	///
	/// This function collects data to be sent to the client.
	auto performCollectTask(const process::ExecutionContext &context) -> void;
	/// @brief Collects the data for all the records and appends it to the pending data
	auto collectData(std::chrono::system_clock::time_point timeStamp) -> void;

	/// @brief This function is called by the "send" task.
	///
	/// This function attempts to send the collected records if the client is up.
	auto performSendTask(const process::ExecutionContext &context) -> void;
	/// @brief Attempts to write send the collected records to the client and updates the state accordingly.
	auto send(std::chrono::system_clock::time_point timeStamp) -> void;	
	/// @brief Handles a send error
	auto handleSendError(std::chrono::system_clock::time_point timeStamp, std::error_code error) -> void;

	/// @brief Updates the state and sends the correct event
	auto updateState(std::chrono::system_clock::time_point timeStamp, std::error_code error = std::error_code()) -> void;

	/// @name Virtual Overrides for skill::Element
	/// @{

	auto load(utils::json::decoder::Object &jsonObject, config::Context &context) -> void final;

	auto realize() -> void final;

	auto prepare() -> void final;

	/// @}

	/// @brief The client this transaction belongs to
	std::reference_wrapper<TemplateClient> _client;
	/// @brief The records to be collected
	std::forward_list<TemplateRecord> _records;

	/// @brief The data to be sent
	/// @todo use a more appropriate type for buffering the data
	utils::core::RawDataBlock _pendingData;

	/// @brief A Xentara event that is raised when the records were successfully sent to the client
	process::Event _sentEvent;
	/// @brief A Xentara event that is raised when a send error occurred
	process::Event _sendErrorEvent;

	/// @brief The "collect" task
	CollectTask _collectTask { *this };
	/// @brief The "send" task
	SendTask _sendTask { *this };

	/// @brief The data block that contains the state
	memory::ObjectBlock<State> _stateDataBlock;
};

} // namespace xentara::plugins::templateUplink