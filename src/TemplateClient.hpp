// Copyright (c) embedded ocean GmbH
#pragma once

#include "Attributes.hpp"
#include "CustomError.hpp"

#include <xentara/memory/Array.hpp>
#include <xentara/model/ElementCategory.hpp>
#include <xentara/process/Event.hpp>
#include <xentara/process/Task.hpp>
#include <xentara/skill/Element.hpp>
#include <xentara/skill/EnableSharedFromThis.hpp>
#include <xentara/utils/core/Uuid.hpp>
#include <xentara/utils/tools/Unique.hpp>

#include <string_view>
#include <functional>
#include <forward_list>

namespace xentara::plugins::templateUplink
{

using namespace std::literals;

/// @brief A class representing a client for specific type of service that data can be sent to.
/// @todo rename this class to something more descriptive
class TemplateClient final : public skill::Element, public skill::EnableSharedFromThis<TemplateClient>
{
public:
	/// @brief The class object containing meta-information about this element type
	/// @todo change class name
	/// @todo assign a unique UUID
	/// @todo change display name
	using Class =
		ConcreteClass<"TemplateClient", "deadbeef-dead-beef-dead-beefdeadbeef"_uuid, "template uplink client">;

	/// @brief A handle used to access the client
	/// @todo implement a proper handle
	class Handle final : private utils::tools::Unique
	{
	};

	/// @brief Returns a handle to the client
	auto handle() const -> const Handle &
	{
		return _handle;
	}

	/// @name Virtual Overrides for skill::Element
	/// @{

	auto createChildElement(const skill::Element::Class &elementClass, skill::ElementFactory &factory) -> std::shared_ptr<skill::Element> final;

	auto forEachAttribute(const model::ForEachAttributeFunction &function) const -> bool final;

	auto makeReadHandle(const model::Attribute &attribute) const noexcept -> std::optional<data::ReadHandle> final;

	auto category() const noexcept -> model::ElementCategory final
	{
		/// @todo Return an appropriate category
		return model::ElementCategory::DataStorage;
	}
	
	/// @}

private:
	/// @name Virtual Overrides for skill::Element
	/// @{

	auto load(utils::json::decoder::Object &jsonObject, config::Context &context) -> void final;

	auto prepare() -> void final;

	auto cleanup() -> void final;

	/// @}

	/// @brief A handle to the client
	Handle _handle;
};

} // namespace xentara::plugins::templateUplink