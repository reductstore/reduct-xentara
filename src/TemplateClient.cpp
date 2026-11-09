// Copyright (c) embedded ocean GmbH
#include "TemplateClient.hpp"

#include "Attributes.hpp"
#include "TemplateTransaction.hpp"

#include <xentara/config/Errors.hpp>
#include <xentara/data/ReadHandle.hpp>
#include <xentara/memory/WriteSentinel.hpp>
#include <xentara/model/Attribute.hpp>
#include <xentara/model/ForEachAttributeFunction.hpp>
#include <xentara/process/ExecutionContext.hpp>
#include <xentara/skill/ElementFactory.hpp>
#include <xentara/utils/eh/currentErrorCode.hpp>
#include <xentara/utils/json/decoder/Errors.hpp>
#include <xentara/utils/json/decoder/Object.hpp>

#include <string_view>

#ifdef _WIN32
#	include <Windows.h>
#else
#	include <errno.h>
#endif

namespace xentara::plugins::templateUplink
{

using namespace std::literals;

auto TemplateClient::load(utils::json::decoder::Object &jsonObject, config::Context &context) -> void
{
	// Go through all the members of the JSON object that represents this object
	for (auto && [name, value] : jsonObject)
    {
		/// @todo load configuration parameters
		if (name == "TODO"sv)
		{
			/// @todo parse the value correctly
			auto todo = value.asNumber<std::uint64_t>();

			/// @todo check that the value is valid
			if (!"TODO")
			{
				/// @todo use an error message that tells the user exactly what is wrong
				utils::json::decoder::throwWithLocation(value, std::runtime_error("TODO is wrong with TODO parameter of template client"));
			}

			/// @todo set the appropriate member variables
		}
		else
		{
            config::throwUnknownParameterError(name);
		}
    }

	/// @todo perform consistency and completeness checks
	if (!"TODO")
	{
		/// @todo use an error message that tells the user exactly what is wrong
		utils::json::decoder::throwWithLocation(jsonObject, std::runtime_error("TODO is wrong with template client"));
	}
}

auto TemplateClient::createChildElement(const skill::Element::Class &elementClass, skill::ElementFactory &factory)
	-> std::shared_ptr<skill::Element>
{
	if (&elementClass == &TemplateTransaction::Class::instance())
	{
		return factory.makeShared<TemplateTransaction>(*this);
	}

	/// @todo add any other supported child element types

	return nullptr;
}

auto TemplateClient::forEachAttribute(const model::ForEachAttributeFunction &function) const -> bool
{
	/// @todo call the function with any attributes this class supports

	return false;
}

auto TemplateClient::makeReadHandle(const model::Attribute &attribute) const noexcept -> std::optional<data::ReadHandle>
{
	/// @todo create read handles for any readable attributes this class supports

	// Nothing found
	return std::nullopt;
}

auto TemplateClient::prepare() -> void
{
	/// @todo open the handle for the service instance
}

auto TemplateClient::cleanup() -> void
{
	/// @todo close the handle to the service instance
}

} // namespace xentara::plugins::templateUplink