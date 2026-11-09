// Copyright (c) embedded ocean GmbH
#include "TemplateRecord.hpp"

#include <xentara/utils/json/decoder/Object.hpp>
#include <xentara/config/Errors.hpp>
#include <xentara/data/Quality.hpp>

#include <format>
#include <string_view>
#include <stdexcept>

namespace xentara::plugins::templateUplink
{
	
using namespace std::literals;

auto TemplateRecord::load(utils::json::decoder::Value &value, config::Context &context) -> void
{
	// Interpret the value as an object
	auto jsonObject = value.asObject();

	// Go through all the members of the JSON object that represents this object
	bool dataPointLoaded = false;
	bool remoteIdLoaded = false;
	for (auto && [name, value] : jsonObject)
    {
		if (name == "dataPoint"sv)
		{
			// Resolve the data point
			context.resolve<model::Element>(value, std::ref(_dataPoint));
			dataPointLoaded = true;
		}
		/// @todo use something specific to the key used by the remote service, like e.g. "objectName"
		else if (name == "remoteId"sv)
		{
			/// @todo load the remote ID using the correct type etc.
			auto remoteId = value.asString<std::string>();

			/// @todo perform more thorough validity check
			if (remoteId.empty())
			{
				/// @todo write a more specific error message
				utils::json::decoder::throwWithLocation(value, std::runtime_error("empty remote ID for template transaction record"));
			}

			// Set the key
			_remoteId = std::move(remoteId);
			remoteIdLoaded = true;
		}
		/// @todo load additional configuration parameters
		else if (name == "TODO"sv)
		{
			/// @todo parse the value correctly
			auto todo = value.asNumber<std::uint64_t>();

			/// @todo check that the value is valid
			if (!"TODO")
			{
				/// @todo use an error message that tells the user exactly what is wrong
				utils::json::decoder::throwWithLocation(value, std::runtime_error("TODO is wrong with TODO parameter of template transaction record"));
			}

			/// @todo set the appropriate member variables
		}
		else
		{
            config::throwUnknownParameterError(name);
		}
    }

	// Check that a data point was specified
	if (!dataPointLoaded)
	{
		utils::json::decoder::throwWithLocation(jsonObject, std::runtime_error("missing data point for template transaction record"));
	}
	// If the remote key is still empty, it wasn't loaded
	if (!remoteIdLoaded)
	{
		utils::json::decoder::throwWithLocation(jsonObject, std::runtime_error("missing remote ID for template transaction record"));
	}
	/// @todo perform additional consistency and completeness checks
}

auto TemplateRecord::collect(std::chrono::system_clock::time_point timeStamp, utils::core::RawDataBlock &data) const
	-> void
{
	// Read the data
	auto value = _valueReadHandle.read<std::string>();
	/// @todo read the value as a more suitable type
	auto quality = _qualityReadHandle.read<data::Quality>();

	/// @todo read other attributes that should be sent

	if (!value || ! quality)
	{
		/// @todo do appropriate error handling, like sending an error status for to the remote service

		return;
	}

	/// @todo encode the record and append it to the data
}

auto TemplateRecord::resolveHandles() -> void
{
	// Get the data point
	if (auto dataPoint = _dataPoint.lock())
	{
		// Get the value read handle
		_valueReadHandle = dataPoint->attributeReadHandle(model::Attribute::kValue);
		// Check it
		if (auto error = _valueReadHandle.hardError())
		{
			throw std::system_error(error,
				std::format(
					"could not construct read handle for the value of {} for template transaction record", *dataPoint));
		}

		// Get the quality read handle
		_qualityReadHandle = dataPoint->attributeReadHandle(model::Attribute::kValue);
		// Check it
		if (auto error = _qualityReadHandle.hardError())
		{
			throw std::system_error(error,
				std::format("could not construct read handle for the quality of {} for template transaction record",
					*dataPoint));
		}

		/// @todo resolve read handles for other attributes that should be sent
	}
}

} // namespace xentara::plugins::templateUplink
