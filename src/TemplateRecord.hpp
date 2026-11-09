// Copyright (c) embedded ocean GmbH
#pragma once

#include <xentara/config/Context.hpp>
#include <xentara/data/ReadHandle.hpp>
#include <xentara/model/Element.hpp>
#include <xentara/utils/core/RawDataBlock.hpp>
#include <xentara/utils/json/decoder/Value.hpp>

#include <chrono>
#include <memory>
#include <string>

namespace xentara::plugins::templateUplink
{

/// @brief A class representing a single record in a transaction.
/// @todo rename this class to something more descriptive
class TemplateRecord final
{
public:
	/// @brief Loads the record from a JSON value
	auto load(utils::json::decoder::Value &value, config::Context &context) -> void;

	/// @brief Collects the data from the record and appends it to a data block
	auto collect(std::chrono::system_clock::time_point timeStamp, utils::core::RawDataBlock &data) const -> void;

	/// @brief Reslves read handles
	auto resolveHandles() -> void;

private:
	/// @brief The data point
	std::weak_ptr<const model::Element> _dataPoint;
	/// @brief The ID of the record in the namespace of the remote service
	/// @todo use the appropriate type here
	/// @todo rename the variable into something specific to the key used by the remote service, like e.g. "objectName"
	std::string _remoteId;

	/// @class xentara::plugins::templateUplink::TemplateRecord
	/// @todo add more properties needed for the record, e.g. data type

	/// @brief The read handle for the value
	data::ReadHandle _valueReadHandle;
	/// @brief The read handle for the quality
	data::ReadHandle _qualityReadHandle;

	/// @todo add read handles for other attributes that should be sent
};

} // namespace xentara::plugins::templateUplink
