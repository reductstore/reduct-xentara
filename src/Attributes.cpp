// Copyright (c) embedded ocean GmbH
#include "Attributes.hpp"

#include <xentara/utils/core/Uuid.hpp>
#include <xentara/data/DataType.hpp>

#include <string_view>

namespace xentara::plugins::templateUplink::attributes
{

using namespace std::literals;

/// @todo assign a unique UUID
const model::Attribute kConnectionState { "deadbeef-dead-beef-dead-beefdeadbeef"_uuid, "connectionState"sv, model::Attribute::Access::ReadOnly, data::DataType::kBoolean };

/// @todo assign a unique UUID
const model::Attribute kTransactionState { "deadbeef-dead-beef-dead-beefdeadbeef"_uuid, "transactionState"sv, model::Attribute::Access::ReadOnly, data::DataType::kBoolean };

/// @todo assign a unique UUID
const model::Attribute kConnectionTime { "deadbeef-dead-beef-dead-beefdeadbeef"_uuid, "connectionTime"sv, model::Attribute::Access::ReadOnly, data::DataType::kTimeStamp };

/// @todo assign a unique UUID
const model::Attribute kSendTime { "deadbeef-dead-beef-dead-beefdeadbeef"_uuid, "sendTime"sv, model::Attribute::Access::ReadOnly, data::DataType::kTimeStamp };

const model::Attribute kError { model::Attribute::kError, model::Attribute::Access::ReadOnly, data::DataType::kErrorCode };

} // namespace xentara::plugins::templateUplink::attributes
