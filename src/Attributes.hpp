// Copyright (c) embedded ocean GmbH
#pragma once


#include <xentara/model/Attribute.hpp>

/// @brief Contains the Xentara attributes of the uplink skill
namespace xentara::plugins::templateUplink::attributes
{

/// @brief A Xentara attribute containing the state of a client connection
extern const model::Attribute kConnectionState;
/// @brief A Xentara attribute containing the state of a transaction
extern const model::Attribute kTransactionState;

/// @brief A Xentara attribute containing the connection time for a client
extern const model::Attribute kConnectionTime;
/// @brief A Xentara attribute containing the send time for a transaction
extern const model::Attribute kSendTime;

/// @brief A Xentara attribute containing an error code for a client connection
extern const model::Attribute kError;

} // namespace xentara::plugins::templateUplink::attributes