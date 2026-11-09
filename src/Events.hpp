// Copyright (c) embedded ocean GmbH
#pragma once

#include <xentara/process/Event.hpp>

/// @brief Contains the Xentara events of the uplink
namespace xentara::plugins::templateUplink::events
{

/// @brief A Xentara event that is raised when the records were successfully sent to the client
extern const process::Event::Role kSent;
/// @brief A Xentara event that is raised when a send error occurred
extern const process::Event::Role kSendError;

} // namespace xentara::plugins::templateUplink::events