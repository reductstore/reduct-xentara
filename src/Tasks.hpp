// Copyright (c) embedded ocean GmbH
#pragma once

#include <xentara/process/Task.hpp>

/// @brief Contains the Xentara tasks of the uplink
namespace xentara::plugins::templateUplink::tasks
{

/// @brief A Xentara task used to collect data for a transaction
extern const process::Task::Role kCollect;
/// @brief A Xentara task used to send a transaction
extern const process::Task::Role kSend;

} // namespace xentara::plugins::templateUplink::tasks