// Copyright (c) embedded ocean GmbH
#include "Tasks.hpp"

#include <xentara/utils/core/Uuid.hpp>

#include <string_view>

namespace xentara::plugins::templateUplink::tasks
{

using namespace std::literals;
using namespace xentara::literals;

/// @todo assign a unique UUID
const process::Task::Role kCollect { "deadbeef-dead-beef-dead-beefdeadbeef"_uuid, "collect"sv };

/// @todo assign a unique UUID
const process::Task::Role kSend { "deadbeef-dead-beef-dead-beefdeadbeef"_uuid, "send"sv };

} // namespace xentara::plugins::templateUplink::tasks