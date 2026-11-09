// Copyright (c) embedded ocean GmbH
#include "Events.hpp"

#include <xentara/utils/core/Uuid.hpp>

#include <string_view>

namespace xentara::plugins::templateUplink::events
{

using namespace std::literals;
using namespace xentara::literals;

/// @todo assign a unique UUID
const process::Event::Role kSent { "deadbeef-dead-beef-dead-beefdeadbeef"_uuid, "sent"sv };

/// @todo assign a unique UUID
const process::Event::Role kSendError { "deadbeef-dead-beef-dead-beefdeadbeef"_uuid, "sendError"sv };

} // namespace xentara::plugins::templateUplink::events