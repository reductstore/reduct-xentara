// Copyright (c) embedded ocean GmbH
#pragma once

#include "TemplateClient.hpp"
#include "TemplateTransaction.hpp"

#include <xentara/skill/Skill.hpp>
#include <xentara/utils/core/Uuid.hpp>

#include <string_view>

namespace xentara::plugins::templateUplink
{

using namespace std::literals;

/// @brief The skill
class Skill final : public skill::Skill
{
public:
	/// @class xentara::plugins::templateUplink::Skill
	/// @todo add constructor to perform global initialization, if necessary

	/// @class xentara::plugins::templateUplink::Skill
	/// @todo add destructor to perform global cleanup, if necessary

	/// @name Virtual Overrides for skill::Skill
	/// @{

	auto createElement(const skill::Element::Class &elementClass, skill::ElementFactory &factory)
		-> std::shared_ptr<skill::Element> final;

	/// @}

private:
	/// @brief The skill class
	/// @todo change class name
	/// @todo assign a unique UUID
	using Class = ConcreteClass<Skill,
		"TemplateUplink",
		"deadbeef-dead-beef-dead-beefdeadbeef"_uuid,
		TemplateClient::Class,
		TemplateTransaction::Class>;

	/// @brief The skill class object
	static Class _class;
};

} // namespace xentara::plugins::templateUplink