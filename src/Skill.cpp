// Copyright (c) embedded ocean GmbH
#include "Skill.hpp"

#include <xentara/skill/Element.hpp>
#include <xentara/skill/ElementFactory.hpp>

namespace xentara::plugins::templateUplink
{

Skill::Class Skill::_class;

auto Skill::createElement(const skill::Element::Class &elementClass, skill::ElementFactory &factory)
	-> std::shared_ptr<skill::Element>
{
	if (&elementClass == &TemplateClient::Class::instance())
	{
		return factory.makeShared<TemplateClient>();
	}

	/// @todo handle any additional top-level microservice classes

	return nullptr;
}

} // namespace xentara::plugins::templateUplink