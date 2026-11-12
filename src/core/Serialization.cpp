#include "reductstore_xentara/Serialization.hpp"

#include <array>
#include <charconv>
#include <cmath>
#include <stdexcept>
#include <string_view>

namespace reductstore_xentara
{

namespace
{

template <typename Value>
auto asJsonNumber(Value value, bool preserveFloatingPoint = false) -> std::string
{
	std::array<char, 128> buffer {};
	const auto [end, error] = std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
	if (error != std::errc {})
	{
		throw std::runtime_error("failed to serialize numeric measurement");
	}
	const auto number = std::string_view(buffer.data(), static_cast<std::size_t>(end - buffer.data()));
	if (!preserveFloatingPoint || number.find_first_of(".eE") != std::string_view::npos)
	{
		return std::string(number);
	}
	std::string result;
	result.reserve(number.size() + 2);
	result.append(number);
	result.append(".0");
	return result;
}


auto jsonStringSize(std::string_view value) -> std::size_t
{
	auto size = std::size_t { 2 };
	for (const auto character : value)
	{
		const auto byte = static_cast<unsigned char>(character);
		size += byte < 0x20 ? 6 : (character == '"' || character == '\\' ? 2 : 1);
	}
	return size;
}

auto appendJsonString(std::string& output, std::string_view value) -> void
{
	static constexpr char hex[] = "0123456789abcdef";
	output.push_back('"');
	for (const auto character : value)
	{
		const auto byte = static_cast<unsigned char>(character);
		if (byte < 0x20)
		{
			output.append("\\u00");
			output.push_back(hex[byte >> 4]);
			output.push_back(hex[byte & 0x0f]);
		}
		else if (character == '"' || character == '\\')
		{
			output.push_back('\\');
			output.push_back(character);
		}
		else
		{
			output.push_back(character);
		}
	}
	output.push_back('"');
}
}

auto serializeScalar(bool value) -> std::string
{
	return value ? "true" : "false";
}

auto serializeScalar(std::intmax_t value) -> std::string
{
	return asJsonNumber(value);
}

auto serializeScalar(std::uintmax_t value) -> std::string
{
	return asJsonNumber(value);
}

auto serializeScalar(double value) -> std::string
{
	if (!std::isfinite(value))
	{
		throw std::invalid_argument("JSON does not support non-finite floating-point values");
	}
	return asJsonNumber(value, true);
}

auto trySerializeScalar(double value) -> std::optional<std::string>
{
	if (!std::isfinite(value))
	{
		return std::nullopt;
	}
	return asJsonNumber(value, true);
}

auto serializeString(std::string_view value) -> std::string
{
	std::string result;
	result.reserve(jsonStringSize(value));
	appendJsonString(result, value);
	return result;
}

auto serializeObject(const std::vector<std::pair<std::string, std::string>>& fields) -> std::string
{
	auto size = std::size_t { 2 };
	for (const auto& [name, value] : fields)
	{
		size += jsonStringSize(name) + 1 + value.size();
	}
	if (fields.size() > 1)
	{
		size += fields.size() - 1;
	}
	std::string payload;
	payload.reserve(size);
	payload.push_back('{');
	for (auto iterator = fields.begin(); iterator != fields.end(); ++iterator)
	{
		if (iterator != fields.begin())
		{
			payload.push_back(',');
		}
		appendJsonString(payload, iterator->first);
		payload.push_back(':');
		payload.append(iterator->second);
	}
	payload.push_back('}');
	return payload;
}

}
