#include "reductstore_xentara/Serialization.hpp"

#include <array>
#include <charconv>
#include <cmath>
#include <stdexcept>

namespace reductstore_xentara
{

namespace
{

template <typename Value>
auto asJsonNumber(Value value) -> std::string
{
	std::array<char, 128> buffer {};
	const auto [end, error] = std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
	if (error != std::errc {})
	{
		throw std::runtime_error("failed to serialize numeric measurement");
	}
	return { buffer.data(), end };
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
	return asJsonNumber(value);
}

}
