#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
namespace reductstore_xentara
{

[[nodiscard]] auto serializeScalar(bool value) -> std::string;
[[nodiscard]] auto serializeScalar(std::intmax_t value) -> std::string;
[[nodiscard]] auto serializeScalar(std::uintmax_t value) -> std::string;
[[nodiscard]] auto serializeScalar(double value) -> std::string;
[[nodiscard]] auto trySerializeScalar(double value) -> std::optional<std::string>;
[[nodiscard]] auto serializeString(std::string_view value) -> std::string;

[[nodiscard]] auto serializeObject(const std::vector<std::pair<std::string, std::string>>& fields) -> std::string;
}
