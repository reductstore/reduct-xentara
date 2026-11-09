#pragma once

#include <cstdint>
#include <string>

namespace reductstore_xentara
{

[[nodiscard]] auto serializeScalar(bool value) -> std::string;
[[nodiscard]] auto serializeScalar(std::intmax_t value) -> std::string;
[[nodiscard]] auto serializeScalar(std::uintmax_t value) -> std::string;
[[nodiscard]] auto serializeScalar(double value) -> std::string;

}
