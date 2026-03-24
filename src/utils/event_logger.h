#pragma once

#include <string_view>

#include "nlohmann/json.hpp"

namespace faas {
namespace utils {

void EmitRuntimeEvent(std::string_view event_type, bool ok,
                      const nlohmann::json& fields = nlohmann::json::object());

}  // namespace utils
}  // namespace faas

