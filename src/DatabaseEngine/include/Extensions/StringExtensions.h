#pragma once

#include <nlohmann/json_fwd.hpp>

namespace DataTypes{
    class String;

    void from_json(const nlohmann::json& j, String& str);
}
