 #include "../../include/Extensions/StringExtensions.h"
#include <nlohmann/json.hpp>
#include "DataTypes/String.h"

 namespace DataTypes{
    void from_json(const nlohmann::json& j, String& str){
         const auto value = j.get<std::string>();
         str.Reserve(static_cast<Int>(value.size()));
         str.Append(value);
     }
}
