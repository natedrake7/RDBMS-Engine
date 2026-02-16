#pragma once
#include <string_view>
#include <string>
#include "../../Systemic/include/DataTypes/DataTypes.h"

namespace QueryPipeline::Messages {
    static std::string ADDED_VARIABLE(const std::string& name){return std::string("Added variable: ") + name;}
    static std::string FAILED_TO_GET_ROLE(const std::string& role){return std::string("Failed to get role: ") + role;}

    static constexpr std::string_view FAILED_TO_ADD_VARIABLE = "Failed to add variable";

    static constexpr std::string_view FAILED_TO_RETRIEVE_USER_SESSION = "Failed to retrieve user session";
    static constexpr std::string_view FAILED_TO_CREATE_USER = "Failed to create user";


    static constexpr std::string_view USE_DATABASE_SUCCESS = "Database changed successfully";
    static constexpr std::string_view USE_DATABASE_FAIL = "Failed to use Database";

    static std::string INSERT_ROWS_FROM_CHILD_QUERY(const Int count){return std::string("Inserted ") + std::to_string(count) + " rows from child query";}
    static std::string INSERT_ROWS_FROM_FIELDS(const Int count){return std::string("Inserted ") + std::to_string(count) + " rows from fields";}
}
