#pragma once
#include "../../Systemic/include/DataTypes/StringView.h"

namespace Network::Messages{
    static constexpr DataTypes::StringView FAILED_TO_GET_USER_SESSION = "Failed to get user session";
    static constexpr DataTypes::StringView FAILED_TO_VALIDATE_SESSION = "Failed to validate session";


    static constexpr DataTypes::StringView QUERY_CANCELLED = "Query was cancelled";

    static constexpr DataTypes::StringView AUTH_REQUEST_MALFORMED_REQUEST = "Malformed request was sent to the server";
    static constexpr DataTypes::StringView AUTH_REQUEST_INVALID_CREDENTIALS = "Invalid credentials provided";
    static constexpr DataTypes::StringView QUERY_REQUEST_NOT_AUTHENTICATED = "User is not authenticated";
    static constexpr DataTypes::StringView QUERY_REQUEST_QUERY_ALREADY_RUNNING = "A query is already running on the current session";

    static constexpr DataTypes::StringView QUERY_REQUEST_RESULT_SET_TOO_LARGE = "A row exceeds the maximum bandwidth";

}
