#pragma once
#include <string_view>

namespace Network::Messages{
    static constexpr std::string_view FAILED_TO_GET_USER_SESSION = "Failed to get user session";
    static constexpr std::string_view FAILED_TO_VALIDATE_SESSION = "Failed to validate session";
}
