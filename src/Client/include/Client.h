#pragma once

#include <string>

#include "DataTypes/DataTypes.h"

struct ConnectionParameters {
    std::string hostName;
    std::string username;
    std::string password;

    Int port;
    Int socket;

    ConnectionParameters() = default;
};
