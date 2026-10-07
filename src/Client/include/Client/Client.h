#pragma once

#include <string>

#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/Network/Socket.h>

struct ConnectionParameters {
    std::string _hostname;
    std::string _username;
    std::string _password;

    Int _port;
    Network::socket_t _socket;

    ConnectionParameters()
        : _port(0), _socket(0){}
};
