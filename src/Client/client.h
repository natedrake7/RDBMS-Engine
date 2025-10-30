#pragma once

#include <string>

struct ConnectionParameters {
  int port;
  std::string hostName;
  std::string username;
  std::string password;

  int socket;

  ConnectionParameters() {
    this->port = 0;
    this->socket = 0;
  }
  ~ConnectionParameters() = default;
};