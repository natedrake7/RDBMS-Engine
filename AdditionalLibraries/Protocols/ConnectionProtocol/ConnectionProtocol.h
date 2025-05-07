#pragma once
#include <cstdint>
#include <vector>

using namespace std;

enum ConnectionProtocolType : uint8_t {
  Invalid = 0,
  Authorize = 1,
  Query = 2,
};

typedef struct ConnectionProtocolHeader {
    uint16_t size;
    ConnectionProtocolType dataType;
  }ConnectionProtocolHeader;

typedef struct ConnectionProtocol {
  ConnectionProtocolHeader header{};
  vector<unsigned char> buffer;

  ConnectionProtocol(){
    this->header = {
      .size = 0,
      .dataType = ConnectionProtocolType::Invalid
    };
  }
  ~ConnectionProtocol() = default;
}ConnectionProtocol;