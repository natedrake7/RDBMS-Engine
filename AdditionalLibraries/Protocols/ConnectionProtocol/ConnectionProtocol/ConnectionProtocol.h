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
  
  ConnectionProtocolHeader(): size(0), dataType(ConnectionProtocolType::Invalid) {}
}ConnectionProtocolHeader;

class ConnectionProtocol {
  protected:
    ConnectionProtocolHeader header;
    vector<unsigned char> buffer;

  public:
    explicit ConnectionProtocol() = default;
    explicit ConnectionProtocol(const ConnectionProtocolHeader &header): header(header) {}
    virtual ~ConnectionProtocol() = default;
    [[nodiscard]] virtual int GetSize() const;
    virtual void Serialize();
    virtual void Deserialize(const vector<unsigned char>& buffer);
    virtual const vector<unsigned char>& GetSerializedProtocol();
};
