#pragma once
#include <cstdint>
#include <vector>

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
    std::vector<char> buffer;

  public:
    explicit ConnectionProtocol() = default;
    explicit ConnectionProtocol(const ConnectionProtocolHeader &header): header(header) {}
    virtual ~ConnectionProtocol() = default;
    [[nodiscard]] virtual int GetSize() const;
    virtual void Serialize();
    virtual void Deserialize(const std::vector<char>& buffer);
    virtual void DeserializeBody(const std::vector<char>& buffer);
    virtual const std::vector<char>& GetSerializedProtocol();
};
