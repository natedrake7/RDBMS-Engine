#pragma once
#include "ConnectionHeader.h"

#include <cstdint>
#include <vector>

namespace Network {
  enum ConnectionProtocolType : UnsignedTinyInt {
    Invalid = 0,
    Authorize = 1,
    Query = 2,
  };

  typedef struct ConnectionProtocolHeader final : ConnectionHeader {
    ConnectionProtocolType type;

    ConnectionProtocolHeader() : ConnectionHeader(), type(ConnectionProtocolType::Invalid) {}
    ~ConnectionProtocolHeader() override = default;
    void Serialize(std::vector<char> &responseBuffer) override;
    void Deserialize(const std::vector<char> &responseBuffer) override;

    constexpr static Int GetSize() { return ConnectionHeader::Size() + sizeof(ConnectionProtocolType); }
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
      virtual void Deserialize(const std::vector<char>& responseBuffer);
      virtual const std::vector<char>& GetSerializedProtocol();
  };

}
