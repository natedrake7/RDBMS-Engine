#pragma once
#include "../../../DatabaseEngine/include/DataStorage/Block.h"
#include "ConnectionHeader.h"
#include "ConnectionProtocol.h"

#include <string>

using namespace std;

enum ResponseType : uint8_t {
  InvalidResponse = 0,
  Unauthorized = 1,
  InvalidCredentials = 2,
  Authenticated = 3,
  QueryResponse = 4,
};

static string authorizationSuccess = "Successfully Authenticated";
static string authorizationFailure = "Failed to authenticate";

namespace Network {
  struct ResponseProtocolHeader final : Network::ConnectionHeader{
    ResponseType statusCode;

    ResponseProtocolHeader() : ConnectionHeader(), statusCode(ResponseType::InvalidResponse) {}
    ~ResponseProtocolHeader()override = default;

    void Serialize(std::vector<char> &data) override;
    void Deserialize(const std::vector<char> &data) override;
    [[nodiscard]] constexpr static int GetSize()  { return static_cast<int>(ConnectionHeader::Size() + sizeof(ResponseType));}
  };

  class ResponseProtocol{
    protected:
      ResponseProtocolHeader header;
      vector<char> buffer;

    public:
      explicit ResponseProtocol() = default;
      explicit ResponseProtocol(const ResponseProtocolHeader &header): header(header) {}
      explicit ResponseProtocol(const ResponseType& statusCode, const DataTypes::Guid& sessionId);
      virtual ~ResponseProtocol() = default;
      [[nodiscard]] virtual int GetSize() const;
      virtual void Serialize();
      virtual void Deserialize(const vector<char>& responseBuffer);
      virtual void DeserializeBody(const vector<char>& buffer);
      virtual const vector<char>& GetSerializedProtocol();
      [[nodiscard]] const ResponseType& GetResponseType() const;
      [[nodiscard]] const DataTypes::Guid& GetSessionId() const;
  };

}
