#pragma once
#include "../ConnectionProtocol/ConnectionProtocol.h"

#include <string>

using namespace std;

enum ResponseType : uint8_t {
  InvalidResponse = 0,
  Unauthorized = 1,
  InvalidCredentials = 2,
  Authenticated = 3,
};

static string authorizationSuccess = "Successfully Authenticated";
static string authorizationFailure = "Failed to authenticate";

typedef struct ResponseProtocolHeader{
  uint16_t size;
  ResponseType statusCode;

  ResponseProtocolHeader() : size(0), statusCode(ResponseType::InvalidResponse) {}
  ~ResponseProtocolHeader() = default;
  [[nodiscard]] static int GetSize()  { return sizeof(uint16_t) + sizeof(ResponseType); }
}ResponseProtocolHeader;

class ResponseProtocol {
  protected:
    ResponseProtocolHeader header;
    vector<char> buffer;

  public:
    explicit ResponseProtocol() = default;
    explicit ResponseProtocol(const ResponseProtocolHeader &header): header(header) {}
    explicit ResponseProtocol(const ResponseType& statusCode);
    virtual ~ResponseProtocol() = default;
    [[nodiscard]] virtual int GetSize() const;
    virtual void Serialize();
    virtual void Deserialize(const vector<char>& buffer);
    virtual void DeserializeBody(const vector<char>& buffer);
    virtual const vector<char>& GetSerializedProtocol();
    [[nodiscard]] const ResponseType& GetResponseType() const;
};