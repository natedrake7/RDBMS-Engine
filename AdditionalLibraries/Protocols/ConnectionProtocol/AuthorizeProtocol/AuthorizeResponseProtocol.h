#pragma once
#include "../ResponseProtocol/ResponseProtocol.h"

class AuthorizeResponseProtocol final : public ResponseProtocol {
  public:
    explicit AuthorizeResponseProtocol(const ResponseType& statusCode) : ResponseProtocol(statusCode) {};
    AuthorizeResponseProtocol() : ResponseProtocol() {};
};