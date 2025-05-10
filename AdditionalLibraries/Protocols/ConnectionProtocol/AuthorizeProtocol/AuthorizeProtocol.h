#pragma once
#include <string>

#include "../ConnectionProtocol/ConnectionProtocol.h"

using namespace std;

class AuthorizeProtocol final : public ConnectionProtocol{
  string username;
  string password;

  public:
    AuthorizeProtocol() = default;
    explicit AuthorizeProtocol(const ConnectionProtocolHeader& header): ConnectionProtocol(header){}
    AuthorizeProtocol(const string& username, const string& password);
    explicit AuthorizeProtocol(const vector<unsigned char>& buffer);
    
    ~AuthorizeProtocol() override = default;
    
    [[nodiscard]] int GetSize() const override;
    void Serialize() override;
    void Deserialize(const vector<unsigned char>& buffer) override;

    [[nodiscard]] const string& GetUsername() const;
    [[nodiscard]] const string& GetPassword() const;
};
