#pragma once
#include "ConnectionProtocol.h"
#include <string>


namespace Network {
  class AuthorizeProtocol final : public ConnectionProtocol{
    std::string username;
    std::string password;

    public:
      AuthorizeProtocol() = default;
      explicit AuthorizeProtocol(const ConnectionProtocolHeader& header): ConnectionProtocol(header){}
      AuthorizeProtocol(const std::string& username, const std::string& password);
      explicit AuthorizeProtocol(const std::vector<char>& buffer);

      ~AuthorizeProtocol() override = default;

      [[nodiscard]] int GetSize() const override;
      void Serialize() override;
      void Deserialize(const std::vector<char>& buffer) override;

      [[nodiscard]] const std::string& GetUsername() const;
      [[nodiscard]] const std::string& GetPassword() const;
  };
}
