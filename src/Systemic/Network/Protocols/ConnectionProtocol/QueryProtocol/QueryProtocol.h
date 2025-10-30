#pragma once
#include <string>
#include "../ConnectionProtocol/ConnectionProtocol.h"

namespace Network {
  class QueryProtocol final : public ConnectionProtocol{
    std::string query;

    public:
      explicit QueryProtocol(const std::string& query, const DataTypes::Guid& sessionId);
      explicit QueryProtocol(const ConnectionProtocolHeader& header): ConnectionProtocol(header){}
      ~QueryProtocol() override = default;
      [[nodiscard]] int GetSize() const override;
      void Serialize() override;
      void Deserialize(const std::vector<char>& buffer) override;
      [[nodiscard]] const std::string& GetQuery() const;
  };

}
