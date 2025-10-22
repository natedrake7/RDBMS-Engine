#pragma once
#include <string>
#include "../ConnectionProtocol/ConnectionProtocol.h"

using namespace std;

class QueryProtocol final : public ConnectionProtocol{
  string query;

  public:
    explicit QueryProtocol(const string& query);
    explicit QueryProtocol(const ConnectionProtocolHeader& header): ConnectionProtocol(header){}
    ~QueryProtocol() override = default;
    [[nodiscard]] int GetSize() const override;
    void Serialize() override;
    void Deserialize(const vector<char>& buffer) override;
    [[nodiscard]] const string& GetQuery() const;
};