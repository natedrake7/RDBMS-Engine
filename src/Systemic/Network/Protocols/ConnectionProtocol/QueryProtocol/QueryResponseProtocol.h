#pragma once
#include "../../../../DataStructures/BitMap/BitMap.h"
#include "../../../../QueryResult/QueryResult.h"
#include "../ResponseProtocol/ResponseProtocol.h"

namespace Network {
  //add to body table headers for response
  //and also null fields too

  class QueryResponseProtocol final : public ResponseProtocol{
    bool hasError;
    std::string message;

    std::vector<std::string> columns;
    std::vector<QueryResult> rows;

    void SerializeMessage();
    void SerializeResult();
    void AssignBufferSizeToProtocolSize();

    void DeserializeMessage(const std::vector<char>& buffer, uint32_t& offSet);
    void DeserializeResult(const std::vector<char>& buffer, uint32_t& offSet);

    public:
      QueryResponseProtocol();
      ~QueryResponseProtocol() override;

      explicit QueryResponseProtocol(const ResponseProtocolHeader &header);
      explicit QueryResponseProtocol(const ResponseType& statusCode, const DataTypes::Guid& sessionId);
      explicit QueryResponseProtocol(const std::string& errorMessage);
      explicit QueryResponseProtocol(
        const bool& hasError,
        const std::string& message,
        const std::vector<std::string>& columns,
        std::vector<QueryResult>& rows
      );
      [[nodiscard]] int GetSize() const override;
      void Serialize() override;
      void Deserialize(const std::vector<char>& buffer) override;

      friend ostream& operator<<(ostream& os, const QueryResponseProtocol& protocol);
  };

}
