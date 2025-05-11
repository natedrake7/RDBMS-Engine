#pragma once
#include "../../../BitMap/BitMap.h"
#include "../ResponseProtocol/ResponseProtocol.h"

typedef struct ResponseRow {
  ByteMaps::BitMap nullBitMap;
  vector<string> columns;

  ResponseRow() = default;
  explicit ResponseRow(const vector<string>& columns, const ByteMaps::BitMap& nullBitMap);
  ~ResponseRow() = default;
  [[nodiscard]] int GetSize() const;
}ResponseRow;

//add to body table headers for response
//and also null fields too

class QueryResponseProtocol final : public ResponseProtocol{
  bool hasError;
  string errorMessage;
  
  vector<string> columns;
  vector<ResponseRow> rows;
  
  public:
    QueryResponseProtocol();
    ~QueryResponseProtocol() override;

    explicit QueryResponseProtocol(const ResponseProtocolHeader &header);
    explicit QueryResponseProtocol(const ResponseType& statusCode);
    explicit QueryResponseProtocol(const string& errorMessage);
    explicit QueryResponseProtocol(const vector<string>& columns, const vector<ResponseRow>& rows);
    [[nodiscard]] int GetSize() const override;
    void Serialize() override;
    void Deserialize(const vector<char>& buffer) override;

    friend ostream& operator<<(ostream& os, const QueryResponseProtocol& protocol);
};