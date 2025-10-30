#include "QueryResponseProtocol.h"

#include <cstring>
#include <ostream>

namespace Network {
  // ResponseRow::ResponseRow(const std::vector<std::string> &columns, const ByteMaps::BitMap& nullBitMap){
  //   this->columns = columns;
  //   this->nullBitMap = nullBitMap;
  // }
  //
  // int ResponseRow::GetSize() const{
  //   int size = this->nullBitMap.GetSizeInBytes();
  //
  //   for (const auto& column: columns)
  //     size += sizeof(int) + column.size();
  //
  //   return size;
  // }

QueryResponseProtocol::QueryResponseProtocol() : ResponseProtocol() {
    this->hasError = false;
    this->header.size = sizeof(bool);
    this->header.statusCode = ResponseType::QueryResponse;
  }

  QueryResponseProtocol::~QueryResponseProtocol() = default;

  QueryResponseProtocol::QueryResponseProtocol(const ResponseProtocolHeader &header) : ResponseProtocol(header) {
    this->hasError = false;
    this->header.size = sizeof(bool);
    this->header.statusCode = ResponseType::QueryResponse;
  }

  QueryResponseProtocol::QueryResponseProtocol(const ResponseType &statusCode, const DataTypes::Guid& sessionId) : ResponseProtocol(statusCode, sessionId) {
    this->hasError = false;
    this->header.size = sizeof(bool);
    this->header.statusCode = ResponseType::QueryResponse;
  }

  QueryResponseProtocol::QueryResponseProtocol(const string &errorMessage){
    this->hasError = true;
    this->errorMessage = errorMessage;
    this->header.size = sizeof(bool) + errorMessage.size();
    this->header.statusCode = ResponseType::QueryResponse;
  }

  QueryResponseProtocol::QueryResponseProtocol(const std::vector<std::string>& columns, std::vector<QueryResult> &rows){
    this->hasError = false;
    this->header.statusCode = ResponseType::QueryResponse;
    this->rows = std::move(rows);
    this->columns = columns;
  }

  int QueryResponseProtocol::GetSize() const{ return ResponseProtocol::GetSize() + header.size; }

  void QueryResponseProtocol::Serialize(){
    this->buffer.clear();

    ResponseProtocol::Serialize();

    // const int size = this->GetSize();

    Vector::AppendToBuffer(this->buffer, &this->hasError, sizeof(bool));

    if (this->hasError) {
      // Vector::AppendToBuffer(this->buffer, &this->hasError, sizeof(bool));
      //
      // const int errorSize = this->errorMessage.size();
      // memcpy(bufferPtr, &errorSize, sizeof(int));
      // bufferPtr += sizeof(int);
      //
      // memcpy(bufferPtr, this->errorMessage.data(), errorSize);
      // bufferPtr += errorSize;

      this->header.size = static_cast<uint16_t>(this->buffer.size());
      return;
    }

    const int numOfTableColumns = static_cast<int>(this->columns.size());
    Vector::AppendToBuffer(this->buffer, &numOfTableColumns, sizeof(int));

    for (const auto& column: this->columns) {
      const int columnSize = static_cast<int>(column.size());
      Vector::AppendToBuffer(this->buffer, &columnSize, sizeof(int));
      Vector::AppendToBuffer(this->buffer, column.data(), columnSize);
    }

    const int numOfRows = static_cast<int>(this->rows.size());
    Vector::AppendToBuffer(this->buffer, &numOfRows, sizeof(int));

    for (const auto& row: this->rows)
      row.Serialize(this->buffer);

    this->header.size = static_cast<uint16_t>(this->buffer.size());
  }

  void QueryResponseProtocol::Deserialize(const vector<char> &buffer) {
    uint32_t offSet = 0;

    memcpy(&this->hasError, buffer.data() + offSet, sizeof(bool));
    offSet += sizeof(bool);

    if (this->hasError) {
      int errorSize = 0;

      memcpy(&errorSize, buffer.data() + offSet, sizeof(int));
      offSet += sizeof(int);

      this->errorMessage.resize(errorSize);
      memcpy(this->errorMessage.data(), buffer.data() + offSet, errorSize);
      offSet += errorSize;

      return;
    }

    int numOfColumns = 0;
    memcpy(&numOfColumns, buffer.data() + offSet, sizeof(int));
    offSet += sizeof(int);

    this->columns.clear();
    this->columns.resize(numOfColumns);

    for (int i = 0;i < numOfColumns; i++) {
      int columnSize = 0;
      memcpy(&columnSize, buffer.data() + offSet, sizeof(int));
      offSet += sizeof(int);

      this->columns[i].resize(columnSize);
      memcpy(this->columns[i].data(), buffer.data() + offSet, columnSize);
      offSet += columnSize;
    }

    int numOfRows = 0;
    memcpy(&numOfRows, buffer.data() + offSet, sizeof(int));
    offSet += sizeof(int);

    this->rows.clear();
    this->rows.reserve(numOfRows);

    for (int i = 0; i < numOfRows; i++) {
      auto row = QueryResult();

      row.Deserialize(buffer, offSet, numOfColumns);
      this->rows.push_back(std::move(row));
    }
  }

  ostream & operator<<(ostream &os, const QueryResponseProtocol &protocol){
      if (protocol.hasError) {
        os << protocol.errorMessage;
        return os;
      }

    for (const auto& column: protocol.columns) {
      os << column << " | ";
    }

    os << std::endl;

    for (const auto& row: protocol.rows) {
      for (const auto & value :  row.GetData())
        os << value << " | ";

      os << std::endl;
    }

    return os;
  }
}