#include "QueryResponseProtocol.h"

#include <cstring>
#include <ostream>

namespace Network {
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
    this->message = errorMessage;
    this->header.size = sizeof(bool) + errorMessage.size();
    this->header.statusCode = ResponseType::QueryResponse;
  }

QueryResponseProtocol::QueryResponseProtocol(
    const bool& hasError,
    const std::string& message,
    const std::vector<std::string>& columns,
    std::vector<QueryResult>& rows
  ){
    this->hasError = hasError;
    this->message = message;
    this->header.statusCode = ResponseType::QueryResponse;
    this->rows = std::move(rows);
    this->columns = columns;
  }

  int QueryResponseProtocol::GetSize() const{ return ResponseProtocol::GetSize() + header.size; }

  void QueryResponseProtocol::SerializeMessage(){
    const int errorSize = static_cast<int>(this->message.size());

    Vector::AppendToBuffer(this->buffer, &errorSize, sizeof(int));
    Vector::AppendToBuffer(this->buffer, this->message.c_str(), errorSize);

    this->AssignBufferSizeToProtocolSize();
  }

  void QueryResponseProtocol::SerializeResult(){
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

    this->AssignBufferSizeToProtocolSize();
  }

  void QueryResponseProtocol::AssignBufferSizeToProtocolSize(){
    this->header.size = static_cast<uint16_t>(this->buffer.size() - ResponseProtocolHeader::GetSize());
    std::memcpy(this->buffer.data(), &this->header.size, sizeof(uint16_t));
  }

  void QueryResponseProtocol::Serialize(){
    this->buffer.clear();

    ResponseProtocol::Serialize();
    Vector::AppendToBuffer(this->buffer, &this->hasError, sizeof(bool));

    this->SerializeMessage();
    this->SerializeResult();
  }

  void QueryResponseProtocol::DeserializeMessage(const std::vector<char> &buffer, uint32_t& offSet){
    int errorSize = 0;

    memcpy(&errorSize, buffer.data() + offSet, sizeof(int));
    offSet += sizeof(int);

    this->message.resize(errorSize);
    memcpy(this->message.data(), buffer.data() + offSet, errorSize);
    offSet += errorSize;
  }

  void QueryResponseProtocol::DeserializeResult(const std::vector<char> &buffer, uint32_t& offSet){
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

  void QueryResponseProtocol::Deserialize(const vector<char> &buffer) {
    uint32_t offSet = 0;

    memcpy(&this->hasError, buffer.data() + offSet, sizeof(bool));
    offSet += sizeof(bool);

    this->DeserializeMessage(buffer, offSet);
    this->DeserializeResult(buffer, offSet);
  }

  ostream & operator<<(ostream &os, const QueryResponseProtocol &protocol){
    os << protocol.message << std::endl;

    if (protocol.hasError)
      return os;

    for (const auto& column : protocol.columns)
      os << column << " || ";

    os << std::endl;

    for (const auto& row: protocol.rows)
      row.Print();

    return os;
  }
}