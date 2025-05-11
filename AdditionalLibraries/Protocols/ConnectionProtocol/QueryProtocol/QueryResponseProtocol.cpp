#include "QueryResponseProtocol.h"

#include <cstring>
#include <ostream>

ResponseRow::ResponseRow(const vector<string> &columns, const ByteMaps::BitMap& nullBitMap){
  this->columns = columns;
  this->nullBitMap = nullBitMap;
}

int ResponseRow::GetSize() const{
  int size = this->nullBitMap.GetSizeInBytes();
  
  for (const auto& column: columns)
    size += sizeof(int) + column.size();

  return size;
}

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

QueryResponseProtocol::QueryResponseProtocol(const ResponseType &statusCode) : ResponseProtocol(statusCode) {
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

QueryResponseProtocol::QueryResponseProtocol(const vector<string>& columns, const vector<ResponseRow> &rows){
  this->hasError = false;

  int size = sizeof(bool) + sizeof(int);
  
  for (const auto& row: rows)
    size += row.GetSize();

  size += sizeof(int);
  for (const auto& column: columns)
    size += sizeof(int) + column.size();

  this->header.size = size;
  this->header.statusCode = ResponseType::QueryResponse;
  this->rows = rows;
  this->columns = columns;
}

int QueryResponseProtocol::GetSize() const{ return ResponseProtocol::GetSize() + header.size; }

void QueryResponseProtocol::Serialize(){
  ResponseProtocol::Serialize();

  const int size = this->GetSize();
  this->buffer.resize(size);

  char* bufferPtr = this->buffer.data() + ResponseProtocol::GetSize();

  memcpy(bufferPtr, &this->hasError, sizeof(bool));
  bufferPtr += sizeof(bool);
  
  if (this->hasError) {
    const int errorSize = this->errorMessage.size();
    memcpy(bufferPtr, &errorSize, sizeof(int));
    bufferPtr += sizeof(int);

    memcpy(bufferPtr, this->errorMessage.data(), errorSize);
    bufferPtr += errorSize;

    return;
  }

  const int numOfTableColumns = this->columns.size();
  memcpy(bufferPtr, &numOfTableColumns, sizeof(int));
  bufferPtr += sizeof(int);

  for (const auto& column: this->columns) {
    const int columnSize = column.size();
    memcpy(bufferPtr, &columnSize, sizeof(int));
    bufferPtr += sizeof(int);
    
    memcpy(bufferPtr, column.data(), columnSize);
    bufferPtr += columnSize;
  }

  const int numOfRows = this->rows.size();

  memcpy(bufferPtr, &numOfRows, sizeof(int));
  bufferPtr += sizeof(int);

  for (auto& row: this->rows) {
    // auto& bitMapData = row.nullBitMap.GetDataUnsafe();
    
    // int bitMapSize = bitMapData.size();
    // memcpy(bufferPtr, &bitMapSize, sizeof(int));
    // bufferPtr += sizeof(int);
    //
    // memcpy(bufferPtr, bitMapData.data(), bitMapSize);
    // bufferPtr += bitMapSize;

    // const auto bitMapSize = row.nullBitMap.GetSize();
    //
    // memcpy(bufferPtr, &bitMapSize, sizeof(Constants::bit_map_size_t));
    // bufferPtr += sizeof(Constants::bit_map_size_t);
    //
    // const int dataSize = bitMapData.size() * sizeof(Constants::byte);
    //     
    // memcpy(bufferPtr, bitMapData.data(), dataSize);
    // bufferPtr += dataSize;
    
    for (const auto& column: row.columns) {
      const int columnSize = column.size();
      
      memcpy(bufferPtr, &columnSize, sizeof(int));
      bufferPtr += sizeof(int);

      memcpy(bufferPtr, column.data(), columnSize);
      bufferPtr += columnSize;
    }
  }
}

void QueryResponseProtocol::Deserialize(const vector<char> &buffer) {
  const char* bufferPtr = buffer.data();

  memcpy(&this->hasError, bufferPtr, sizeof(bool));
  bufferPtr += sizeof(bool);

  if (this->hasError) {
    int errorSize = 0;
    
    memcpy(&errorSize, bufferPtr, sizeof(int));
    bufferPtr += sizeof(int);

    this->errorMessage.resize(errorSize);
    memcpy(this->errorMessage.data(), bufferPtr, errorSize);
    bufferPtr += errorSize;

    return;
  }

  int numOfColumns = 0;
  memcpy(&numOfColumns, bufferPtr, sizeof(int));
  bufferPtr += sizeof(int);

  this->columns.resize(numOfColumns);

  for (int i = 0;i < numOfColumns; i++) {
    int columnSize = 0;
    memcpy(&columnSize, bufferPtr, sizeof(int));
    bufferPtr += sizeof(int);

    this->columns[i].resize(columnSize);
    memcpy(this->columns[i].data(), bufferPtr, columnSize);
    bufferPtr += columnSize;
  }

  int numOfRows = 0;
  memcpy(&numOfRows, bufferPtr, sizeof(int));
  bufferPtr += sizeof(int);

  this->rows.resize(numOfRows);

  for (int i = 0; i < numOfRows; i++) {
    this->rows[i].columns.resize(numOfColumns);

    // auto& bitMapSize = this->rows[i].nullBitMap.GetSizeUnsafe();
    //
    // memcpy(&bitMapSize, bufferPtr, sizeof(Constants::bit_map_size_t));
    // bufferPtr += sizeof(Constants::bit_map_size_t);
    //     
    // const Constants::bit_map_size_t &bytesToRead = (bitMapSize + 7) / 8;
    //
    // for (Constants::bit_map_size_t bitMapBytes = 0; i < bytesToRead; i++)
    // {
    //   Constants::byte value;
    //   memcpy(&value, bufferPtr, sizeof(Constants::byte));
    //   this->rows[i].nullBitMap.SetByte(bitMapBytes, value);
    //
    //   bufferPtr += sizeof(Constants::byte);
    // }

    // int bitMapSize = 0;
    // memcpy(&bitMapSize, bufferPtr, sizeof(int));
    // bufferPtr += sizeof(int);
    //
    // auto& bitMapData = this->rows[i].nullBitMap.GetDataUnsafe();
    // bitMapData.resize(bitMapSize);
    //
    // memcpy(bitMapData.data(), bufferPtr, bitMapSize);
    // bufferPtr += bitMapSize;

    for (int j = 0; j < numOfColumns; j++) {
      int columnSize = 0;
      memcpy(&columnSize, bufferPtr, sizeof(int));
      bufferPtr += sizeof(int);

      this->rows[i].columns[j].resize(columnSize);
      
      memcpy(this->rows[i].columns[j].data(), bufferPtr, columnSize);
      bufferPtr += columnSize;
    }
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

  os << endl;

  for (const auto& row: protocol.rows) {
    for (int i = 0; i< row.columns.size(); i++) {
      // if (row.nullBitMap.Get(i)) {
      //   os << "NULL" << " | ";
      //   continue;
      // }

      os << row.columns[i] << " | ";
    }
    
    os << endl;
  }

  return os;
}