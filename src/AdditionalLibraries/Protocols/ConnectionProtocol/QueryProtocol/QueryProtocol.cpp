#include "QueryProtocol.h"
#include <cstring>

QueryProtocol::QueryProtocol(const string &query){
  this->header.dataType = ConnectionProtocolType::Query;
  this->query = query;
  this->header.size = query.size();
}

int QueryProtocol::GetSize() const{ return ConnectionProtocol::GetSize() + this->header.size; }

void QueryProtocol::Serialize(){

  this->buffer.resize(this->GetSize());
  
  ConnectionProtocol::Serialize();

  char* bufferPtr = this->buffer.data() + sizeof(ConnectionProtocolHeader);

  memcpy(bufferPtr, this->query.data(), this->header.size);
  bufferPtr += this->header.size;
}

void QueryProtocol::Deserialize(const vector<char> &buffer){
  const char* bufferPtr = buffer.data();

  query.resize(this->header.size);
  memcpy(query.data(), bufferPtr, this->header.size);
}

const string & QueryProtocol::GetQuery() const { return this->query; }