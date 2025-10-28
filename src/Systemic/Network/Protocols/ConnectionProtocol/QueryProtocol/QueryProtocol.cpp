#include "QueryProtocol.h"
#include <cstring>

namespace Network {
  QueryProtocol::QueryProtocol(const std::string &query){
    this->header.type = ConnectionProtocolType::Query;
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

  void QueryProtocol::Deserialize(const std::vector<char> &buffer){
    const char* bufferPtr = buffer.data();

    query.resize(this->header.size);
    memcpy(query.data(), bufferPtr, this->header.size);
  }

  const std::string & QueryProtocol::GetQuery() const { return this->query; }
}
