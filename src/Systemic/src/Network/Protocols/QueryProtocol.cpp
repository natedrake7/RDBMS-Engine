#include "../../../include/Network/QueryProtocol.h"
#include <cstring>

namespace Network {
  QueryProtocol::QueryProtocol(const std::string &query, const DataTypes::Guid& sessionId){
    this->header.sessionId = sessionId;
    this->header.type = ConnectionProtocolType::Query;
    this->header.size = query.size();
    this->query = query;
  }

  int QueryProtocol::GetSize() const{ return ConnectionProtocol::GetSize() + this->header.size; }

  void QueryProtocol::Serialize(){
    ConnectionProtocol::Serialize();
    Vector::AppendToBuffer(this->buffer, this->query.data(), this->header.size);
    // char* bufferPtr = this->buffer.data() + sizeof(ConnectionProtocolHeader);
    //
    // memcpy(bufferPtr, this->query.data(), this->header.size);
    // bufferPtr += this->header.size;
  }

  void QueryProtocol::Deserialize(const std::vector<char> &buffer){
    const char* bufferPtr = buffer.data();

    query.resize(this->header.size);
    memcpy(query.data(), bufferPtr, this->header.size);
  }

  const std::string & QueryProtocol::GetQuery() const { return this->query; }
}
