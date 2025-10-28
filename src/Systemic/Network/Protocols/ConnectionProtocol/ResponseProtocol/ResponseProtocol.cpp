#include "ResponseProtocol.h"
#include <cstring>

namespace Network {
  ResponseProtocol::ResponseProtocol(const ResponseType& statusCode, const DataTypes::Guid& sessionId){
    this->header.statusCode = statusCode;
    this->header.sessionId = sessionId;
    this->header.size = 0;
  }

  void ResponseProtocolHeader::Serialize(std::vector<char> &data){
      data.resize(ResponseProtocolHeader::GetSize());

      ConnectionHeader::Serialize(data);


    memcpy(data.data() + ConnectionHeader::Size(), &this->statusCode, sizeof(ResponseType));
  }

  void ResponseProtocolHeader::Deserialize(const std::vector<char> &data){
    ConnectionHeader::Deserialize(data);

    memcpy(&this->statusCode, data.data() + ConnectionHeader::Size(), sizeof(ResponseType));
  }

  int ResponseProtocol::GetSize() const{ return  ResponseProtocolHeader::GetSize(); }

  void ResponseProtocol::Serialize(){
    this->header.Serialize(this->buffer);
  }

  void ResponseProtocol::Deserialize(const vector<char> &responseBuffer){
    const char* bufferPtr = responseBuffer.data();

    this->header.Deserialize(responseBuffer);

    bufferPtr += ResponseProtocolHeader::GetSize();

    int messageSize = 0;
    memcpy(&messageSize, bufferPtr, sizeof(int));
    bufferPtr += sizeof(int);
  }

  void ResponseProtocol::DeserializeBody(const vector<char> &buffer){}

  const vector<char> & ResponseProtocol::GetSerializedProtocol(){
    if (this->buffer.empty())
      this->Serialize();

    return this->buffer;
  }

  const ResponseType & ResponseProtocol::GetResponseType() const{ return this->header.statusCode; }
}
