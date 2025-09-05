#include "ResponseProtocol.h"
#include <cstring>

ResponseProtocol::ResponseProtocol(const ResponseType &statusCode){
  this->header.statusCode = statusCode;
  this->header.size = 0;
}

int ResponseProtocol::GetSize() const{ return  ResponseProtocolHeader::GetSize(); }

void ResponseProtocol::Serialize(){
  this->buffer.resize(this->GetSize());

  char* bufferPtr = this->buffer.data();

  memcpy(bufferPtr, &this->header.size, sizeof(uint16_t));
  bufferPtr += sizeof(uint16_t);

  memcpy(bufferPtr, &this->header.statusCode, sizeof(ResponseType));
  bufferPtr += sizeof(ResponseType);
}

void ResponseProtocol::Deserialize(const vector<char> &buffer){
  const char* bufferPtr = buffer.data();

  memcpy(&this->header.size, bufferPtr, sizeof(uint16_t));
  bufferPtr += sizeof(uint16_t);

  memcpy(&this->header.statusCode, bufferPtr, sizeof(ResponseType));
  bufferPtr += sizeof(ResponseType);

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