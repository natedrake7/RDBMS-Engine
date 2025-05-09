#pragma once
#include "ConnectionProtocol.h"
#include <cstring>


int ConnectionProtocol::GetSize() const{ return sizeof(ConnectionProtocolHeader); }

void ConnectionProtocol::Serialize(){
  if (buffer.empty()) {
    this->buffer.clear();
    this->buffer.resize(sizeof(ConnectionProtocolHeader));
  }
  
  unsigned char* bufferPtr = this->buffer.data();

  memcpy(bufferPtr, &this->header, sizeof(ConnectionProtocolHeader));
}

void ConnectionProtocol::Deserialize(const vector<unsigned char> &buffer){
  const unsigned char* bufferPtr = buffer.data();

  memcpy(&this->header, bufferPtr, sizeof(ConnectionProtocolHeader));
}

void ConnectionProtocol::DeserializeBody(const vector<unsigned char> &buffer) { }

const vector<unsigned char> & ConnectionProtocol::GetSerializedProtocol() {
  if (this->buffer.empty())
    this->Serialize();

  return this->buffer;
} 
