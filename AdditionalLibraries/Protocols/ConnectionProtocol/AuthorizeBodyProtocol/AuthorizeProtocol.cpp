#include "AuthorizeProtocol.h"

#include <cstring>

 AuthorizeProtocol::AuthorizeProtocol(const string &username, const string &password) : username(username), password(password) {
  this->header.size = this->GetSize();
  this->header.dataType = ConnectionProtocolType::Authorize; 
}

AuthorizeProtocol::AuthorizeProtocol(const vector<unsigned char> &buffer){
  this->Deserialize(buffer);
}

int AuthorizeProtocol::GetSize() const{
  return ConnectionProtocol::GetSize() + this->username.size() + this->password.size();
}

void AuthorizeProtocol::Serialize(){
  buffer.resize(this->GetSize());

  ConnectionProtocol::Serialize();

  unsigned char *bufferPtr = this->buffer.data() + sizeof(ConnectionProtocolHeader);
  
  const int usernameSize = this->username.size();

  memcpy(bufferPtr, &usernameSize, sizeof(int));
  bufferPtr += sizeof(int);

  memcpy(bufferPtr, this->username.c_str(), usernameSize);
  bufferPtr += usernameSize;
  
  const int passwordSize = this->password.size();

  memcpy(bufferPtr, &passwordSize, sizeof(int));
  bufferPtr += sizeof(int);

  memcpy(bufferPtr, this->password.c_str(), passwordSize);
}

void AuthorizeProtocol::Deserialize(const vector<unsigned char> &buffer){
  ConnectionProtocol::Deserialize(buffer);
  this->DeserializeBody(buffer);
}

void AuthorizeProtocol::DeserializeBody(const vector<unsigned char> &buffer){
  const unsigned char* bufferPtr = buffer.data() + sizeof(ConnectionProtocolHeader);
      
  int usernameSize = 0, passwordSize = 0;
    
  memcpy(&usernameSize, bufferPtr, sizeof(int));
  bufferPtr += sizeof(int);

  this->username.resize(usernameSize);
  memcpy(this->username.data(), bufferPtr, usernameSize);
  bufferPtr += usernameSize;

  memcpy(&passwordSize, bufferPtr, sizeof(int));
  bufferPtr += sizeof(int);
    
  this->password.resize(passwordSize);
  memcpy(this->password.data(), bufferPtr, passwordSize);
}

const vector<unsigned char> & AuthorizeProtocol::GetSerializedProtocol(){
  if (this->buffer.empty())
    this->Serialize();
  
  return this->buffer;
}

const string & AuthorizeProtocol::GetUsername() const{ return this->username; }

const string & AuthorizeProtocol::GetPassword() const{ return this->password; }


