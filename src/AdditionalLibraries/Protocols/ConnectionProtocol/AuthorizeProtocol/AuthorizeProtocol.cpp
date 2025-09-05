#include "AuthorizeProtocol.h"

#include <cstring>

 AuthorizeProtocol::AuthorizeProtocol(const string &username, const string &password) : username(username), password(password) {
  this->header.size = this->GetSize();
  this->header.dataType = ConnectionProtocolType::Authorize; 
}

AuthorizeProtocol::AuthorizeProtocol(const vector<char> &buffer){
  this->Deserialize(buffer);
}

int AuthorizeProtocol::GetSize() const{
  return ConnectionProtocol::GetSize() + 2 * sizeof(int) + this->username.size() + this->password.size();
}

void AuthorizeProtocol::Serialize(){
  buffer.resize(this->GetSize());

  ConnectionProtocol::Serialize();

  char *bufferPtr = this->buffer.data() + sizeof(ConnectionProtocolHeader);
  
  const int usernameSize = this->username.size();

  memcpy(bufferPtr, &usernameSize, sizeof(int));
  bufferPtr += sizeof(int);

  memcpy(bufferPtr, this->username.c_str(), usernameSize);
  bufferPtr += usernameSize;
  
  const int passwordSize = this->password.size();

  memcpy(bufferPtr, &passwordSize, sizeof(int));
  bufferPtr += sizeof(int);

  memcpy(bufferPtr, this->password.c_str(), passwordSize);
  bufferPtr += passwordSize;
}

void AuthorizeProtocol::Deserialize(const vector<char> &buffer){
   const char* bufferPtr = buffer.data();
      
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

const string & AuthorizeProtocol::GetUsername() const{ return this->username; }

const string & AuthorizeProtocol::GetPassword() const{ return this->password; }


