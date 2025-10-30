#include "ConnectionProtocol.h"
#include "../../../../DataStructures/Vector/Vector.h"

#include <cstring>

namespace Network {
  void ConnectionProtocolHeader::Serialize(std::vector<char> &responseBuffer){
    responseBuffer.resize(ConnectionProtocolHeader::Size());

    ConnectionHeader::Serialize(responseBuffer);

    Vector::AppendToBuffer(responseBuffer, &this->type, sizeof(ConnectionProtocolType));

    // memcpy(responseBuffer.data() + ConnectionHeader::Size(), &this->type, sizeof(ConnectionProtocolType));
  }

  void ConnectionProtocolHeader::Deserialize(const std::vector<char> &responseBuffer){
    ConnectionHeader::Deserialize(responseBuffer);

    memcpy(&this->type, responseBuffer.data() + ConnectionHeader::Size(), sizeof(ConnectionProtocolType));
  }

  int ConnectionProtocol::GetSize() const{ return Network::ConnectionProtocolHeader::GetSize(); }

  void ConnectionProtocol::Serialize(){
    if (this->buffer.empty()) {
      this->buffer.clear();
      this->header.Serialize(this->buffer);
    }

  }

  void ConnectionProtocol::Deserialize(const std::vector<char> &responseBuffer){
    this->header.Deserialize(responseBuffer);
  }

  const std::vector<char> & ConnectionProtocol::GetSerializedProtocol() {
    if (this->buffer.empty())
      this->Serialize();

    return this->buffer;
  }

}
