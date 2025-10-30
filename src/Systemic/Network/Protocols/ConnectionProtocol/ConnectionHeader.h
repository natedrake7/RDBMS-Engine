#pragma once
#include "../../../DataStructures/Vector/Vector.h"


#include <cstdint>
#include "../../../DataTypes/Guid/Guid.h"

#include <cstring>
#include <vector>

namespace Network {

  struct ConnectionHeader {
      uint16_t size;
      DataTypes::Guid sessionId;

    ConnectionHeader()
      : size(0), sessionId(DataTypes::Guid::Empty()){}

    virtual ~ConnectionHeader() = default;

    virtual void Serialize(std::vector<char>& responseBuffer) {
      Vector::AppendToBuffer(responseBuffer, &this->size, sizeof(uint16_t));
      Vector::AppendToBuffer(responseBuffer, sessionId.GetDataUnsafe().data(), DataTypes::Guid::GuidSize());
    }

    virtual void Deserialize(const std::vector<char>& responseBuffer) {
      if (responseBuffer.empty())
        return;

      memcpy(&this->size, responseBuffer.data(), sizeof(uint16_t));
      memcpy(this->sessionId.GetDataUnsafe().data(), responseBuffer.data() + sizeof(uint16_t), DataTypes::Guid::GuidSize());
    }

    constexpr static int Size(){ return sizeof(uint16_t) + DataTypes::Guid::GuidSize(); }
  };
}