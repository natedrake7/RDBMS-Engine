#pragma once
#include <cstdint>
#include "../DataTypes/Guid.h"
#include "../DataStructures/Vector.h"

#include <cstring>
#include <vector>

namespace Network {

  struct ConnectionHeader {
      UnsignedSmallInt size;
      DataTypes::Guid sessionId;

    ConnectionHeader()
      : size(0), sessionId(DataTypes::Guid::Empty()){}

    virtual ~ConnectionHeader() = default;

    virtual void Serialize(std::vector<char>& responseBuffer) {
      Vector::AppendToBuffer(responseBuffer, &this->size, sizeof(UnsignedSmallInt));
      Vector::AppendToBuffer(responseBuffer, sessionId.GetDataUnsafe().data(), DataTypes::Guid::Size());
    }

    virtual void Deserialize(const std::vector<char>& responseBuffer) {
      if (responseBuffer.empty())
        return;

      memcpy(&this->size, responseBuffer.data(), sizeof(UnsignedSmallInt));
      memcpy(this->sessionId.GetDataUnsafe().data(), responseBuffer.data() + sizeof(UnsignedSmallInt), DataTypes::Guid::Size());
    }

    constexpr static int Size(){ return sizeof(UnsignedSmallInt) + DataTypes::Guid::Size(); }
  };
}