#pragma once
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
      if (responseBuffer.empty())
        responseBuffer.resize(ConnectionHeader::Size());

      memcpy(responseBuffer.data(), &size, sizeof(uint16_t));

      memcpy(responseBuffer.data() + sizeof(uint16_t), sessionId.GetDataUnsafe().data(), DataTypes::Guid::GuidSize());
    }

    virtual void Deserialize(const std::vector<char>& responseBuffer) {
      if (responseBuffer.empty())
        return;

      memcpy(&size, responseBuffer.data(), sizeof(uint16_t));
      memcpy(sessionId.GetDataUnsafe().data(), responseBuffer.data() + sizeof(uint16_t), DataTypes::Guid::GuidSize());
    }

    constexpr static int Size(){ return sizeof(uint16_t) + DataTypes::Guid::GuidSize(); }
  };
}