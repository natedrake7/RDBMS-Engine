#pragma once
#include <vector>

namespace Vector {
  static inline void AppendToBuffer(std::vector<char>& buffer, const void* data, const int& size) {
    const auto* bytes = static_cast<const char*>(data);
    buffer.insert(buffer.end(), bytes, bytes + size);
  }
}
