#pragma once
#include <array>
#include <cstdint>
#include <string>

namespace DataTypes {
  class Guid {
    std::array<uint8_t, 16> data;

    public:
      Guid();
      Guid(const unsigned char* data, const int& size);
      ~Guid();
      [[nodiscard]] int Size() const;
      [[nodiscard]] const std::array<uint8_t, 16>& GetData() const;

      [[nodiscard]] std::string ToString() const;

      friend std::ostream& operator<<(std::ostream& os, const Guid& guid);
      static std::array<uint8_t, 16> NewGuid();
  };

  bool operator==(const Guid& guid1, const Guid& guid2);
  bool operator!=(const Guid& guid1, const Guid& guid2);
  bool operator<(const Guid& guid1, const Guid& guid2);
  bool operator>(const Guid& guid1, const Guid& guid2);
  bool operator<=(const Guid& guid1, const Guid& guid2);
  bool operator>=(const Guid& guid1, const Guid& guid2);

}
