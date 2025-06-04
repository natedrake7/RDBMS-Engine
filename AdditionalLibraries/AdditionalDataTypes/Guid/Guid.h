#pragma once
#include <array>
#include <cstdint>
#include <string>

namespace DataTypes {
  constexpr int GUID_SIZE = 16;

  class Guid {
    std::array<uint8_t, GUID_SIZE> data;

    public:
      Guid();
      Guid(const unsigned char* data, const int& size);
      explicit Guid(const std::array<uint8_t, GUID_SIZE>& data);
      ~Guid();
      [[nodiscard]] int Size() const;
      [[nodiscard]] const std::array<uint8_t, GUID_SIZE>& GetData() const;

      [[nodiscard]] std::string ToString() const;
      static Guid Parse(const std::string& str);
      static bool Validate(const std::string& str);

      friend std::ostream& operator<<(std::ostream& os, const Guid& guid);
      static Guid NewGuid();
  };

  bool operator==(const Guid& guid1, const Guid& guid2);
  bool operator!=(const Guid& guid1, const Guid& guid2);
  bool operator<(const Guid& guid1, const Guid& guid2);
  bool operator>(const Guid& guid1, const Guid& guid2);
  bool operator<=(const Guid& guid1, const Guid& guid2);
  bool operator>=(const Guid& guid1, const Guid& guid2);

}
