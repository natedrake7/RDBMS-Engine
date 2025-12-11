#pragma once
#include <array>
#include <cstdint>
#include <string>

namespace DataTypes {
  constexpr int GUID_SIZE = 16;

  class Guid {
    std::array<uint8_t, GUID_SIZE> data{};

    public:
      Guid();
      Guid(const unsigned char* data, const int& size);
      explicit Guid(const std::array<uint8_t, GUID_SIZE>& data);
      ~Guid();
      [[nodiscard]] int Size() const;
      [[nodiscard]] std::array<uint8_t, GUID_SIZE>& GetDataUnsafe();
      [[nodiscard]] const std::array<uint8_t, GUID_SIZE>& GetData() const;

      [[nodiscard]] std::string ToString() const;
      static Guid Parse(const std::string& str);
      static bool Validate(const std::string& str);

      friend std::ostream& operator<<(std::ostream& os, const Guid& guid);
      static Guid NewGuid();
      static Guid Empty();
      static Guid FromString(const std::string& str);
      constexpr static int GuidSize() { return GUID_SIZE; };
  };

  bool operator==(const Guid& guid1, const Guid& guid2);
  bool operator!=(const Guid& guid1, const Guid& guid2);
  bool operator<(const Guid& guid1, const Guid& guid2);
  bool operator>(const Guid& guid1, const Guid& guid2);
  bool operator<=(const Guid& guid1, const Guid& guid2);
  bool operator>=(const Guid& guid1, const Guid& guid2);
}

template<>
  struct std::hash<DataTypes::Guid> {
    size_t operator()(const DataTypes::Guid& guid) const noexcept {
      size_t result = 0;

      if constexpr (requires { guid.GetData(); }) {
        for (const auto byte : guid.GetData()) {
          result ^= std::hash<uint8_t>{}(byte)
                    + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
        }
      }

      return result;
    }
  };