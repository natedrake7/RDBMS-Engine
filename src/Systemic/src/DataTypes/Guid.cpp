#include "../../include/DataTypes/Guid.h"

#include <array>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <random>
#include <regex>
#include <sstream>

namespace DataTypes {
  Guid::Guid() {
    this->data = std::array<uint8_t, GUID_SIZE>{0};
  }

  Guid::Guid(const unsigned char *data, const Int size){
    std::memcpy(this->data.data(), data, size);
  }

  Guid::Guid(const std::array<uint8_t, 16> &data) : data(data){}

  Guid::~Guid() = default;

  std::array<uint8_t, GUID_SIZE>& Guid::GetDataUnsafe(){ return this->data; }

  const std::array<uint8_t, GUID_SIZE> & Guid::GetData() const{ return this->data; }

  std::string Guid::ToString() const{
    std::ostringstream oss;

    for (int i = 0; i < GUID_SIZE; i++) {
      // Insert dashes at GUID positions (after bytes 4, 6, 8, 10)
      if (i == 4 || i == 6 || i == 8 || i == 10)
        oss << '-';

      oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(this->data[i]);
    }

    return oss.str();
  }

  Guid Guid::Parse(const std::string &str){
    if (!Guid::Validate(str))
      return {};

    std::string hex;
    hex.reserve(32);

    for (const char& c : str) {
      if (c == '-')
        continue;

      hex += c;
    }

    std::array <uint8_t, GUID_SIZE> data{};

    for (size_t i = 0; i < GUID_SIZE; i++) {
      std::string byteStr = hex.substr(i * 2, 2);
      data[i] = static_cast<uint8_t>(std::stoul(byteStr, nullptr, 16));
    }

    return Guid(data);
  }

  bool Guid::Validate(const std::string& str){
    static std::regex pattern("^[{]?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}[}]?$");

    if (str.empty()) {
      std::cerr << "Expected Guid but got empty string instead" << std::endl;
      return false;
    }

    if(!regex_match(str, pattern)) {
      std::cerr << "Invalid Guid specified" << std::endl;
      return false;
    }

    return true;
  }

  bool operator==(const Guid &guid1, const Guid &guid2) { return memcmp(guid1.GetData().data(), guid2.GetData().data(), GUID_SIZE) == 0; }

  bool operator!=(const Guid &guid1, const Guid &guid2){ return !(guid1 == guid2); }

  bool operator<(const Guid &guid1, const Guid &guid2) { return memcmp(guid1.GetData().data(), guid2.GetData().data(), GUID_SIZE) < 0; }

  bool operator>(const Guid &guid1, const Guid &guid2){ return guid2 < guid1; }

  bool operator<=(const Guid &guid1, const Guid &guid2){ return guid2 >= guid1; }

  bool operator>=(const Guid &guid1, const Guid &guid2){ return !(guid1 < guid2); }

  std::ostream & operator<<(std::ostream &os, const Guid &guid){
    os << guid.ToString();
    return os;
  }

  Guid Guid::NewGuid(){
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(0, 255);

    std::array<uint8_t, GUID_SIZE> data {};

    for (auto& byte : data)
      byte = static_cast<uint8_t>(dist(gen));

    // Set UUID version to 4 (random)
    data[6] = (data[6] & 0x0F) | 0x40;

    // Set the variant to 10xxxxxx (RFC 4122)
    data[8] = (data[8] & 0x3F) | 0x80;

    return Guid(data);
  }

  Guid Guid::Empty() {
    static Guid Empty;

    return Empty;
  }


  Guid Guid::FromString(const std::string &str){
    std::string hex_str;
    hex_str.reserve(32);
    for (const char& c : str) {
        if (!std::isxdigit(c))
          continue;

        hex_str += c;
    }

    std::array<uint8_t, GUID_SIZE> data{};
    for (size_t i = 0; i < GUID_SIZE; ++i) {
      std::string byte_str = hex_str.substr(i * 2, 2);
      data[i] = static_cast<uint8_t>(std::stoul(byte_str, nullptr, 16));
    }

    return Guid(data);
  }

  long double Guid::Interpolate() const{
    uint64_t result = 0;

    for (int i = 0; i < 8; i++)
      result = (result << 8) | static_cast<uint64_t>(this->data[i]);

    return static_cast<long double>(result);
  }
}
