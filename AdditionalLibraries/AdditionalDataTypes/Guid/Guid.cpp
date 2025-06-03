#include "Guid.h"

#include <array>
#include <cstring>
#include <iomanip>
#include <random>
#include <sstream>

namespace DataTypes {
  Guid::Guid(){
    this->data = Guid::NewGuid();
 }

  Guid::Guid(const unsigned char *data, const int &size){
    memcpy(this->data.data(), data, size);
  }

  Guid::~Guid() = default;

  int Guid::Size() const{ return static_cast<int>(this->data.size()); }

  const std::array<uint8_t, 16>& Guid::GetData() const{ return this->data; }std::string Guid::ToString() const{
    std::ostringstream oss;

    for (size_t i = 0; i < this->data.size(); ++i) {
      // Insert dashes at GUID positions (after bytes 4, 6, 8, 10)
      if (i == 4 || i == 6 || i == 8 || i == 10)
        oss << '-';

      oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(this->data[i]);
    }

    return oss.str();
  }

  bool operator==(const Guid &guid1, const Guid &guid2) { return guid1.GetData() == guid2.GetData(); }

  bool operator!=(const Guid &guid1, const Guid &guid2){ return !(guid1 == guid2); }

  bool operator<(const Guid &guid1, const Guid &guid2) { return guid1.GetData() < guid2.GetData(); }

  bool operator>(const Guid &guid1, const Guid &guid2){ return guid2 < guid1; }

  bool operator<=(const Guid &guid1, const Guid &guid2){ return guid2 >= guid1; }

  bool operator>=(const Guid &guid1, const Guid &guid2){ return !(guid1 < guid2); }

  std::ostream & operator<<(std::ostream &os, const Guid &guid){
    os << guid.ToString();

    return os;
  }

  std::array<uint8_t, 16> Guid::NewGuid(){
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(0, 255);

    std::array<uint8_t, 16> data {};

    for (auto& byte : data)
      byte = static_cast<uint8_t>(dist(gen));

    // Set UUID version to 4 (random)
    data[6] = (data[6] & 0x0F) | 0x40;

    // Set the variant to 10xxxxxx (RFC 4122)
    data[8] = (data[8] & 0x3F) | 0x80;

    return data;
  }

}