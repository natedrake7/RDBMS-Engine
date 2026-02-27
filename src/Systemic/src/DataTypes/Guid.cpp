#include "../../include/DataTypes/Guid.h"

#include <array>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <random>
#include <regex>

#include "DataTypes/StringView.h"

namespace DataTypes {
    bool Guid::Validate(const char* str, const Int size){
        static std::regex pattern(GUID_VALIDATION_FORMAT.Data());

        if (size == 0) {
            std::cerr << "Expected Guid but got empty string instead" << std::endl;
            return false;
        }

        if(!regex_match(str, pattern)) {
            std::cerr << "Invalid Guid specified" << std::endl;
            return false;
        }

        return true;
    }

    Guid Guid::Parse(const char* str, const Int size){
        if (!Guid::Validate(str, size)) return Guid();

        char buffer[GUID_STRING_NO_HYPHEN_SIZE];
        Int bufferPos = 0;
        for (int i = 0; i < size; i++){
            if (str[i] == '-') continue;
            buffer[bufferPos++] = str[i];
        }

        std::array<UnsignedTinyInt, GUID_SIZE> data{};
        for (auto i = 0; i < GUID_SIZE; i++) {
            const char byteStr[3] = { buffer[i * 2], buffer[i * 2 + 1], '\0' };
            data[i] = static_cast<UnsignedTinyInt>(std::strtoul(byteStr, nullptr, 16));
        }

        return Guid(data);
    }

    Guid::Guid() {
        this->_data = {};
    }

    Guid::Guid(const unsigned char *data, const Int size){
        std::memcpy(this->_data.data(), data, size);
    }

    Guid::Guid(const std::array<UnsignedTinyInt, 16> &data) : _data(data){}

    Guid::~Guid() = default;

    std::array<UnsignedTinyInt, GUID_SIZE>& Guid::GetDataUnsafe(){ return this->_data; }

    const std::array<UnsignedTinyInt, GUID_SIZE> & Guid::GetData() const{ return this->_data; }

    String Guid::ToString(const ::Memory::IAllocator* allocator) const {
        char buffer[GUID_STRING_SIZE] = {};
        std::snprintf(buffer, sizeof(buffer) + 1,
            GUID_STRING_FORMAT.Data(),
                this->_data[0],  this->_data[1],  this->_data[2],  this->_data[3],
                this->_data[4],  this->_data[5],
                this->_data[6],  this->_data[7],
                this->_data[8],  this->_data[9],
                this->_data[10], this->_data[11], this->_data[12],
                this->_data[13], this->_data[14], this->_data[15]
        );

        return String(buffer, GUID_STRING_SIZE, allocator);
    }

    Guid Guid::Parse(const String& str){ return Guid::Parse(str.Data(), str.Size());}
    Guid Guid::Parse(const StringView& str){ return Guid::Parse(str.Data(), str.Size());}
    Guid Guid::Parse(const std::string& str){ return Guid::Parse(str.c_str(), static_cast<Int>(str.size()));}
    Guid Guid::Parse(const std::string_view& str) { return Guid::Parse(str.data(), static_cast<Int>(str.size())); }
    Guid Guid::Parse(const char* str) { return Guid::Parse(str, static_cast<Int>(std::strlen(str)));}

    bool Guid::Validate(const String& str){ return Guid::Validate(str.Data(), str.Size()); }
    bool Guid::Validate(const StringView& str){ return Guid::Validate(str.Data(), str.Size()); }
    bool Guid::Validate(const std::string& str){ return Guid::Validate(str.c_str(), static_cast<Int>(str.size()));}
    bool Guid::Validate(const std::string_view& str) { return Guid::Validate(str.data(), static_cast<Int>(str.size())); }
    bool Guid::Validate(const char* str) { return Guid::Validate(str, static_cast<Int>(std::strlen(str)));}

    bool operator==(const Guid &guid1, const Guid &guid2) { return std::memcmp(guid1.GetData().data(), guid2.GetData().data(), GUID_SIZE) == 0; }

    bool operator!=(const Guid &guid1, const Guid &guid2){ return !(guid1 == guid2); }

    bool operator<(const Guid &guid1, const Guid &guid2) { return std::memcmp(guid1.GetData().data(), guid2.GetData().data(), GUID_SIZE) < 0; }

    bool operator>(const Guid &guid1, const Guid &guid2){ return guid2 < guid1; }

    bool operator<=(const Guid &guid1, const Guid &guid2){ return guid2 >= guid1; }

    bool operator>=(const Guid &guid1, const Guid &guid2){ return !(guid1 < guid2); }

    std::ostream & operator<<(std::ostream &os, const Guid &guid){
        // os << guid.ToString();
        return os;
    }

    Guid Guid::NewGuid(){
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dist(0, 255);

        std::array<UnsignedTinyInt, GUID_SIZE> data{};

        for (auto& byte : data)
        byte = static_cast<UnsignedTinyInt>(dist(gen));

        // Set UUID version to 4 (random)
        data[6] = (data[6] & 0x0F) | 0x40;

        // Set the variant to 10xxxxxx (RFC 4122)
        data[8] = (data[8] & 0x3F) | 0x80;

        return Guid(data);
    }

    Guid Guid::Empty() { return Guid(); }

    long double Guid::Interpolate() const{
        uint64_t result = 0;
        for (int i = 0; i < 8; i++)
            result = (result << 8) | static_cast<uint64_t>(this->_data[i]);
        return static_cast<long double>(result);
    }
}
