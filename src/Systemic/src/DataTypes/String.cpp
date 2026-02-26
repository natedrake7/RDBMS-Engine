#include "../../include/DataTypes/String.h"

#include <cstring>
#include <ostream>

#include "Memory/IAllocator.h"

namespace DataTypes{
    bool String::EqualsIgnoreCase(const String& other) const{
        if (this->size != other.size)
            return false;

        for (Int i = 0; i < this->size; i++) {
            if (std::tolower(this->_data[i]) != std::tolower(other._data[i]))
                return false;
        }

        return true;
    }

    bool String::StartsWith(const String& other) const{
        if (this->size < other.size)
            return false;

        return std::memcmp(this->_data, other._data, other.size) == 0;
    }

    bool String::StartsWithIgnoreCase(const String& other) const{
        if (this->size < other.size)
            return false;

        for (int i = 0; i < other.size; i++){
            if (std::tolower(this->_data[i]) != std::tolower(other._data[i]))
                return false;
        }

        return true;
    }

    bool String::EndsWith(const String& other) const{
        if (this->size < other.size)
            return false;

        return std::memcmp(this->_data + this->size - other.size, other._data, other.size) == 0;
    }

    bool String::EndsWithIgnoreCase(const String& other) const{
        if (this->size < other.size)
            return false;

        for (int i = 0; i < other.size; i++){
            if (std::tolower(this->_data[this->size - other.size + i]) != std::tolower(other._data[i]))
                return false;
        }

        return true;
    }

    bool String::Contains(const String& other) const {
        if (other.size == 0)
            return true;

        if (this->size < other.size)
            return false;

        for (Int i = 0; i <= this->size - other.size; i++) {
            if (std::memcmp(this->_data + i, other._data, other.size) == 0)
                return true;
        }

        return false;
    }

    bool String::ContainsIgnoreCase(const String& other) const {
        if (other.size == 0)
            return true;

        if (this->size < other.size)
            return false;

        for (Int i = 0; i <= this->size - other.size; i++) {
            bool match = true;
            for (Int j = 0; j < other.size; j++) {
                if (std::tolower(this->_data[i + j]) != std::tolower(other._data[j])) {
                    match = false;
                    break;
                }
            }
            if (match)
                return true;
        }

        return false;
    }


    String::String(const object_t* data, const Int size){
        this->_data = data;
        this->size = size;
    }

    String::String(const String& other) = default;

    String::String(String&& other) noexcept
        : _data(other._data), size(other.size){
        other._data = nullptr;
    }

    String::String(const char* other){
        this->size = std::strlen(other);
        this->_data = reinterpret_cast<const object_t*>(other);
    }

    String& String::operator=(const char* other){
        this->size = std::strlen(other);
        this->_data = reinterpret_cast<const object_t*>(other);
        return *this;
    }

    String& String::operator=(String&& other) noexcept{
        if (this == &other)
            return *this;

        this->_data = other._data;
        this->size = other.size;
        other._data = nullptr;
        return *this;
    }

    String& String::operator=(const String& other){
        if (this == &other)
            return *this;

        this->_data = other._data;
        this->size = other.size;
        return *this;
    }

    String::~String() = default;

    const object_t* String::Data() const{ return this->_data; }

    const char* String::GetDataAsChar() const{ return reinterpret_cast<const char*>(this->_data); }

    std::ostream& operator<<(std::ostream& os, const String& sv){
        os.write(reinterpret_cast<const char*>(sv._data), sv.size);
        return os;
    }

    char String::operator[](const Int index) const{ return  this->_data[index];}

    bool String::operator==(const String& other) const{
        return this->size == other.size
            && std::memcmp(this->_data, other._data, this->size) == 0;
    }

    bool String::operator!=(const String& other) const{
        return !(*this == other);
    }

    String::operator std::string_view() const{
        return std::string_view(GetDataAsChar(), size);
    }

    String::operator std::span<const char>() const{
        return std::span(GetDataAsChar(), size);
    }

    String String::Concat(const String& other, const Memory::IAllocator* allocator) const{
        const Int newSize = this->size + other.size;
        auto* newString = static_cast<object_t*>(allocator->AllocateRaw(newSize));

        std::memcpy(newString, this->_data, this->size);
        std::memcpy(newString + this->size, other._data, other.size);

        return String(newString, newSize);
    }

    String String::Substring(const Int startIndex, const Int length) const{
        if (startIndex < 0 || startIndex >= this->size)
            throw std::out_of_range("Index out of range.");

        if (length < 0 || startIndex + length > this->size)
            throw std::out_of_range("Length out of range.");

        return String(this->_data + startIndex, length);
    }

    Int String::Size() const{
        return this->size;
    }

    Int String::IndexOf(const char c) const{
        for (Int i = 0; i < this->size; i++)
            if (this->_data[i] == c)
                return i;
        return -1;
    }

    bool String::Contains(const String& other, const StringComparisonType type) const{
        switch (type) {
        case StringComparisonType::Equals:
            return *this == other;
        case StringComparisonType::EqualsIgnoreOrdinalCase:
            return this->EqualsIgnoreCase(other);
        case StringComparisonType::StartsWith:
            return this->StartsWith(other);
        case StringComparisonType::StartsWithIgnoreOrdinalCase:
            return this->StartsWithIgnoreCase(other);
        case StringComparisonType::EndsWith:
            return this->EndsWith(other);
        case StringComparisonType::EndsWithIgnoreOrdinalCase:
            return this->EndsWithIgnoreCase(other);
        case StringComparisonType::Contains:
            return this->Contains(other);
        case StringComparisonType::ContainsIgnoreCase:
            return this->ContainsIgnoreCase(other);
        default:
            throw std::invalid_argument("Invalid StringComparisonType.");
        }
    }

    bool String::Empty() const{ return this->size == 0; }

    String::const_iterator String::begin() const{
        return this->_data;
    }

    String::const_iterator String::end() const{
        return this->_data + this->size;
    }
}
