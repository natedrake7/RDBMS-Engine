#include "../../include/DataTypes/String.h"

#include <cstring>
#include <ostream>

#include "Memory/IAllocator.h"

namespace DataTypes{
    void String::CalculateCapacity(const Int size){
        if (this->_capacity == 0)
            this->_capacity = 1;

        while (this->_capacity < size)
            this->_capacity *= 2;
    }

    bool String::CanFit(const Int size) const{
        return this->_capacity >= size;
    }

    bool String::Equals(const char* other, const Int size) const{
        return this->_size == size
            && std::memcmp(this->_data, other, size) == 0;
    }

    bool String::EqualsIgnoreCase(const char* other, const Int size) const{
        if (this->_size != size)
            return false;

        for (Int i = 0; i < this->_size; i++) {
            if (std::tolower(this->_data[i]) != std::tolower(other[i]))
                return false;
        }

        return true;
    }

    bool String::StartsWith(const char* other, const Int size) const{
        if (this->_size < size)
            return false;

        return std::memcmp(this->_data, other, size) == 0;
    }

    bool String::StartsWithIgnoreCase(const char* other, const Int size) const{
        if (this->_size < size)
            return false;

        for (int i = 0; i < size; i++){
            if (std::tolower(this->_data[i]) != std::tolower(other[i]))
                return false;
        }

        return true;
    }

    bool String::EndsWith(const char* other, const Int size) const{
        if (this->_size < size)
            return false;

        return std::memcmp(this->_data + this->_size - size, other, size) == 0;
    }

    bool String::EndsWithIgnoreCase(const char* other, const Int size) const{
        if (this->_size < size)
            return false;

        for (int i = 0; i < size; i++){
            if (std::tolower(this->_data[this->_size - size + i]) != std::tolower(other[i]))
                return false;
        }

        return true;
    }

    bool String::Contains(const char* other, const Int size) const {
        if (size == 0)
            return true;

        if (this->_size < size)
            return false;

        for (Int i = 0; i <= this->_size - size; i++) {
            if (std::memcmp(this->_data + i, other, size) == 0)
                return true;
        }

        return false;
    }

    bool String::ContainsIgnoreCase(const char* other, const Int size) const {
        if (size == 0)
            return true;

        if (this->_size < size)
            return false;

        for (Int i = 0; i <= this->_size - size; i++) {
            bool match = true;
            for (Int j = 0; j < size; j++) {
                if (std::tolower(this->_data[i + j]) != std::tolower(other[j])) {
                    match = false;
                    break;
                }
            }
            if (match)
                return true;
        }

        return false;
    }

    String::String(const Memory::IAllocator* allocator)
        : _allocator(allocator), _data(nullptr), _size(0), _capacity(0) {}

    String::String(const Memory::IAllocator* allocator, const Int size){
        this->_allocator = allocator;

        this->_size = 0;
        this->_capacity = size;

        this->_data = static_cast<char*>(allocator->AllocateRaw(size));
    }

    String::String(const object_t* str, const Int size, const Memory::IAllocator* allocator){
        this->_allocator = allocator;
        this->_data = static_cast<char*>(allocator->AllocateRaw(size));

        std::memcpy(this->_data, str, size);
        this->_size = size;
        this->_capacity = size;
    }

    String::String(const char* str, const Memory::IAllocator* allocator){
        this->_allocator = allocator;

        this->_size = static_cast<Int>(std::strlen(str));
        this->_capacity = this->_size;

        this->_data = static_cast<char*>(allocator->AllocateRaw(this->_size));
        std::strcpy(this->_data, str);
    }

    String::String(char* str, const Int size, const Memory::IAllocator* allocator){
        this->_allocator = allocator;

        this->_data = str;

        this->_size = size;
        this->_capacity = size;
    }

    String::String(const String& other)
        : _allocator(other._allocator), _data(nullptr), _size(other._size), _capacity(other._capacity){
        this->_data = static_cast<char*>(this->_allocator->AllocateRaw(other._size));
        std::strcpy(this->_data, other._data);
    }

    String& String::operator=(const String& other){
        if (this == &other)
            return *this;

        this->_allocator = other._allocator;

        this->_size = other._size;
        this->_capacity = other._capacity;

        this->_data = static_cast<char*>(this->_allocator->AllocateRaw(other._size));
        std::strcpy(this->_data, other._data);

        return *this;
    }

    String::String(String&& other) noexcept
        : _allocator(other._allocator), _data(other._data), _size(other._size), _capacity(other._capacity){
        other._allocator = nullptr;
        other._data = nullptr;
    }

    String& String::operator=(String&& other) noexcept{
        if (this == &other)
            return *this;
        this->_allocator = other._allocator;
        this->_data = other._data;

        this->_size = other._size;
        this->_capacity = other._capacity;

        other._allocator = nullptr;
        other._data = nullptr;

        return *this;
    }

    String::~String() = default;

    std::ostream& operator<<(std::ostream& os, const String& sv){
        os.write(sv._data, sv._size);
        return os;
    }

    char String::operator[](const Int index) const{ return this->_data[index];}

    char& String::operator[](const Int index){
        return this->_data[index];
    }

    bool String::operator==(const String& other) const{
        return this->_size == other._size
            && std::memcmp(this->_data, other._data, this->_size) == 0;
    }

    bool String::operator!=(const String& other) const{
        return !(*this == other);
    }

    String::operator std::string_view() const{
        return std::string_view(this->_data, this->_size);
    }

    String::operator std::span<const char>() const{
        return std::span(this->_data, this->_size);
    }

    String operator+(const String& lhs, const String& rhs){
        return lhs.Concat(rhs);
    }

    String operator+(const String& lhs, const char* other){
        return lhs.Concat(other);
    }

    String operator+(const char* lhs, const String& rhs){
        return rhs.Concat(lhs);
    }

    String operator+(const String& lhs, const StringView& rhs){
        return lhs.Concat(rhs);
    }

    String operator+(const StringView& lhs, const String& rhs){
        return rhs.Concat(lhs);
    }

    String operator+(const String& lhs, const std::string_view rhs){
        return lhs.Concat(rhs);
    }

    String operator+(const std::string_view lhs, const String& rhs){
        return rhs.Concat(lhs);
    }

    String operator+(const String& lhs, const std::string& rhs){
        return lhs.Concat(rhs);
    }

    String operator+(const std::string& lhs, const String& rhs){
        return rhs.Concat(lhs);
    }

    String& String::operator+=(const String& other){
        return this->Append(other);
    }

    String& String::operator+=(const char* other){
        return this->Append(other);
    }

    String& String::operator+=(const StringView& other){
        return this->Append(other);
    }

    String& String::operator+=(const std::string_view other){
        return this->Append(other);
    }

    String& String::operator+=(const std::string& other){
        return this->Append(other);
    }

    const char* String::Data() const{
        return this->_data;
    }

    String String::Concat(const String& other) const{
        const Int newSize = this->_size + other._size;
        auto* newString = static_cast<char*>(this->_allocator->AllocateRaw(newSize));

        std::memcpy(newString, this->_data, this->_size);
        std::memcpy(newString + this->_size, other._data, other._size);

        return String(newString, newSize, this->_allocator);
    }

    String String::Concat(const char* other) const{
        const Int otherSize = static_cast<Int>(strlen(other));
        const Int newSize = this->_size + otherSize;
        auto* newString = static_cast<char*>(this->_allocator->AllocateRaw(newSize));

        std::memcpy(newString, this->_data, this->_size);
        std::memcpy(newString + this->_size, other, otherSize);

        return String(newString, newSize, this->_allocator);
    }

    String String::Concat(const StringView& other) const{
        const Int newSize = this->_size + other.Size();
        auto* newString = static_cast<char*>(this->_allocator->AllocateRaw(newSize));
        std::memcpy(newString, this->_data, this->_size);
        std::memcpy(newString + this->_size, other.Data(), other.Size());

        return String(newString, newSize, this->_allocator);
    }

    String String::Concat(const std::string_view other) const{
        const Int newSize = static_cast<Int>(this->_size + other.size());
        auto* newString = static_cast<char*>(this->_allocator->AllocateRaw(newSize));

        std::memcpy(newString, this->_data, this->_size);
        std::memcpy(newString + this->_size, other.data(), other.size());

        return String(newString, newSize, this->_allocator);
    }

    String String::Concat(const std::string& other) const{
        const Int newSize = static_cast<Int>(this->_size + other.size());
        auto* newString = static_cast<char*>(this->_allocator->AllocateRaw(newSize));

        std::memcpy(newString, this->_data, this->_size);
        std::memcpy(newString + this->_size, other.data(), other.size());

        return String(newString, newSize, this->_allocator);
    }

    String& String::Append(const String& other){
        const Int newSize = this->_size + other._size;
        if (!this->CanFit(newSize))
            this->CalculateCapacity(newSize);

        auto* newStr = static_cast<char*>(this->_allocator->AllocateRaw(this->_capacity));

        std::memcpy(newStr, this->_data, this->_size);
        std::memcpy(newStr + this->_size, other._data, other._size);

        return *this;

    }

    String& String::Append(const StringView& other){
        const Int newSize = this->_size + other.Size();
        if (!this->CanFit(newSize))
            this->CalculateCapacity(newSize);

        auto* newStr = static_cast<char*>(this->_allocator->AllocateRaw(this->_capacity));

        std::memcpy(newStr, this->_data, this->_size);
        std::memcpy(newStr + this->_size, other.Data(), other.Size());

        return *this;
    }

    String& String::Append(const char* other){
        const auto otherSize = static_cast<Int>(std::strlen(other));
        const Int newSize = this->_size + otherSize;

        if (!this->CanFit(newSize))
            this->CalculateCapacity(newSize);

        auto* newStr = static_cast<char*>(this->_allocator->AllocateRaw(this->_capacity));

        std::memcpy(newStr, this->_data, this->_size);
        std::memcpy(newStr + this->_size, other, otherSize);

        return *this;
    }

    String& String::Append(const std::string_view other){
        const Int newSize = static_cast<Int>(this->_size + other.size());

        if (!this->CanFit(newSize))
            this->CalculateCapacity(newSize);

        auto* newStr = static_cast<char*>(this->_allocator->AllocateRaw(this->_capacity));
        std::memcpy(newStr, this->_data, this->_size);
        std::memcpy(newStr + this->_size, other.data(), other.size());

        return *this;
    }

    String& String::Append(const std::string& other){
        const Int newSize = static_cast<Int>(this->_size + other.size());

        if (!this->CanFit(newSize))
            this->CalculateCapacity(newSize);

        auto* newStr = static_cast<char*>(this->_allocator->AllocateRaw(this->_capacity));
        std::memcpy(newStr, this->_data, this->_size);
        std::memcpy(newStr + this->_size, other.data(), other.size());

        return *this;
    }

    void String::Insert(const Int pos, const char* data, const Int size){
        if (pos < 0 || pos > this->_size)
            throw std::out_of_range("String::Insert: Position out of range.");

        const Int newSize = this->_size + size;
        if (!this->CanFit(newSize))
            this->CalculateCapacity(newSize);

        auto* newStr = static_cast<char*>(this->_allocator->AllocateRaw(this->_capacity));

        std::memcpy(newStr, this->_data, pos);
        std::memcpy(newStr + pos, data, size);
        std::memcpy(newStr + pos + size, this->_data + pos, this->_size - pos);

        this->_size = newSize;
        this->_data = newStr;
    }

    String String::Substring(const Int startIndex, const Int length) const{
        if (startIndex < 0 || startIndex >= this->_size)
            throw std::out_of_range("Index out of range.");

        if (length < 0 || startIndex + length > this->_size)
            throw std::out_of_range("Length out of range.");

        return String(this->_data + startIndex, length, this->_allocator);
    }

    StringView String::SubstringView(const Int startIndex, const Int length) const{
        if (startIndex < 0 || startIndex >= this->_size)
            throw std::out_of_range("Index out of range.");

        if (length < 0 || startIndex + length > this->_size)
            throw std::out_of_range("Length out of range.");

        return StringView(this->_data + startIndex, length);
    }

    Int String::Size() const{
        return this->_size;
    }

    Int String::IndexOf(const char c) const{
        for (Int i = 0; i < this->_size; i++)
            if (this->_data[i] == c)
                return i;
        return -1;
    }

    bool String::Contains(const String& other, const StringComparisonType type) const{
        switch (type) {
            case StringComparisonType::Equals:
                return *this == other;
            case StringComparisonType::EqualsIgnoreOrdinalCase:
                return this->EqualsIgnoreCase(other._data, other._size);
            case StringComparisonType::StartsWith:
                return this->StartsWith(other._data, other._size);
            case StringComparisonType::StartsWithIgnoreOrdinalCase:
                return this->StartsWithIgnoreCase(other._data, other._size);
            case StringComparisonType::EndsWith:
                return this->EndsWith(other._data, other._size);
            case StringComparisonType::EndsWithIgnoreOrdinalCase:
                return this->EndsWithIgnoreCase(other._data, other._size);
            case StringComparisonType::Contains:
                return this->Contains(other._data, other._size);
            case StringComparisonType::ContainsIgnoreCase:
                return this->ContainsIgnoreCase(other._data, other._size);
            default:
                throw std::invalid_argument("Invalid StringComparisonType.");
        }
    }

    bool String::Contains(const char* other, const StringComparisonType type) const{
        const auto otherSize = static_cast<Int>(std::strlen(other));
            switch (type) {
                case StringComparisonType::Equals:
                    return this->Equals(other, otherSize);
                case StringComparisonType::EqualsIgnoreOrdinalCase:
                    return this->EqualsIgnoreCase(other, otherSize);
                case StringComparisonType::StartsWith:
                    return this->StartsWith(other, otherSize);
                case StringComparisonType::StartsWithIgnoreOrdinalCase:
                    return this->StartsWithIgnoreCase(other, otherSize);
                case StringComparisonType::EndsWith:
                    return this->EndsWith(other, otherSize);
                case StringComparisonType::EndsWithIgnoreOrdinalCase:
                    return this->EndsWithIgnoreCase(other, otherSize);
                case StringComparisonType::Contains:
                    return this->Contains(other, otherSize);
                case StringComparisonType::ContainsIgnoreCase:
                    return this->ContainsIgnoreCase(other, otherSize);
                default:
                    throw std::invalid_argument("Invalid StringComparisonType.");
        }
    }

    bool String::Contains(const StringView& other, const StringComparisonType type) const{
        switch (type) {
            case StringComparisonType::Equals:
                return this->Equals(other.Data(), other.Size());
            case StringComparisonType::EqualsIgnoreOrdinalCase:
                return this->EqualsIgnoreCase(other.Data(), other.Size());
            case StringComparisonType::StartsWith:
                return this->StartsWith(other.Data(), other.Size());
            case StringComparisonType::StartsWithIgnoreOrdinalCase:
                return this->StartsWithIgnoreCase(other.Data(), other.Size());
            case StringComparisonType::EndsWith:
                return this->EndsWith(other.Data(), other.Size());
            case StringComparisonType::EndsWithIgnoreOrdinalCase:
                return this->EndsWithIgnoreCase(other.Data(), other.Size());
            case StringComparisonType::Contains:
                return this->Contains(other.Data(), other.Size());
            case StringComparisonType::ContainsIgnoreCase:
                return this->ContainsIgnoreCase(other.Data(), other.Size());
            default:
                throw std::invalid_argument("Invalid StringComparisonType.");
        }
    }

    bool String::Contains(const std::string_view other, const StringComparisonType type) const{
        switch (type) {
            case StringComparisonType::Equals:
                return this->Equals(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::EqualsIgnoreOrdinalCase:
                return this->EqualsIgnoreCase(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::StartsWith:
                return this->StartsWith(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::StartsWithIgnoreOrdinalCase:
                return this->StartsWithIgnoreCase(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::EndsWith:
                return this->EndsWith(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::EndsWithIgnoreOrdinalCase:
                return this->EndsWithIgnoreCase(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::Contains:
                return this->Contains(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::ContainsIgnoreCase:
                return this->ContainsIgnoreCase(other.data(), static_cast<Int>(other.size()));
            default:
                throw std::invalid_argument("Invalid StringComparisonType.");
        }
    }

    bool String::Contains(const std::string& other, const StringComparisonType type) const{
        switch (type) {
            case StringComparisonType::Equals:
                return this->Equals(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::EqualsIgnoreOrdinalCase:
                return this->EqualsIgnoreCase(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::StartsWith:
                return this->StartsWith(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::StartsWithIgnoreOrdinalCase:
                return this->StartsWithIgnoreCase(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::EndsWith:
                return this->EndsWith(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::EndsWithIgnoreOrdinalCase:
                return this->EndsWithIgnoreCase(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::Contains:
                return this->Contains(other.data(), static_cast<Int>(other.size()));
            case StringComparisonType::ContainsIgnoreCase:
                return this->ContainsIgnoreCase(other.data(), static_cast<Int>(other.size()));
            default:
                throw std::invalid_argument("Invalid StringComparisonType.");
        }
    }

    bool String::Empty() const{
        return this->_size == 0;
    }

    String::const_iterator String::begin() const{
        return this->_data;
    }

    String::const_iterator String::end() const{
        return this->_data + this->_size;
    }
}
