#include "../../include/DataTypes/String.h"
#include "../../include/Memory/IAllocator.h"
#include <cstring>
#include <ostream>

#include "Comparators.h"

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

    bool String::Equals(const StringView& lhs, const StringView& rhs){
        return lhs.Size() == rhs.Size()
            && std::memcmp(lhs.Data(), rhs.Data(), lhs.Size()) == 0;
    }

    bool String::EqualsIgnoreCase(const StringView& lhs, const StringView& rhs){
        const auto leftSize = lhs.Size();
        return (leftSize == rhs.Size())
            && strncasecmp(lhs.Data(), rhs.Data(), leftSize) == 0;
    }

    bool String::StartsWith(const StringView& lhs, const StringView& rhs){
        return (lhs.Size() >= rhs.Size())
            && strncmp(lhs.Data(), rhs.Data(), rhs.Size()) == 0;
    }

    bool String::StartsWithIgnoreCase(const StringView& lhs, const StringView& rhs){
        return (lhs.Size() >= rhs.Size())
            && strncasecmp(lhs.Data(), rhs.Data(), rhs.Size()) == 0;
    }

    bool String::EndsWith(const StringView& lhs, const StringView& rhs){
        return (lhs.Size() >= rhs.Size())
            && strncmp(lhs.Data() + lhs.Size() - rhs.Size(), rhs.Data(), rhs.Size()) == 0;
    }

    bool String::EndsWithIgnoreCase(const StringView& lhs, const StringView& rhs){
        return (lhs.Size() >= rhs.Size())
            && strncasecmp(lhs.Data() + lhs.Size() - rhs.Size(), rhs.Data(), rhs.Size()) == 0;
    }

    bool String::Contains(const StringView& lhs, const StringView& rhs){
        const auto leftSize = lhs.Size();
        const auto rightSize = rhs.Size();

        if (leftSize == 0)
            return true;

        if (leftSize < rightSize)
            return false;

        for (Int i = 0; i <= leftSize - rightSize; i++) {
            if (strncmp(lhs.Data() + i, rhs.Data(), rhs.Size()) == 0)
                return true;
        }

        return false;
    }

    bool String::ContainsIgnoreCase(const StringView& lhs, const StringView& rhs){
        const auto leftSize = lhs.Size();
        const auto rightSize = rhs.Size();

        if (leftSize == 0)
            return true;

        if (leftSize < rightSize)
            return false;

        for (Int i = 0; i <= leftSize - rightSize; i++) {
            if (strncasecmp(lhs.Data() + i, rhs.Data(), rhs.Size()) == 0)
                return true;
        }

        return false;
    }

    String& String::Append(const char* data, const Int size){
        const Int newSize = this->_size + size;

        if (!this->CanFit(newSize))
            this->CalculateCapacity(newSize);

        auto* newStr = static_cast<char*>(this->_allocator->AllocateRaw(this->_capacity));

        std::memcpy(newStr, this->_data, this->_size);
        std::memcpy(newStr + this->_size, data, size);

        this->_data = newStr;
        this->_size = newSize;
        return *this;
    }

    String String::Normalize(
        const char* str,
        const Int size,
        const ::Memory::IAllocator* allocator
    ){
        auto* newStr = static_cast<char*>(allocator->AllocateRaw(size));
        for (Int i = 0; i < size; i++)
            newStr[i] = std::tolower(str[i]);
        return String(newStr, size, allocator);
    }

    String String::Lower(
        const char* str,
        const Int size,
        const Memory::IAllocator* allocator
    ){
        auto* newStr = static_cast<char*>(allocator->AllocateRaw(size));
        for (Int i = 0; i < size; i++)
            newStr[i] = std::tolower(str[i]);
        return String(newStr, size, allocator);
    }

    String String::Upper(
        const char* str,
        const Int size,
        const Memory::IAllocator* allocator
    ){
        auto* newStr = static_cast<char*>(allocator->AllocateRaw(size));
        for (Int i = 0; i < size; i++)
            newStr[i] = std::toupper(str[i]);
        return String(newStr, size, allocator);
    }

    StringView String::Trim(
        const char* str,
        const Int size
    ){
        if (size == 0)
            return StringView(str, 0);

        int firstIndex = 0;
        int lastIndex = size - 1;

        for (Int i = 0; i < size; i++){
            if (!::isspace(str[i])){
                firstIndex = i;
                break;
            }
        }

        for (Int i = size - 1; i >= 0; i--){
            if (!::isspace(str[i])){
                lastIndex = i;
                break;
            }
        }

        return StringView(str + firstIndex, size - lastIndex);
    }

    StringView String::TrimLeft(
        const char* str,
        const Int size
    ){
        if (size == 0)
            return StringView(str, 0);

        int firstIndex = 0;

        for(int i = 0;i < size; i++)
            if(!isspace(str[i]))
            {
                firstIndex = i;
                break;
            }
        return StringView(str + firstIndex, size - firstIndex);
    }

    StringView String::TrimRight(
        const char* str,
        const Int size
    ){
        if (size == 0)
            return StringView(str, 0);

        int lastIndex = size - 1;
        for(int i = size - 1; i >= 0; i--)
            if(!isspace(str[i]))
            {
                lastIndex = i;
                break;
            }

        return StringView(str, lastIndex + 1);
    }

    String String::Replace(
        const char* str,
        const Int size,
        const StringView& subStr,
        const StringView& newStr,
        const ::Memory::IAllocator* allocator
    ){
        if (size == 0 || subStr.Size() == 0){
            auto* strCopy = static_cast<char*>(allocator->AllocateRaw(size));
            std::memcpy(strCopy, str, size);
            return String(strCopy, size, allocator);
        }

        Int count = 0;
        const auto subStrSize = subStr.Size();
        for (Int i = 0; i <= size - subStrSize; ){
            if (std::memcmp(str + i, subStr.Data(), subStrSize) == 0){
                count += 1;
                i += subStrSize;
            } else {
                i++;
            }
        }

        if (count == 0){
            auto* strCopy = static_cast<char*>(allocator->AllocateRaw(size));
            std::memcpy(strCopy, str, size);
            return String(strCopy, size, allocator);
        }

        const auto newSize = size - subStrSize * count + newStr.Size() * count;
        auto* newStrCopy = static_cast<char*>(allocator->AllocateRaw(newSize));

        Int i = 0, j = 0;
        while (j < size && i < newSize){
            if (j + subStrSize <= size && std::memcmp(str + j, subStr.Data(), subStrSize) == 0){
                std::memcpy(newStrCopy + i, newStr.Data(), newStr.Size());
                i += static_cast<Int>(newStr.Size());
                j += subStrSize;
            } else {
                newStrCopy[i++] = str[j++];
            }
        }

        return String(newStrCopy, newSize, allocator);
    }

    StringView String::SubString(
        const char* str,
        const Int size,
        const Int start,
        const Int end
    ){
        if (start < 0 || start >= size)
            throw std::out_of_range("String::SubString: Index out of range.");
        if (end < 0 || end > size || start > end)
            throw std::out_of_range("String::SubString: Size out of range.");

        return StringView(str + start, end - start);
    }

    String String::Reverse(
        const char* str,
        const Int size,
        const Memory::IAllocator* allocator
    ){
        if (size == 0)
            return String::Empty(allocator);

        auto* newStr = static_cast<char*>(allocator->AllocateRaw(size));
        for (Int i = 0; i < size / 2; i++){
            newStr[i] = str[size - i - 1];
            newStr[size - i - 1] = str[i];
        }
        if (size % 2 != 0)
            newStr[size / 2] = str[size / 2];
        return String(newStr, size, allocator);
    }

    StringView String::Left(const char* str, const Int size, const Int count){
        if (size == 0)
            throw std::out_of_range("String::Left: Size out of range.");

        if (count > size)
            throw std::out_of_range("String::Left: Count out of range.");

        return StringView(str, count);
    }

    StringView String::Right(const char* str, const Int size, const Int count){
        if (size == 0)
            throw std::out_of_range("String::Right: Size out of range.");

        if (count > size)
            throw std::out_of_range("String::Right: Count out of range.");

        return StringView(str + (size - count), count);
    }

    String String::Repeat(
        const char* str,
        const Int size,
        const Int count,
        const Memory::IAllocator* allocator
    ){
        if (size == 0)
            return String::Empty(allocator);

        const auto newSize = size * count;
        auto* newStr = static_cast<char*>(allocator->AllocateRaw(newSize));
        for (Int i = 0; i < count; i++){
            std::memcpy(newStr + size * i, str, size);
        }
        return String(newStr, newSize, allocator);
    }

    StringView String::Split(
        const char* str,
        const Int size,
        const Int startIndex,
        const char delimiter
    ){
        if (size == 0)
            return StringView(str, 0);

        for (int i = startIndex; i < size; i++){
            if (strncasecmp(&str[i], &delimiter, 1) == 0)
                return StringView(str + startIndex, i - startIndex);
        }

        return StringView(str + startIndex, size - startIndex);
    }

    String::String(){
        this->_allocator = nullptr;
        this->_data = nullptr;
        this->_size = 0;
        this->_capacity = 0;
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

    String::String(object_t* str, const Int size, const ::Memory::IAllocator* allocator){
        this->_allocator = allocator;
        this->_data = reinterpret_cast<char*>(str);
        this->_size = size;
        this->_capacity = size;
    }

    String::String(const char* str, const Int size, const Memory::IAllocator* allocator){
        this->_allocator = allocator;
        this->_data = static_cast<char*>(allocator->AllocateRaw(size));

        std::memcpy(this->_data, str, size);

        this->_size = size;
        this->_capacity = size;
    }

    String::String(char* str, const Int size, const Memory::IAllocator* allocator){
        this->_allocator = allocator;

        this->_data = str;

        this->_size = size;
        this->_capacity = size;
    }

    String::String(const String& other)
        : _allocator(other._allocator), _data(nullptr), _size(other._size), _capacity(other._capacity){
        if (other._data == nullptr)
            return;

        this->_data = static_cast<char*>(this->_allocator->AllocateRaw(other._size));
        std::memcpy(this->_data, other._data, other._size);
    }

    String& String::operator=(const String& other){
        if (this == &other)
            return *this;

        this->_allocator = other._allocator;

        this->_size = other._size;
        this->_capacity = other._capacity;

        if (other._data != nullptr){
            this->_data = static_cast<char*>(this->_allocator->AllocateRaw(other._size));
            std::memcpy(this->_data, other._data, other._size);
        }

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

    String String::Empty(const Memory::IAllocator* allocator){
        return String(allocator, 0);
    }

    String String::Null(){
        return String();
    }

    void String::SetAllocator(const Memory::IAllocator* allocator){
        this->_allocator = allocator;
    }

    void String::Reserve(const Int size){
        if (this->_capacity >= size)
            return;

        auto newCapacity = this->_capacity == 0 ? 1 : this->_capacity * 2;
        while (newCapacity < size)
            newCapacity *= 2;

        auto* newData = static_cast<char*>(this->_allocator->AllocateRaw(newCapacity));
        std::memcpy(newData, this->_data, this->_size);

        this->_data = newData;
        this->_capacity = newCapacity;
    }

    void String::Resize(const Int size){
        if (this->_size == size)
            return;

        if (size > this->_capacity)
            this->Reserve(size);

        this->_size = size;
    }

    std::ostream& operator<<(std::ostream& os, const String& sv){
        os.write(sv._data, sv._size);
        return os;
    }

    char String::operator[](const Int index) const{ return this->_data[index];}

    char& String::operator[](const Int index){
        return this->_data[index];
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

    bool operator==(const String& lhs, const String& rhs){
        return String::Equals(lhs.ToView(), rhs.ToView());
    }

    bool operator!=(const String& lhs, const String& rhs){
        return !String::Equals(lhs.ToView(), rhs.ToView());
    }

    bool operator<=(const String& lhs, const String& rhs){
        return Comparators::Compare(lhs, rhs) <= Comparators::Comparator::Equal;
    }

    bool operator<(const String& lhs, const String& rhs){
        return Comparators::Compare(lhs, rhs) == Comparators::Comparator::Less;
    }

    bool operator>=(const String& lhs, const String& rhs){
        return Comparators::Compare(lhs, rhs) >= Comparators::Comparator::Equal;
    }

    bool operator>(const String& lhs, const String& rhs){
        return Comparators::Compare(lhs, rhs) == Comparators::Comparator::Greater;
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

    char* String::Data(){
        return this->_data;
    }

    const char* String::Data() const{
        return this->_data;
    }

    StringView String::ToView() const{
        return StringView(this->_data, this->_size);
    }

    String String::FromView(const StringView& str, const Memory::IAllocator* allocator){
        return String(str.Data(), str.Size(), allocator);
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
        return this->Append(other.Data(), other.Size());
    }

    String& String::Append(const StringView& other){
        return this->Append(other.Data(), other.Size());
    }

    String& String::Append(const char* other){
        return this->Append(other, static_cast<Int>(std::strlen(other)));
    }

    String& String::Append(const std::string_view other){
        return this->Append(other.data(), static_cast<Int>(other.size()));
    }

    String& String::Append(const std::string& other){
        return this->Append(other.data(), static_cast<Int>(other.size()));
    }

    String& String::Append(const char other){
        return this->Append(&other, 1);
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

    Int String::Size() const{
        return this->_size;
    }

    Int String::IndexOf(const char c) const{
        for (Int i = 0; i < this->_size; i++)
            if (this->_data[i] == c)
                return i;
        return -1;
    }

    bool String::Empty() const{
        return this->_size == 0;
    }

    String String::ToLower() const{
        auto* buf = static_cast<char*>(this->_allocator->AllocateRaw(this->_size));
        for (Int i = 0; i < this->_size; i++)
            buf[i] = static_cast<char>(std::tolower(this->_data[i]));
        return String(buf, this->_size, this->_allocator);
    }

    String String::ToUpper() const{
        auto* buf = static_cast<char*>(this->_allocator->AllocateRaw(this->_size));
        for (Int i = 0; i < this->_size; i++)
            buf[i] = static_cast<char>(std::toupper(this->_data[i]));
        return String(buf, this->_size, this->_allocator);
    }

    void String::ToLowerInPlace() const{
        for (Int i = 0; i < this->_size; i++)
            this->_data[i] = static_cast<char>(std::tolower(this->_data[i]));
    }

    void String::ToUpperInPlace() const{
        for (Int i = 0; i < this->_size; i++)
            this->_data[i] = static_cast<char>(std::toupper(this->_data[i]));
    }

    String String::Normalize(const String& str){
        return str.ToLower();
    }

    String String::Normalize(const char* str, const ::Memory::IAllocator* allocator){
        const auto size = static_cast<Int>(std::strlen(str));
        return String::Normalize(str, size, allocator);
    }

    String String::Normalize(const StringView& str, const ::Memory::IAllocator* allocator){
        return String::Normalize(str.Data(), str.Size(), allocator);
    }

    String String::Normalize(const std::string_view str, const ::Memory::IAllocator* allocator){
        return String::Normalize(str.data(), str.size(), allocator);
    }

    String String::Normalize(const std::string& str, const ::Memory::IAllocator* allocator){
        return String::Normalize(str.data(), str.size(), allocator);
    }

    String String::Lower(const String& str){
        return str.ToLower();
    }

    String String::Lower(const char* str, const Memory::IAllocator* allocator){
        return String::Lower(str, static_cast<Int>(std::strlen(str)), allocator);
    }

    String String::Lower(const StringView& str, const Memory::IAllocator* allocator){
        return String::Lower(str.Data(), str.Size(), allocator);
    }

    String String::Lower(const std::string_view str, const Memory::IAllocator* allocator){
        return String::Lower(str.data(), str.size(), allocator);
    }

    String String::Lower(const std::string& str, const Memory::IAllocator* allocator){
        return String::Lower(str.data(), str.size(), allocator);
    }

    String String::Upper(const String& str){
        return str.ToUpper();
    }

    String String::Upper(const char* str, const Memory::IAllocator* allocator){
        return String::Upper(str, static_cast<Int>(std::strlen(str)), allocator);
    }

    String String::Upper(const StringView& str, const Memory::IAllocator* allocator){
        return String::Upper(str.Data(), str.Size(), allocator);
    }

    String String::Upper(const std::string_view str, const Memory::IAllocator* allocator){
        return String::Upper(str.data(), str.size(), allocator);
    }

    String String::Upper(const std::string& str, const Memory::IAllocator* allocator){
        return String::Upper(str.data(), str.size(), allocator);
    }

    StringView String::Trim(const String& str){
        return String::Trim(str._data, str._size);
    }

    StringView String::Trim(const char* str){
        return String::Trim(str, static_cast<Int>(std::strlen(str)));
    }

    StringView String::Trim(const StringView& str){
        return String::Trim(str.Data(), str.Size());
    }

    StringView String::Trim(const std::string_view str){
        return String::Trim(str.data(), str.size());
    }

    StringView String::Trim(const std::string& str){
        return String::Trim(str.data(), str.size());
    }

    Int String::Ascii(const String& str){
        return str.Empty() ? 0 : str[0];
    }

    Int String::Ascii(const char* str){
        return (str == nullptr || str[0] == '\0')
                   ? 0
                   : str[0];
    }

    Int String::Ascii(const StringView& str){
        return str.Empty() ? 0 : str[0];
    }

    Int String::Ascii(const std::string_view str){
        return str.empty() ? 0 : str[0];
    }

    Int String::Ascii(const std::string& str){
        return str.empty() ? 0 : str[0];
    }

    Int String::Length(const String& str){
        return str.Size();
    }

    Int String::Length(const char* str){
        return str == nullptr
                   ? 0
                   : static_cast<Int>(std::strlen(str));
    }

    Int String::Length(const StringView& str){
        return str.Size();
    }

    Int String::Length(const std::string_view str){
        return str.size();
    }

    Int String::Length(const std::string& str){
        return str.size();
    }

    StringView String::TrimLeft(const String& str){
        return String::TrimLeft(str._data, str._size);
    }

    StringView String::TrimLeft(const char* str){
        return String::TrimLeft(str, static_cast<Int>(std::strlen(str)));
    }

    StringView String::TrimLeft(const StringView& str){
        return String::TrimLeft(str.Data(), str.Size());
    }

    StringView String::TrimLeft(const std::string_view str){
        return String::TrimLeft(str.data(), str.size());
    }

    StringView String::TrimLeft(const std::string& str){
        return String::TrimLeft(str.data(), str.size());
    }

    StringView String::TrimRight(const String& str){
        return String::TrimRight(str._data, str._size);
    }

    StringView String::TrimRight(const char* str){
        return String::TrimRight(str, static_cast<Int>(std::strlen(str)));
    }

    StringView String::TrimRight(const StringView& str){
        return String::TrimRight(str.Data(), str.Size());
    }

    StringView String::TrimRight(const std::string_view str){
        return String::TrimRight(str.data(), str.size());
    }

    StringView String::TrimRight(const std::string& str){
        return String::TrimRight(str.data(), str.size());
    }

    String String::Replace(
        const String& str,
        const StringView& oldStr,
        const StringView& newStr
    ){
        return String::Replace(
            str._data,
            str._size,
            oldStr,
            newStr,
            str._allocator
        );
    }

    String String::Replace(
        const StringView& str,
        const StringView& oldStr,
        const StringView& newStr,
        const ::Memory::IAllocator* allocator
    ){
        return String::Replace(
            str.Data(),
            str.Size(),
            oldStr,
            newStr,
            allocator
        );
    }

    String String::Replace(
        const char* str,
        const StringView& oldStr,
        const StringView& newStr,
        const ::Memory::IAllocator* allocator
    ){
        return String::Replace(
            str,
            static_cast<Int>(std::strlen(str)),
            oldStr,
            newStr,
            allocator
        );
    }

    String String::Replace(
        const std::string_view str,
        const StringView& oldStr,
        const StringView& newStr,
        const ::Memory::IAllocator* allocator
    ){
        return String::Replace(
            str.data(),
            str.size(),
            oldStr,
            newStr,
            allocator
        );
    }

    String String::Replace(
        const std::string& str,
        const StringView& oldStr,
        const StringView& newStr,
        const ::Memory::IAllocator* allocator
    ){
        return String::Replace(
            str.data(),
            str.size(),
            oldStr,
            newStr,
            allocator
        );
    }

    StringView String::SubString(const String& str, const Int start, const Int end){
        return String::SubString(str._data, str._size, start, end);
    }

    StringView String::SubString(const StringView& str, const Int start, const Int end){
        return String::SubString(str.Data(), str.Size(), start, end);
    }

    StringView String::SubString(const char* str, Int start, Int end){
        return String::SubString(str, static_cast<Int>(std::strlen(str)), start, end);
    }

    StringView String::SubString(const std::string_view str, const Int start, const Int end){
        return String::SubString(str.data(), str.size(), start, end);
    }

    StringView String::SubString(const std::string& str, const Int start, const Int end){
        return String::SubString(str.data(), str.size(), start, end);
    }

    String String::Reverse(const String& str){
        return String::Reverse(str._data, str._size, str._allocator);
    }

    String String::Reverse(const StringView& str, const ::Memory::IAllocator* allocator){
        return String::Reverse(str.Data(), str.Size(), allocator);
    }

    String String::Reverse(const char* str, const Memory::IAllocator* allocator){
        return String::Reverse(str, static_cast<Int>(std::strlen(str)), allocator);
    }

    String String::Reverse(const std::string_view str, const Memory::IAllocator* allocator){
        return String::Reverse(str.data(), str.size(), allocator);
    }

    String String::Reverse(const std::string& str, const Memory::IAllocator* allocator){
        return String::Reverse(str.data(), str.size(), allocator);
    }

    StringView String::Left(const String& str, const Int count){
        return String::Left(str._data, str._size, count);
    }

    StringView String::Left(const StringView& str, const Int count){
        return String::Left(str.Data(), str.Size(), count);
    }

    StringView String::Left(const char* str, const Int count){
        return String::Left(str, static_cast<Int>(std::strlen(str)), count);
    }

    StringView String::Left(const std::string_view str, const Int count){
        return String::Left(str.data(), str.size(), count);
    }

    StringView String::Left(const std::string& str, const Int count){
        return String::Left(str.data(), str.size(), count);
    }

    StringView String::Right(const String& str, const Int count){
        return String::Right(str._data, str._size, count);
    }

    StringView String::Right(const StringView& str, const Int count){
        return String::Right(str.Data(), str.Size(), count);
    }

    StringView String::Right(const char* str, const Int count){
        return String::Right(str, static_cast<Int>(std::strlen(str)), count);
    }

    StringView String::Right(std::string_view str, const Int count){
        return String::Right(str.data(), str.size(), count);
    }

    StringView String::Right(const std::string& str, const Int count){
        return String::Right(str.data(), str.size(), count);
    }

    String String::Char(const Int AsciiCode, const Memory::IAllocator* allocator){
        auto* buf = static_cast<char*>(allocator->AllocateRaw(1));
        buf[0] = static_cast<char>(AsciiCode);
        return String(buf, 1, allocator);
    }

    Int String::CharIndex(const StringView& subStr, const StringView& str, const Int start){
        if(subStr.Empty()
            || str.Empty()
            || start < 0
            || start > str.Size()
            || subStr.Size() > str.Size()
        ) return 0;

        for (int i = start; i < str.Size() - subStr.Size() + 1; i++) {
            if (std::strncmp(str.Data() + i, subStr.Data(), subStr.Size()) == 0)
                return i;
        }
        return -1;
    }

    String String::Repeat(const String& str, const Int count){
        return String::Repeat(str._data, str._size, count, str._allocator);
    }

    String String::Repeat(const StringView& str, const Int count, const Memory::IAllocator* allocator){
        return String::Repeat(str.Data(), str.Size(), count, allocator);
    }

    String String::Repeat(const char* str, const Int count, const Memory::IAllocator* allocator){
        return String::Repeat(str, static_cast<Int>(std::strlen(str)), count, allocator);
    }

    String String::Repeat(const std::string_view str, const Int count, const Memory::IAllocator* allocator){
        return String::Repeat(str.data(), str.size(), count, allocator);
    }

    String String::Repeat(const std::string& str, const Int count, const Memory::IAllocator* allocator){
        return String::Repeat(str.data(), str.size(), count, allocator);
    }

    String String::Space(const Int count, const Memory::IAllocator* allocator){
        auto* buf = static_cast<char*>(allocator->AllocateRaw(count));
        std::memset(buf, ' ', count);
        return String(buf, count, allocator);
    }

    StringView String::Split(
        const String& str,
        const Int startIndex,
        const char delimiter
    ){
        return String::Split(str._data, str._size, startIndex, delimiter);
    }

    StringView String::Split(
        const StringView& str,
        const Int startIndex,
        const char delimiter
    ){
        return String::Split(str.Data(), str.Size(), startIndex, delimiter);
    }

    StringView String::Split(
        const char* str,
        const Int startIndex,
        const char delimiter
    ){
        return String::Split(str, static_cast<Int>(std::strlen(str)), startIndex, delimiter);
    }

    StringView String::Split(
        const std::string_view str,
        const Int startIndex,
        const char delimiter
    ){
        return String::Split(str.data(), static_cast<Int>(str.size()), startIndex, delimiter);
    }

    StringView String::Split(
        const std::string& str,
        const Int startIndex,
        const char delimiter
    ){
        return String::Split(str.data(), static_cast<Int>(str.size()), startIndex, delimiter);
    }

    String::const_iterator String::begin() const{
        return this->_data;
    }

    String::const_iterator String::end() const{
        return this->_data + this->_size;
    }

    String::reverse_iterator String::rbegin() const{
        return reverse_iterator(this->_data + this->_size);
    }

    String::reverse_iterator String::rend() const{
        return reverse_iterator(this->_data);
    }

    char String::First() const{
        if (this->_size == 0)
            throw std::out_of_range("String is empty.");
        return this->_data[0];
    }

    char String::Last() const{
        if (this->_size == 0)
            throw std::out_of_range("String is empty.");
        return this->_data[this->_size - 1];
    }

    void String::Insert(const Int index, const char c){
        if (index < 0 || index > this->_size)
            throw std::out_of_range("Index out of range.");

        if (!this->CanFit(this->_size + 1))
            this->CalculateCapacity(this->_size + 1);

        auto* buf = static_cast<char*>(this->_allocator->AllocateRaw(this->_capacity));
        std::memcpy(buf, this->_data, index * sizeof(char));
        buf[index] = c;
        std::memcpy(buf + index + 1, this->_data + index, (this->_size - index) * sizeof(char));
        this->_data = buf;
        this->_size++;
    }

    void String::Pop(){
        if (this->_size == 0)
            throw std::out_of_range("String is empty.");
        this->_size--;
    }

    bool StringEqualsIgnoreCase::operator()(const String& lhs, const String& rhs) const {
        return String::Compare<StringComparisonType::EqualsIgnoreCase>(lhs, rhs);
    }
}
