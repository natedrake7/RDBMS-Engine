#include "../../include/DataTypes/String.h"
#include "../../include/Memory/IAllocator.h"
#include <cstring>
#include <ostream>

#include "Comparators.h"
#include "DataTypes/StringValue.h"

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

    String& String::Append(const StringView& other){
        const auto otherSize = other.Size();

        const Int newSize = this->_size + otherSize;

        if (this->CanFit(newSize)){
            std::memcpy(this->_data + this->_size, other.Data(), otherSize);
            this->_size = newSize;
            return *this;
        }

        this->CalculateCapacity(newSize);

        auto* newStr = static_cast<char*>(this->_allocator->AllocateRaw(this->_capacity));

        std::memcpy(newStr, this->_data, this->_size);
        std::memcpy(newStr + this->_size, other.Data(), otherSize);

        this->_data = newStr;
        this->_size = newSize;
        return *this;
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
        :   _allocator(other._allocator), _data(nullptr),
            _size(other._size), _capacity(other._size){
        if (other._data == nullptr)
            return;

        this->_data = static_cast<char*>(this->_allocator->AllocateRaw(other._size));
        std::memcpy(this->_data, other._data, other._size);
    }

    String& String::operator=(const String& other){
        if (this == &other)
            return *this;

        this->_allocator = other._allocator;

        if (other._data != nullptr){
            this->_data = static_cast<char*>(this->_allocator->AllocateRaw(other._size));
            std::memcpy(this->_data, other._data, other._size);
            this->_size = other._size;
            this->_capacity = other._size;
        }
        else{
            this->_data = nullptr;
            this->_capacity = 0;
            this->_size = 0;
            this->_capacity = 0;
        }

        return *this;
    }

    String::String(String&& other) noexcept
        :   _allocator(other._allocator), _data(other._data),
            _size(other._size), _capacity(other._capacity){
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

        if (this->_data != nullptr){
            auto* newBuffer = static_cast<char*>(this->_allocator->AllocateRaw(this->_size));
            std::memcpy(newBuffer, this->_data, this->_size);
            this->_data = newBuffer;
        }
    }

    const Memory::IAllocator* String::GetAllocator() const{
        return this->_allocator;
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

    bool operator==(const String& lhs, const String& rhs){
        return StringView::Compare<StringComparisonType::Equals>(lhs, rhs);
    }

    bool operator!=(const String& lhs, const String& rhs){
        return !StringView::Compare<StringComparisonType::Equals>(lhs, rhs);
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

    char* String::Data(){
        return this->_data;
    }

    const char* String::Data() const{
        return this->_data;
    }

    String String::FromView(const StringView& str, const Memory::IAllocator* allocator){
        return String(str.Data(), str.Size(), allocator);
    }

    void String::Insert(const Int pos, const char* data, const Int size){
        if (pos < 0 || pos > this->_size)
            throw std::out_of_range("String::Insert: Position out of range.");

        const auto newSize = this->_size + size;

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

    String String::Normalize(const StringView& str, const ::Memory::IAllocator* allocator){
        const auto size = str.Size();
        const auto* data = str.Data();

        auto* newStr = static_cast<char*>(allocator->AllocateRaw(size));
        for (Int i = 0; i < size; i++)
            newStr[i] = std::tolower(data[i]);

        return String(newStr, size, allocator);
    }

    String String::Lower(const StringView& str, const Memory::IAllocator* allocator){
        const auto size = str.Size();
        const auto* data = str.Data();

        auto* newStr = static_cast<char*>(allocator->AllocateRaw(size));
        for (Int i = 0; i < size; i++)
            newStr[i] = std::tolower(data[i]);
        return String(newStr, size, allocator);
    }

    String String::Upper(const StringView& str, const Memory::IAllocator* allocator){
        const auto size = str.Size();
        auto* newStr = static_cast<char*>(allocator->AllocateRaw(size));
        for (Int i = 0; i < size; i++)
            newStr[i] = std::toupper(str[i]);
        return String(newStr, size, allocator);
    }

    StringView String::Trim(const StringView& str){
        const auto size = str.Size();
        const auto* data = str.Data();

        if (size == 0)
            return StringView(data, 0);

        int firstIndex = 0;
        int lastIndex = size - 1;

        for (Int i = 0; i < size; i++){
            if (!::isspace(data[i])){
                firstIndex = i;
                break;
            }
        }

        for (Int i = size - 1; i >= 0; i--){
            if (!::isspace(data[i])){
                lastIndex = i;
                break;
            }
        }

        return StringView(data + firstIndex, size - lastIndex);
    }

    Int String::Ascii(const StringView& str){
        return str.Empty() ? 0 : str[0];
    }

    StringView String::TrimLeft(const StringView& str){
        const auto size = str.Size();
        const auto* data = str.Data();

        if (size == 0)
            return StringView(data, 0);

        int firstIndex = 0;

        for(int i = 0;i < size; i++)
            if(!isspace(data[i]))
            {
                firstIndex = i;
                break;
            }
        return StringView(data + firstIndex, size - firstIndex);
    }

    StringView String::TrimRight(const StringView& str){
        const auto size = str.Size();
        const auto* data = str.Data();

        if (size == 0)
            return StringView(data, 0);

        int lastIndex = size - 1;
        for(int i = size - 1; i >= 0; i--)
            if(!isspace(data[i]))
            {
                lastIndex = i;
                break;
            }

        return StringView(data, lastIndex + 1);
    }

    String String::Replace(
        const StringView& str,
        const StringView& subStr,
        const StringView& newStr,
        const ::Memory::IAllocator* allocator
    ){
        const auto size = str.Size();
        const auto* data = str.Data();

        if (size == 0 || subStr.Size() == 0){
            auto* strCopy = static_cast<char*>(allocator->AllocateRaw(size));
            std::memcpy(strCopy, data, size);
            return String(strCopy, size, allocator);
        }

        Int count = 0;
        const auto subStrSize = subStr.Size();
        const auto* subStrData = subStr.Data();

        for (Int i = 0; i <= size - subStrSize; ){
            if (std::memcmp(data + i, subStrData, subStrSize) == 0){
                count += 1;
                i += subStrSize;
            } else {
                i++;
            }
        }

        if (count == 0){
            auto* strCopy = static_cast<char*>(allocator->AllocateRaw(size));
            std::memcpy(strCopy, data, size);
            return String(strCopy, size, allocator);
        }

        const auto newSize = size - subStrSize * count + newStr.Size() * count;
        auto* newStrCopy = static_cast<char*>(allocator->AllocateRaw(newSize));

        Int i = 0, j = 0;
        const auto newStrSize = newStr.Size();
        const auto* newStrData = newStr.Data();
        while (j < size && i < newSize){
            if (j + subStrSize <= size && std::memcmp(data + j, newStrData, subStrSize) == 0){
                std::memcpy(newStrCopy + i, newStrData, newStrSize);
                i += newStrSize;
                j += subStrSize;
            } else {
                newStrCopy[i++] = str[j++];
            }
        }

        return String(newStrCopy, newSize, allocator);
    }

    StringView String::SubString(const StringView& str, const Int start, const Int end){
        const auto size = str.Size();

        if (start < 0 || start >= size)
            throw std::out_of_range("String::SubString: Index out of range.");
        if (end < 0 || end > size || start > end)
            throw std::out_of_range("String::SubString: Size out of range.");

        return StringView(str.Data() + start, end - start);
    }

    String String::Reverse(const StringView& str, const ::Memory::IAllocator* allocator){
        const auto size = str.Size();
        const auto* data = str.Data();

        if (size == 0)
            return String::Empty(allocator);

        auto* newStr = static_cast<char*>(allocator->AllocateRaw(size));
        for (Int i = 0; i < size / 2; i++){
            newStr[i] = data[size - i - 1];
            newStr[size - i - 1] = data[i];
        }
        if (size % 2 != 0)
            newStr[size / 2] = data[size / 2];
        return String(newStr, size, allocator);
    }

    StringView String::Left(const StringView& str, const Int count){
        const auto size = str.Size();

        if (size == 0)
            throw std::out_of_range("String::Left: Size out of range.");

        if (count > size)
            throw std::out_of_range("String::Left: Count out of range.");

        return StringView(str.Data(), count);
    }

    StringView String::Right(const StringView& str, const Int count){
        const auto size = str.Size();

        if (size == 0)
            throw std::out_of_range("String::Right: Size out of range.");

        if (count > size)
            throw std::out_of_range("String::Right: Count out of range.");

        return StringView(str.Data() + (size - count), count);
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

    String String::Repeat(
        const StringView& str,
        const Int count,
        const Memory::IAllocator* allocator
    ){
        const auto size = str.Size();
        const auto* data = str.Data();

        const auto totalSize = size * count;
        if (totalSize == 0)
            return String::Empty(allocator);

        auto* newStr = static_cast<char*>(allocator->AllocateRaw(totalSize));
        for (Int i = 0; i < count; i++)
            std::memcpy(newStr + i * size, data, size);
        return String(newStr, totalSize, allocator);
    }

    String String::Space(const Int count, const Memory::IAllocator* allocator){
        auto* buf = static_cast<char*>(allocator->AllocateRaw(count));
        std::memset(buf, ' ', count);
        return String(buf, count, allocator);
    }

    StringView String::Split(
        const StringView& str,
        const Int startIndex,
        const char delimiter
    ){
        const auto size = str.Size();
        const auto* data = str.Data();

        if (size == 0)
            return StringView(data, 0);

        for (int i = startIndex; i < size; i++){
            if (strncasecmp(&data[i], &delimiter, 1) == 0)
                return StringView(data + startIndex, i - startIndex);
        }

        return StringView(data + startIndex, size - startIndex);
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
}
