#pragma once
#include <cstring>
#include <span>
#include "DataTypes.h"
#include <ostream>

namespace Memory{
    class IAllocator;
}

namespace DataTypes{
    class String;

    class StringView{
        const char* _data;
        Int _size;

        [[nodiscard]] constexpr bool Equals(const char* other, const Int size) const{
            return this->_size == size
                && std::memcmp(this->_data, other, size) == 0;
        }
        [[nodiscard]] inline bool EqualsIgnoreCase(const char* other, Int size) const;
        [[nodiscard]] inline bool StartsWith(const char* other, Int size) const;
        [[nodiscard]] inline bool StartsWithIgnoreCase(const char* other, Int size) const;
        [[nodiscard]] inline bool EndsWith(const char* other, Int size) const;
        [[nodiscard]] inline bool EndsWithIgnoreCase(const char* other, Int size) const;
        [[nodiscard]] inline bool Contains(const char* other, Int size) const;
        [[nodiscard]] inline bool ContainsIgnoreCase(const char* other, Int size) const;
        public:
        /**
             *
             * @param data Non-owning pointer of the actual data
             * @param size The size of the string view in bytes (not including null terminator, if any).
             * The string view can contain null characters within it and is not required to be null-terminated.
        */
        StringView(const char* data, Int size);

        constexpr StringView(const StringView& other)noexcept{
            this->_data = other._data;
            this->_size = other._size;
        }

        StringView(StringView&& other) noexcept;

        constexpr StringView()
            : _data(nullptr), _size(0){}

        constexpr StringView(const char* other)
            : _data(other), _size(StringView::CalculateSize(other)){}

        constexpr StringView& operator=(const char* other){
            this->_data = other;
            this->_size = StringView::CalculateSize(other);
            return *this;
        }

        explicit StringView(const std::string& other);

        StringView& operator=(StringView&& other) noexcept;
        constexpr StringView& operator=(const StringView& other)= default;
        constexpr ~StringView() = default;

        [[nodiscard]] constexpr const char* Data() const noexcept{ return this->_data; }

        //operators
        friend std::ostream& operator<<(std::ostream& os, const StringView& sv);
        [[nodiscard]] constexpr char operator[](const Int index) const{
            return this->_data[index];
        }

        //Equality Operators
        [[nodiscard]] constexpr bool operator==(const char* other) const{
            return this->Equals(other, this->_size);
        }

        [[nodiscard]] constexpr bool operator==(const StringView& other) const{
            return this->Equals(other._data, other._size);
        }

        [[nodiscard]] constexpr bool operator!=(const char* other) const{
            return !(*this == other);
        }

        [[nodiscard]] constexpr bool operator!=(const StringView& other) const{
            return !(*this == other);
        }

        //Comparison Operators
        [[nodiscard]] constexpr bool operator<(const char* other) const {
            const auto otherSize = StringView::CalculateSize(other);

            const auto size = std::min(this->_size, otherSize);
            const auto cmp = std::memcmp(this->_data, other, size);
            if (cmp != 0) return cmp < 0;

            return this->_size < otherSize;
        }

        [[nodiscard]] constexpr bool operator<(const StringView& other) const noexcept{
            const auto size = std::min(this->_size, other._size);
            const auto cmp = std::memcmp(this->_data, other._data, size);
            if (cmp != 0) return cmp < 0;

            return this->_size < other._size;
        }

        [[nodiscard]] constexpr bool operator<(const std::string& other) const {
            const auto otherSize = static_cast<Int>(other.size());
            const auto size = std::min(this->_size, otherSize);
            const auto cmp = std::memcmp(this->_data, other.data(), size);
            if (cmp != 0) return cmp < 0;

            return this->_size < otherSize;
        }

        [[nodiscard]] constexpr bool operator<(const std::string_view& other) const {
            const auto otherSize = static_cast<Int>(other.size());
            const auto size = std::min(this->_size, otherSize);
            const auto cmp = std::memcmp(this->_data, other.data(), size);
            if (cmp != 0) return cmp < 0;

            return this->_size < otherSize;
        }

        [[nodiscard]] constexpr bool operator<=(const char* other) const {
            return !(*this > other);
        }

        [[nodiscard]] constexpr bool operator<=(const StringView& other) const {
            return !(*this > other);
        }

        [[nodiscard]] constexpr bool operator<=(const std::string& other) const {
            return !(*this > other);
        }

        [[nodiscard]] constexpr bool operator<=(const std::string_view& other) const {
            return !(*this > other);
        }

        [[nodiscard]] constexpr bool operator>(const StringView& other) const noexcept{
            const auto size = std::min(this->_size, other._size);
            const auto cmp = std::memcmp(this->_data, other._data, size);
            if (cmp != 0) return cmp > 0;

            return this->_size > other._size;
        }

        [[nodiscard]] constexpr bool operator>(const char* other) const {
            const auto otherSize = StringView::CalculateSize(other);

            const auto size = std::min(this->_size, otherSize);
            const auto cmp = std::memcmp(this->_data, other, size);
            if (cmp != 0) return cmp > 0;

            return this->_size > otherSize;
        }

        [[nodiscard]] constexpr bool operator>(const std::string& other) const {
            const auto otherSize = static_cast<Int>(other.size());
            const auto size = std::min(this->_size, otherSize);
            const auto cmp = std::memcmp(this->_data, other.data(), size);
            if (cmp != 0) return cmp > 0;

            return this->_size > otherSize;
        }

        [[nodiscard]] constexpr bool operator>(const std::string_view& other) const {
            const auto otherSize = static_cast<Int>(other.size());
            const auto size = std::min(this->_size, otherSize);
            const auto cmp = std::memcmp(this->_data, other.data(), size);
            if (cmp != 0) return cmp > 0;

            return this->_size > otherSize;
        }

        [[nodiscard]] constexpr bool operator>=(const char* other) const {
            return !(*this < other);
        }

        [[nodiscard]] constexpr bool operator>=(const StringView& other) const {
            return !(*this < other);
        }

        [[nodiscard]] constexpr bool operator>=(const std::string& other) const {
            return !(*this < other);
        }

        [[nodiscard]] constexpr bool operator>=(const std::string_view& other) const {
            return !(*this < other);
        }

        operator std::string_view() const;
        operator std::span<const char>() const;

        //functions
        [[nodiscard]] StringView Substring(Int startIndex, Int length) const;

        [[nodiscard]] constexpr Int Size()const{ return this->_size; }

        [[nodiscard]] Int IndexOf(char c) const;
        [[nodiscard]] bool Contains(const StringView& other, StringComparisonType type) const;
        [[nodiscard]] bool Contains(const String& other, StringComparisonType type) const;
        [[nodiscard]] bool Contains(const char* other, StringComparisonType type) const;
        [[nodiscard]] bool Contains(std::string_view other, StringComparisonType type) const;
        [[nodiscard]] bool Contains(const std::string& other, StringComparisonType type) const;
        [[nodiscard]] bool Empty() const;

        // STL compatibility
        using const_iterator = const char*;

        [[nodiscard]] constexpr const_iterator begin() const{
            return this->_data;
        }
        
        [[nodiscard]] constexpr const_iterator end() const{
            return this->_data + this->_size;
        }

        static constexpr Int CalculateSize(const char* str){
            if (!str) return 0;

            Int size = 0;
            while (str[size] != '\0') size++;
            return size;
        }
    };
}
