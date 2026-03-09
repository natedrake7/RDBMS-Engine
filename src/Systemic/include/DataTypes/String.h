#pragma once
#include <span>

#include "StringView.h"
#include "DataTypes.h"

#include "../Memory/IAllocator.h"

namespace DataTypes{
    class String{
        const ::Memory::IAllocator* _allocator;
        char* _data;
        Int _size;
        Int _capacity;

        void CalculateCapacity(Int size);
        [[nodiscard]] bool CanFit(Int size) const;

        [[nodiscard]] inline bool Equals(const char* other, Int size) const;
        [[nodiscard]] inline bool EqualsIgnoreCase(const char* other, Int size) const;
        [[nodiscard]] inline bool StartsWith(const char* other, Int size) const;
        [[nodiscard]] inline bool StartsWithIgnoreCase(const char* other, Int size) const;
        [[nodiscard]] inline bool EndsWith(const char* other, Int size) const;
        [[nodiscard]] inline bool EndsWithIgnoreCase(const char* other, Int size) const;
        [[nodiscard]] inline bool Contains(const char* other, Int size) const;
        [[nodiscard]] inline bool ContainsIgnoreCase(const char* other, Int size) const;

        [[nodiscard]] inline String& Append(
            const char* data,
            Int size
        );

        [[nodiscard]] static inline String Normalize(
            const char* str,
            Int size,
            const ::Memory::IAllocator* allocator
        );

        [[nodiscard]] static inline String Lower(
            const char* str,
            Int size,
            const ::Memory::IAllocator* allocator
        );

        [[nodiscard]] static inline String Upper(
            const char* str,
            Int size,
            const ::Memory::IAllocator* allocator
        );

        [[nodiscard]] static inline StringView Trim(
            const char* str,
            Int size
        );

        [[nodiscard]] static inline StringView TrimLeft(
            const char* str,
            Int size
        );

        [[nodiscard]] static inline StringView TrimRight(
            const char* str,
            Int size
        );

        [[nodiscard]] static inline String Replace(
            const char* str,
            Int size,
            const StringView& subStr,
            const StringView& newStr,
            const ::Memory::IAllocator* allocator
        );

        [[nodiscard]] static inline StringView SubString(
            const char* str,
            Int size,
            Int start,
            Int end
        );

        [[nodiscard]] static inline String Reverse(
            const char* str,
            Int size,
            const ::Memory::IAllocator* allocator
        );

        static inline StringView Left(
            const char* str,
            Int size,
            Int count
        );

        static inline StringView Right(
            const char* str,
            Int size,
            Int count
        );

        static inline String Repeat(
            const char* str,
            Int size,
            Int count,
            const ::Memory::IAllocator* allocator
        );

        public:
            String();
            String(const ::Memory::IAllocator* allocator);
            String(const ::Memory::IAllocator* allocator, Int size);
            String(const String& str, const ::Memory::IAllocator* allocator);
            String(const StringView& str, const ::Memory::IAllocator* allocator);
            String(const object_t* str, Int size, const ::Memory::IAllocator* allocator);
            String(const char* str, const ::Memory::IAllocator* allocator);
            String(const std::string& str, const ::Memory::IAllocator* allocator);
            String(char* str, Int size, const ::Memory::IAllocator* allocator);
            String(const char* str, Int size, const ::Memory::IAllocator* allocator);
            String(const String& other);
            String& operator=(const String& other);
            String(String&& other) noexcept;
            String& operator=(String&& other) noexcept;

            static String Empty(const ::Memory::IAllocator* allocator);
            static String Null();

            void SetAllocator(const ::Memory::IAllocator* allocator);

            void Reserve(Int size);

            ~String();

            friend std::ostream& operator<<(std::ostream& os, const String& sv);
            [[nodiscard]] char operator[](Int index) const;
            [[nodiscard]] char& operator[](Int index);
            bool operator==(const String& other) const;
            bool operator!=(const String& other) const;

            operator std::string_view() const;
            operator std::span<const char>() const;

            friend String operator+(const String& lhs, const String& rhs);
            friend String operator+(const String& lhs, const char* other);
            friend String operator+(const char* lhs, const String& rhs);
            friend String operator+(const String& lhs, const StringView& rhs);
            friend String operator+(const StringView& lhs, const String& rhs);
            friend String operator+(const String& lhs, std::string_view rhs);
            friend String operator+(std::string_view lhs, const String& rhs);
            friend String operator+(const String& lhs, const std::string& rhs);
            friend String operator+(const std::string& lhs, const String& rhs);

            String& operator+=(const String& other);
            String& operator+=(const char* other);
            String& operator+=(const StringView& other);
            String& operator+=(std::string_view other);
            String& operator+=(const std::string& other);

            [[nodiscard]] const char* Data()const;

            [[nodiscard]] StringView ToView()const;
            [[nodiscard]] static String FromView(const StringView& str, const ::Memory::IAllocator* allocator);

            //functions
            template<typename... Args>
            [[nodiscard]] static String Concat(const ::Memory::IAllocator* allocator, const Args&... args);

            template<typename... Args>
            [[nodiscard]] String ConcatInPlace(const Args&... args) const;
            [[nodiscard]] String Concat(const String& other) const;
            [[nodiscard]] String Concat(const char* other) const;
            [[nodiscard]] String Concat(const StringView& other) const;
            [[nodiscard]] String Concat(std::string_view other) const;
            [[nodiscard]] String Concat(const std::string& other) const;
            [[nodiscard]] static String Concat(const StringView& lhs, const StringView& rhs, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static String Concat(const String& lhs, const String& rhs, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static String Concat(const char* lhs, const char* rhs, const ::Memory::IAllocator* allocator);

            template<typename... Args>
            [[nodiscard]] static String Join(
                const Memory::IAllocator* allocator,
                char delimiter,
                const Args&... args
            );

            String& Append(const String& other);
            String& Append(const StringView& other);
            String& Append(const char* other);
            String& Append(std::string_view other);
            String& Append(const std::string& other);

            void Insert(Int pos, const char* data, Int size);

            [[nodiscard]] Int Size()const;
            [[nodiscard]] Int IndexOf(char c) const;
            [[nodiscard]] bool Contains(const String& other, StringComparisonType type) const;
            [[nodiscard]] bool Contains(const char* other, StringComparisonType type) const;
            [[nodiscard]] bool Contains(const StringView& other, StringComparisonType type) const;
            [[nodiscard]] bool Contains(std::string_view other, StringComparisonType type) const;
            [[nodiscard]] bool Contains(const std::string& other, StringComparisonType type) const;
            [[nodiscard]] bool Empty() const;

            [[nodiscard]] String ToLower() const;
            [[nodiscard]] String ToUpper() const;
            void ToLowerInPlace() const;
            void ToUpperInPlace() const;

            /**
             *
             * @param str string to modify
             * @return returns a normalized (lower cased) version of the input string. The original string is not modified.
             * Normalization is done by converting all characters to lower case. This is useful for case-insensitive comparisons and operations,
             * ensuring that strings are treated uniformly regardless of their original case.
             */
            [[nodiscard]] static String Normalize(const String& str);
            [[nodiscard]] static String Normalize(const char* str, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static String Normalize(const StringView& str, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static String Normalize(std::string_view str, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static String Normalize(const std::string& str, const ::Memory::IAllocator* allocator);

            [[nodiscard]] static String Lower(const String& str);
            [[nodiscard]] static String Lower(const char* str, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static String Lower(const StringView& str, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static String Lower(std::string_view str, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static String Lower(const std::string& str, const ::Memory::IAllocator* allocator);

            [[nodiscard]] static String Upper(const String& str);
            [[nodiscard]] static String Upper(const char* str, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static String Upper(const StringView& str, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static String Upper(std::string_view str, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static String Upper(const std::string& str, const ::Memory::IAllocator* allocator);

            [[nodiscard]] static StringView Trim(const String& str);
            [[nodiscard]] static StringView Trim(const char* str);
            [[nodiscard]] static StringView Trim(const StringView& str);
            [[nodiscard]] static StringView Trim(std::string_view str);
            [[nodiscard]] static StringView Trim(const std::string& str);

            [[nodiscard]] static Int Ascii(const String& str);
            [[nodiscard]] static Int Ascii(const char* str);
            [[nodiscard]] static Int Ascii(const StringView& str);
            [[nodiscard]] static Int Ascii(std::string_view str);
            [[nodiscard]] static Int Ascii(const std::string& str);

            [[nodiscard]] static Int Length(const String& str);
            [[nodiscard]] static Int Length(const char* str);
            [[nodiscard]] static Int Length(const StringView& str);
            [[nodiscard]] static Int Length(std::string_view str);
            [[nodiscard]] static Int Length(const std::string& str);

            [[nodiscard]] static StringView TrimLeft(const String& str);
            [[nodiscard]] static StringView TrimLeft(const char* str);
            [[nodiscard]] static StringView TrimLeft(const StringView& str);
            [[nodiscard]] static StringView TrimLeft(std::string_view str);
            [[nodiscard]] static StringView TrimLeft(const std::string& str);

            [[nodiscard]] static StringView TrimRight(const String& str);
            [[nodiscard]] static StringView TrimRight(const char* str);
            [[nodiscard]] static StringView TrimRight(const StringView& str);
            [[nodiscard]] static StringView TrimRight(std::string_view str);
            [[nodiscard]] static StringView TrimRight(const std::string& str);

            [[nodiscard]] static String Replace(
                const String& str,
                const StringView& oldStr,
                const StringView& newStr
            );
            [[nodiscard]] static String Replace(
                const StringView& str,
                const StringView& oldStr,
                const StringView& newStr,
                const ::Memory::IAllocator* allocator
            );
            [[nodiscard]] static String Replace(
                const char* str,
                const StringView& oldStr,
                const StringView& newStr,
                const ::Memory::IAllocator* allocator
            );
            [[nodiscard]] static String Replace(
                std::string_view str,
                const StringView& oldStr,
                const StringView& newStr,
                const ::Memory::IAllocator* allocator
            );
            [[nodiscard]] static String Replace(
                const std::string& str,
                const StringView& oldStr,
                const StringView& newStr,
                const ::Memory::IAllocator* allocator
            );

            [[nodiscard]] static StringView SubString(const String& str, Int start, Int end);
            [[nodiscard]] static StringView SubString(const StringView& str, Int start, Int end);
            [[nodiscard]] static StringView SubString(const char* str, Int start, Int end);
            [[nodiscard]] static StringView SubString(std::string_view str, Int start, Int end);
            [[nodiscard]] static StringView SubString(const std::string& str, Int start, Int end);

            [[nodiscard]] static String Reverse(const String& str);
            [[nodiscard]] static String Reverse(const StringView& str, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static String Reverse(const char* str, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static String Reverse(std::string_view str, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static String Reverse(const std::string& str, const ::Memory::IAllocator* allocator);

            [[nodiscard]] static StringView Left(const String& str, Int count);
            [[nodiscard]] static StringView Left(const StringView& str, Int count);
            [[nodiscard]] static StringView Left(const char* str, Int count);
            [[nodiscard]] static StringView Left(std::string_view str, Int count);
            [[nodiscard]] static StringView Left(const std::string& str, Int count);

            [[nodiscard]] static StringView Right(const String& str, Int count);
            [[nodiscard]] static StringView Right(const StringView& str, Int count);
            [[nodiscard]] static StringView Right(const char* str, Int count);
            [[nodiscard]] static StringView Right(std::string_view str, Int count);
            [[nodiscard]] static StringView Right(const std::string& str, Int count);

            [[nodiscard]] static String Char(Int AsciiCode, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static Int CharIndex(const StringView& subStr, const StringView& str, Int start);

            [[nodiscard]] static String Repeat(const String& str, Int count);
            [[nodiscard]] static String Repeat(const StringView& str, Int count, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static String Repeat(const char* str, Int count, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static String Repeat(std::string_view str, Int count, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static String Repeat(const std::string& str, Int count, const ::Memory::IAllocator* allocator);

            [[nodiscard]] static String Space(Int count, const ::Memory::IAllocator* allocator);

            // STL compatibility
            using const_iterator = const char*;
            using reverse_iterator = std::reverse_iterator<const_iterator>;

            [[nodiscard]] const_iterator begin() const;
            [[nodiscard]] const_iterator end() const;

            [[nodiscard]] reverse_iterator rbegin() const;
            [[nodiscard]] reverse_iterator rend() const;
    };

    template <typename...Args>
    String String::Concat(const Memory::IAllocator* allocator, const Args&... args){
        // Calculate total size first
        Int totalSize = 0;
        ([&]<typename Type>(const Type& arg) {
            if constexpr (std::is_same_v<std::decay_t<Type>, String>)
                totalSize += arg.Size();
            else if constexpr (std::is_same_v<std::decay_t<Type>, StringView>)
                totalSize += arg.Size();
            else if constexpr (std::is_same_v<std::decay_t<Type>, std::string>)
                totalSize += static_cast<Int>(arg.size());
            else if constexpr (std::is_same_v<std::decay_t<Type>, std::string_view>)
                totalSize += static_cast<Int>(arg.size());
            else if constexpr (std::is_same_v<std::decay_t<Type>, char*> ||
                               std::is_same_v<std::decay_t<Type>, const char*>)
                totalSize += static_cast<Int>(std::strlen(arg));
        }(args), ...);

        // Allocate and copy
        auto* buf = static_cast<char*>(allocator->AllocateRaw(totalSize));
        Int offset = 0;
        ([&]<typename Type>(const Type& arg) {
            if constexpr (std::is_same_v<std::decay_t<Type>, String>) {
                std::memcpy(buf + offset, arg.Data(), arg.Size());
                offset += arg.Size();
            } else if constexpr (std::is_same_v<std::decay_t<Type>, StringView>) {
                std::memcpy(buf + offset, arg.Data(), arg.Size());
                offset += arg.Size();
            } else if constexpr (std::is_same_v<std::decay_t<Type>, std::string>) {
                std::memcpy(buf + offset, arg.data(), arg.size());
                offset += static_cast<Int>(arg.size());
            } else if constexpr (std::is_same_v<std::decay_t<Type>, std::string_view>) {
                std::memcpy(buf + offset, arg.data(), arg.size());
                offset += static_cast<Int>(arg.size());
            } else if constexpr (std::is_same_v<std::decay_t<Type>, char*> ||
                                 std::is_same_v<std::decay_t<Type>, const char*>) {
                const auto len = static_cast<Int>(std::strlen(arg));
                std::memcpy(buf + offset, arg, len);
                offset += len;
            }
        }(args), ...);

        return String(buf, totalSize, allocator);
    }

template<typename... Args>
String String::ConcatInPlace(const Args&... args) const {
    // Calculate total size first
    Int totalSize = this->_size;
    ([&]<typename Type>(const Type& arg) {
        if constexpr (std::is_same_v<std::decay_t<Type>, String>)
            totalSize += arg.Size();
        else if constexpr (std::is_same_v<std::decay_t<Type>, StringView>)
            totalSize += arg.Size();
        else if constexpr (std::is_same_v<std::decay_t<Type>, std::string>)
            totalSize += static_cast<Int>(arg.size());
        else if constexpr (std::is_same_v<std::decay_t<Type>, std::string_view>)
            totalSize += static_cast<Int>(arg.size());
        else if constexpr (std::is_same_v<std::decay_t<Type>, char*> ||
                           std::is_same_v<std::decay_t<Type>, const char*>)
            totalSize += static_cast<Int>(std::strlen(arg));
    }(args), ...);

    // Allocate and copy
    auto* buf = static_cast<char*>(this->_allocator->AllocateRaw(totalSize));
    Int offset = 0;

    std::memcpy(buf, this->_data, this->_size);
    offset += this->_size;

    ([&]<typename Type>(const Type& arg) {
        if constexpr (std::is_same_v<std::decay_t<Type>, String>) {
            std::memcpy(buf + offset, arg.Data(), arg.Size());
            offset += arg.Size();
        } else if constexpr (std::is_same_v<std::decay_t<Type>, StringView>) {
            std::memcpy(buf + offset, arg.Data(), arg.Size());
            offset += arg.Size();
        } else if constexpr (std::is_same_v<std::decay_t<Type>, std::string>) {
            std::memcpy(buf + offset, arg.data(), arg.size());
            offset += static_cast<Int>(arg.size());
        } else if constexpr (std::is_same_v<std::decay_t<Type>, std::string_view>) {
            std::memcpy(buf + offset, arg.data(), arg.size());
            offset += static_cast<Int>(arg.size());
        } else if constexpr (std::is_same_v<std::decay_t<Type>, char*> ||
                             std::is_same_v<std::decay_t<Type>, const char*>) {
            const auto len = static_cast<Int>(std::strlen(arg));
            std::memcpy(buf + offset, arg, len);
            offset += len;
        }
    }(args), ...);

    return String(buf, totalSize, this->_allocator);
}

template <typename ... Args>
String String::Join(const Memory::IAllocator* allocator, const char delimiter, const Args&... args){
    Int totalSize = sizeof...(Args) > 1 ? static_cast<Int>(sizeof...(Args) - 1) : 0;
    ([&]<typename Type>(const Type& arg) {
        if constexpr (std::is_same_v<std::decay_t<Type>, String>)
            totalSize += arg.Size();
        else if constexpr (std::is_same_v<std::decay_t<Type>, StringView>)
            totalSize += arg.Size();
        else if constexpr (std::is_same_v<std::decay_t<Type>, std::string>)
            totalSize += static_cast<Int>(arg.size());
        else if constexpr (std::is_same_v<std::decay_t<Type>, std::string_view>)
            totalSize += static_cast<Int>(arg.size());
        else if constexpr (std::is_same_v<std::decay_t<Type>, char*> ||
                           std::is_same_v<std::decay_t<Type>, const char*>)
            totalSize += static_cast<Int>(std::strlen(arg));
    }(args), ...);

    // Allocate and copy
    Int offset = 0;
    auto* data = static_cast<char*>(allocator->AllocateRaw(totalSize));

    ([&]<typename Type>(const Type& arg) {
        if (offset != 0)
            data[offset++] = delimiter;

        if constexpr (std::is_same_v<std::decay_t<Type>, String>) {
            std::memcpy(data + offset, arg.Data(), arg.Size());
            offset += arg.Size();
        } else if constexpr (std::is_same_v<std::decay_t<Type>, StringView>) {
            std::memcpy(data + offset, arg.Data(), arg.Size());
            offset += arg.Size();
        } else if constexpr (std::is_same_v<std::decay_t<Type>, std::string>) {
            std::memcpy(data + offset, arg.data(), arg.size());
            offset += static_cast<Int>(arg.size());
        } else if constexpr (std::is_same_v<std::decay_t<Type>, std::string_view>) {
            std::memcpy(data + offset, arg.data(), arg.size());
            offset += static_cast<Int>(arg.size());
        } else if constexpr (std::is_same_v<std::decay_t<Type>, char*> ||
                             std::is_same_v<std::decay_t<Type>, const char*>) {
            const auto len = static_cast<Int>(std::strlen(arg));
            std::memcpy(data + offset, arg, len);
            offset += len;
        }
    }(args), ...);

    return String(data, totalSize, allocator);
}
}

template <>
struct std::hash<DataTypes::String> {
    size_t operator()(const DataTypes::String& str) const noexcept {
        // FNV-1a hash
        size_t hash = 14695981039346656037ULL;
        for (size_t i = 0; i < str.Size(); ++i) {
            hash ^= static_cast<size_t>(str.Data()[i]);
            hash *= 1099511628211ULL;
        }
        return hash;
    }
};