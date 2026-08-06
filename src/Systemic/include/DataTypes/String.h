#pragma once
#include <span>

#include "DataTypes.h"
#include "StringView.h"
#include "../Memory/IAllocator.h"

namespace DataTypes{
    class String{
        const ::Memory::IAllocator* _allocator;
        char* _data;
        Int _size;
        Int _capacity;

        void CalculateCapacity(Int size);
        [[nodiscard]] bool CanFit(Int size) const;

        public:
            String();
            String(const ::Memory::IAllocator* allocator);
            String(const ::Memory::IAllocator* allocator, Int size);

            template<IsStringLike T>
            String(const T& str, const ::Memory::IAllocator* allocator);
            String(const char* str, Int size, const ::Memory::IAllocator* allocator);

            String(const object_t* str, Int size, const ::Memory::IAllocator* allocator);
            String(object_t* str, Int size, const ::Memory::IAllocator* allocator);

            String(char* str, Int size, const ::Memory::IAllocator* allocator);

            String(const String& other);
            String& operator=(const String& other);
            String(String&& other) noexcept;
            String& operator=(String&& other) noexcept;

            static String Empty(const ::Memory::IAllocator* allocator);
            static String Null();

            void SetAllocator(const ::Memory::IAllocator* allocator);
            [[nodiscard]] const ::Memory::IAllocator* GetAllocator()const;

            void Reserve(Int size);
            void Resize(Int size);

            friend std::ostream& operator<<(std::ostream& os, const String& sv);
            [[nodiscard]] char operator[](Int index) const;
            [[nodiscard]] char& operator[](Int index);

            friend bool operator==(const String& lhs, const String& rhs);
            friend bool operator!=(const String& lhs, const String& rhs);
            friend bool operator<=(const String& lhs, const String& rhs);
            friend bool operator<(const String& lhs, const String& rhs);
            friend bool operator>=(const String& lhs, const String& rhs);
            friend bool operator>(const String& lhs, const String& rhs);

            operator std::string_view() const;
            operator std::span<const char>() const;

            template<IsStringLike TLeft, IsStringLike TRight>
            friend String operator+(const TLeft& lhs, const TRight& rhs){
                if constexpr (std::is_same_v<std::decay_t<TLeft>, String>){
                    const auto* allocator = lhs.Allocator();
                    return String::Concat(allocator, lhs, rhs);
                }
                else if constexpr (std::is_same_v<std::decay_t<TRight>, String>){
                    const auto* allocator = rhs.Allocator();
                    return String::Concat(allocator, rhs, lhs);
                }
                else
                    static_assert(DataTypes::AlwaysFalse<TLeft, TRight>, "Unsupported types");

                return String::Null();
            }

            template<IsStringLike T>
            String& operator+=(const T& other){
                return this->Append(other);
            }

            [[nodiscard]] char* Data();
            [[nodiscard]] const char* Data()const;

            [[nodiscard]] static String FromView(const StringView& str, const ::Memory::IAllocator* allocator);

            //functions
            template<typename... Args>
            [[nodiscard]] static String Concat(const ::Memory::IAllocator* allocator, const Args&... args);

            template<typename... Args>
            [[nodiscard]] String ConcatInPlace(const Args&... args) const;

            template<typename... Args>
            [[nodiscard]] static String Join(
                const Memory::IAllocator* allocator,
                char delimiter,
                const Args&... args
            );

            template<IsStringLike T>
            String& Append(const T& other){
                return this->Append(StringView::ViewOf(other));
            }
            String& Append(const char* other, const Int size){
                return this->Append(StringView(other, size));
            }

            String& Append(const StringView& other);

            void Insert(Int pos, const char* data, Int size);

            [[nodiscard]] Int Size()const;
            [[nodiscard]] Int IndexOf(char c) const;

            [[nodiscard]] bool Empty() const;
            [[nodiscard]] String ToLower() const;
            [[nodiscard]] String ToUpper() const;
            void ToLowerInPlace() const;
            void ToUpperInPlace() const;

            /**
             *
             * @param str string to modify
             * @param allocator local memory arena allocator
             * @return returns a normalized (lower cased) version of the input string. The original string is not modified.
             * Normalization is done by converting all characters to lower case. This is useful for case-insensitive comparisons and operations,
             * ensuring that strings are treated uniformly regardless of their original case.
             */

            [[nodiscard]] static String Normalize(const StringView& str, const ::Memory::IAllocator* allocator);
            template<IsStringLike T>
            [[nodiscard]] static String Normalize(const T& str, const ::Memory::IAllocator* allocator){
                return String::Normalize(StringView::ViewOf(str), allocator);
            }

            [[nodiscard]] static String Lower(const StringView& str, const ::Memory::IAllocator* allocator);
            template<IsStringLike T>
            [[nodiscard]] static String Lower(const T& str, const ::Memory::IAllocator* allocator){
                return String::Lower(StringView::ViewOf(str), allocator);
            }

            [[nodiscard]] static String Upper(const StringView& str, const ::Memory::IAllocator* allocator);
            template<IsStringLike T>
            [[nodiscard]] static String Upper(const T& str, const ::Memory::IAllocator* allocator){
                return String::Upper(StringView::ViewOf(str), allocator);
            }

            [[nodiscard]] static StringView Trim(const StringView& str);
            template<IsStringLike T>
            [[nodiscard]] static StringView Trim(const T& str){
                return String::Trim(StringView::ViewOf(str));
            }

            [[nodiscard]] static Int Ascii(const StringView& str);
            template<IsStringLike T>
            [[nodiscard]] static Int Ascii(const T& str){
                return String::Ascii(StringView::ViewOf(str));
            }

            template<IsStringLike T>
            [[nodiscard]] static Int Length(const T& str){
                return StringView::ViewOf(str).Size();
            }

            [[nodiscard]] static StringView TrimLeft(const StringView& str);
            template<IsStringLike T>
            [[nodiscard]] static StringView TrimLeft(const T& str){
                return String::TrimLeft(StringView::ViewOf(str));
            }

            [[nodiscard]] static StringView TrimRight(const StringView& str);
            template<IsStringLike T>
            [[nodiscard]] static StringView TrimRight(const T& str){
                return String::TrimRight(StringView::ViewOf(str));
            }

            [[nodiscard]] static String Replace(
                const StringView& str,
                const StringView& subStr,
                const StringView& newStr,
                const ::Memory::IAllocator* allocator
            );
            template<IsStringLike TStr, IsStringLike TSubStr, IsStringLike TNewStr>
            [[nodiscard]] static String Replace(
                const TStr& str,
                const TSubStr& oldStr,
                const TNewStr& newStr,
                const ::Memory::IAllocator* allocator
            ){
                return String::Replace(
                    StringView::ViewOf(str),
                    StringView::ViewOf(oldStr),
                    StringView::ViewOf(newStr),
                    allocator
                );
            }

            [[nodiscard]] static StringView SubString(const StringView& str, Int start, Int end);
            template<IsStringLike T>
            [[nodiscard]] static StringView SubString(const T& str, const Int start, const Int end){
                return String::SubString(StringView::ViewOf(str), start, end);
            }

            [[nodiscard]] static String Reverse(const StringView& str, const ::Memory::IAllocator* allocator);
            template<IsStringLike T>
            [[nodiscard]] static String Reverse(const T& str, const ::Memory::IAllocator* allocator){
                return String::Reverse(StringView::ViewOf(str), allocator);
            }

            [[nodiscard]] static StringView Left(const StringView& str, Int count);
            template<IsStringLike T>
            [[nodiscard]] static StringView Left(const T& str, const Int count){
                return String::Left(StringView::ViewOf(str), count);
            }

            [[nodiscard]] static StringView Right(const StringView& str, Int count);
            template<IsStringLike T>
            [[nodiscard]] static StringView Right(const T& str, const Int count){
                return String::Right(StringView::ViewOf(str), count);
            }

            [[nodiscard]] static String Char(Int AsciiCode, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static Int CharIndex(const StringView& subStr, const StringView& str, Int start);

            [[nodiscard]] static String Repeat(const StringView& str, Int count, const ::Memory::IAllocator* allocator);
            template<IsStringLike T>
            [[nodiscard]] static String Repeat(
                const T& str,
                const Int count,
                const ::Memory::IAllocator* allocator
            ){
                return String::Repeat(StringView::ViewOf(str), count, allocator);
            }

            [[nodiscard]] static String Space(Int count, const ::Memory::IAllocator* allocator);

            template<IsStringLike T>
            [[nodiscard]] static StringView Split(const T& str, Int startIndex, char delimiter){
                const auto strView = StringView::ViewOf(str);
                return String::Split(strView, startIndex, delimiter);
            }

            static StringView Split(const StringView& str, Int startIndex, char delimiter);

            // STL compatibility
            using const_iterator = const char*;
            using reverse_iterator = std::reverse_iterator<const_iterator>;

            [[nodiscard]] const_iterator begin() const;
            [[nodiscard]] const_iterator end() const;

            [[nodiscard]] reverse_iterator rbegin() const;
            [[nodiscard]] reverse_iterator rend() const;

            [[nodiscard]] char First()const;
            [[nodiscard]] char Last()const;

            void Insert(Int index, char c);
            void Pop();

            //Constant Evaluation Functions
            [[nodiscard]] static constexpr char ToLower(char c) noexcept;

    };

    constexpr char String::ToLower(const char c) noexcept{
        return (c >= 'A' && c <= 'Z') ? static_cast<char>(c + 32) : c;
    }

    template <IsStringLike T>
    String::String(const T& str, const Memory::IAllocator* allocator){
        const auto view = StringView::ViewOf(str);
        this->_allocator = allocator;
        this->_size = view.Size();
        this->_capacity = this->_size;

        this->_data = static_cast<char*>(allocator->AllocateRaw(this->_size));
        std::memcpy(this->_data, view.Data(), this->_size);
    }

    template <typename...Args>
    String String::Concat(const Memory::IAllocator* allocator, const Args&... args){
        // Calculate total size first
        Int totalSize = 0;
        ([&]<typename Type>(const Type& arg) {
            if constexpr (
                std::is_same_v<std::decay_t<Type>, String>
                || std::is_same_v<std::decay_t<Type>, StringView>
                || std::is_same_v<std::decay_t<Type>, StringValue>
            ) totalSize += arg.Size();
            else if constexpr (std::is_same_v<std::decay_t<Type>, StringView>)
                totalSize += arg.Size();
            else if constexpr (std::is_same_v<std::decay_t<Type>, std::string>)
                totalSize += static_cast<Int>(arg.size());
            else if constexpr (std::is_same_v<std::decay_t<Type>, std::string_view>)
                totalSize += static_cast<Int>(arg.size());
            else if constexpr (std::is_same_v<std::decay_t<Type>, char*> ||
                               std::is_same_v<std::decay_t<Type>, const char*>
            ) totalSize += static_cast<Int>(std::strlen(arg));
            else
                static_assert(DataTypes::AlwaysFalse<Type>, "Unsupported type");
        }(args), ...);

        // Allocate and copy
        auto* buf = static_cast<char*>(allocator->AllocateRaw(totalSize));
        Int offset = 0;
        ([&]<typename Type>(const Type& arg) {
            if constexpr (std::is_same_v<std::decay_t<Type>, String>
                || std::is_same_v<std::decay_t<Type>, StringView>
                || std::is_same_v<std::decay_t<Type>, StringValue>
            ) {
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
            else
                static_assert(DataTypes::AlwaysFalse<Type>, "Unsupported type");
        }(args), ...);

        return String(buf, totalSize, allocator);
    }

template<typename... Args>
String String::ConcatInPlace(const Args&... args) const {
    // Calculate total size first
    Int totalSize = this->_size;
    ([&]<typename Type>(const Type& arg) {
        if constexpr (
            std::is_same_v<std::decay_t<Type>, String>
            || std::is_same_v<std::decay_t<Type>, StringView>
        ) totalSize += arg.Size();
        else if constexpr (
            std::is_same_v<std::decay_t<Type>, std::string>
            || std::is_same_v<std::decay_t<Type>, std::string_view>
        ) totalSize += static_cast<Int>(arg.size());
        else if constexpr (
            std::is_same_v<std::decay_t<Type>, char*> ||
            std::is_same_v<std::decay_t<Type>, const char*>
        )
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
        } else if constexpr (
            std::is_same_v<std::decay_t<Type>, char*> ||
            std::is_same_v<std::decay_t<Type>, const char*>
        ) {
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
        if constexpr (
            std::is_same_v<std::decay_t<Type>, String>
            || std::is_same_v<std::decay_t<Type>, StringView>
        ) totalSize += arg.Size();
        else if constexpr (
            std::is_same_v<std::decay_t<Type>, std::string>
            || std::is_same_v<std::decay_t<Type>, std::string_view>
        ) totalSize += static_cast<Int>(arg.size());
        else if constexpr (
            std::is_same_v<std::decay_t<Type>, char*> ||
            std::is_same_v<std::decay_t<Type>, const char*>
        ) totalSize += static_cast<Int>(std::strlen(arg));
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