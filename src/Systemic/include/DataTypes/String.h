#pragma once
#include <span>
#include "StringView.h"
#include "DataTypes.h"
#include <ostream>

namespace Memory{
    class IAllocator;
}

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

        public:
            String(const ::Memory::IAllocator* allocator);
            String(const ::Memory::IAllocator* allocator, Int size);
            String(const object_t* str, Int size, const ::Memory::IAllocator* allocator);
            String(const char* str, const ::Memory::IAllocator* allocator);
            String(char* str, Int size, const ::Memory::IAllocator* allocator);
            String(const String& other);
            String& operator=(const String& other);
            String(String&& other) noexcept;
            String& operator=(String&& other) noexcept;

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

            //functions
            [[nodiscard]] String Concat(const String& other) const;
            [[nodiscard]] String Concat(const char* other) const;
            [[nodiscard]] String Concat(const StringView& other) const;
            [[nodiscard]] String Concat(std::string_view other) const;
            [[nodiscard]] String Concat(const std::string& other) const;
            [[nodiscard]] static String Concat(const StringView& lhs, const StringView& rhs, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static String Concat(const String& lhs, const String& rhs, const ::Memory::IAllocator* allocator);
            [[nodiscard]] static String Concat(const char* lhs, const char* rhs, const ::Memory::IAllocator* allocator);


            [[nodiscard]] String& Append(const String& other);
            [[nodiscard]] String& Append(const StringView& other);
            [[nodiscard]] String& Append(const char* other);
            [[nodiscard]] String& Append(std::string_view other);
            [[nodiscard]] String& Append(const std::string& other);

            void Insert(Int pos, const char* data, Int size);

            [[nodiscard]] String Substring(Int startIndex, Int length) const;
            [[nodiscard]] StringView SubstringView(Int startIndex, Int length) const;

            [[nodiscard]] Int Size()const;
            [[nodiscard]] Int IndexOf(char c) const;
            [[nodiscard]] bool Contains(const String& other, StringComparisonType type) const;
            [[nodiscard]] bool Contains(const char* other, StringComparisonType type) const;
            [[nodiscard]] bool Contains(const StringView& other, StringComparisonType type) const;
            [[nodiscard]] bool Contains(std::string_view other, StringComparisonType type) const;
            [[nodiscard]] bool Contains(const std::string& other, StringComparisonType type) const;
            [[nodiscard]] bool Empty() const;

            // STL compatibility
            using const_iterator = const char*;

            [[nodiscard]] const_iterator begin() const;
            [[nodiscard]] const_iterator end() const;
    };

}
