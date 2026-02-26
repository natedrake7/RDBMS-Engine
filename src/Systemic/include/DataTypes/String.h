#pragma once
#include <span>

#include "DataTypes.h"
#include "../../../DatabaseEngine/include/Managers/GlobalMemoryManager.h"

namespace Memory
{
    class IAllocator;
}

namespace DataTypes{
    enum class StringComparisonType: UnsignedTinyInt{
        Equals = 0,
        EqualsIgnoreOrdinalCase = 1,
        StartsWith = 2,
        StartsWithIgnoreOrdinalCase = 3,
        EndsWith = 4,
        EndsWithIgnoreOrdinalCase = 5,
        Contains = 6,
        ContainsIgnoreCase = 7
    };

    class String{
        const object_t* _data;
        Int size;

        [[nodiscard]] inline bool EqualsIgnoreCase(const String& other) const;
        [[nodiscard]] inline bool StartsWith(const String& other) const;
        [[nodiscard]] inline bool StartsWithIgnoreCase(const String& other) const;
        [[nodiscard]] inline bool EndsWith(const String& other) const;
        [[nodiscard]] inline bool EndsWithIgnoreCase(const String& other) const;
        [[nodiscard]] inline bool Contains(const String& other) const;
        [[nodiscard]] inline bool ContainsIgnoreCase(const String& other) const;
        public:
        /**
             *
             * @param data Non-owning pointer of the actual data
             * @param size The size of the string view in bytes (not including null terminator, if any).
             * The string view can contain null characters within it and is not required to be null-terminated.
        */
        String(const object_t* data, Int size);
        String(const String& other);
        String(String&& other) noexcept;

        String(const char* other);
        String& operator=(const char* other);

        String& operator=(String&& other) noexcept;
        String& operator=(const String& other);
        ~String();

        [[nodiscard]] const object_t* Data() const;
        [[nodiscard]] const char* GetDataAsChar() const;

        //operators
        friend std::ostream& operator<<(std::ostream& os, const String& sv);
        [[nodiscard]] char operator[](Int index) const;
        bool operator==(const String& other) const;
        bool operator!=(const String& other) const;

        operator std::string_view() const;
        operator std::span<const char>() const;

        //functions
        [[nodiscard]] String Concat(const String& other, const ::Memory::IAllocator* allocator) const;
        [[nodiscard]] String Substring(Int startIndex, Int length) const;

        [[nodiscard]] Int Size()const;
        [[nodiscard]] Int IndexOf(char c) const;
        [[nodiscard]] bool Contains(const String& other, StringComparisonType type) const;
        [[nodiscard]] bool Empty() const;

        // STL compatibility
        using const_iterator = const object_t*;

        const_iterator begin() const;
        const_iterator end() const;
    };
}
