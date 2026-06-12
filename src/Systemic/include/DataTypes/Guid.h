#pragma once
#include <array>
#include <cstdint>
#include <string>
#include "DataTypes.h"
#include "String.h"
#include "StringView.h"
#include "../DataStructures/StaticArray.h"

namespace DataTypes {
    constexpr Int GUID_SIZE = 16;
    constexpr static Int GUID_STRING_SIZE = 36;
    constexpr static Int GUID_STRING_NO_HYPHEN_SIZE = 32;
    constexpr static Int GUID_HEX_SIZE = 2;
    constexpr static StringView GUID_STRING_FORMAT = "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x";
    constexpr static StringView GUID_VALIDATION_FORMAT = "^[{]?[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}[}]?$";


    class Guid {
        UnsignedTinyInt _data[GUID_SIZE];

        static bool Validate(const char* str, Int size);
        static Guid Parse(const char* str, Int size);

        using StringBuffer = DataStructures::StaticArray<char, GUID_STRING_SIZE>;

    public:
        Guid();
        Guid(const unsigned char* data, Int size);
        explicit Guid(const std::array<UnsignedTinyInt, GUID_SIZE>& data);
        [[nodiscard]] UnsignedTinyInt* GetDataUnsafe();
        [[nodiscard]] const UnsignedTinyInt* GetData() const;

        [[nodiscard]] String ToString(const ::Memory::IAllocator* allocator) const;
        [[nodiscard]] StringBuffer ToStringBuffer() const;
        static Guid Parse(const String& str);
        static Guid Parse(const StringView& str);
        static Guid Parse(const std::string& str);
        static Guid Parse(const std::string_view& str);
        static Guid Parse(const char* str);

        static bool Validate(const String& str);
        static bool Validate(const StringView& str);
        static bool Validate(const std::string& str);
        static bool Validate(const std::string_view& str);
        static bool Validate(const char* str);

        friend std::ostream& operator<<(std::ostream& os, const Guid& guid);
        static Guid NewGuid();
        static Guid Empty();

        [[nodiscard]] long double Interpolate() const;

        friend bool operator==(const Guid& lhs, const Guid& rhs);
        friend bool operator!=(const Guid& lhs, const Guid& rhs);
        friend bool operator<(const Guid& lhs, const Guid& rhs);
        friend bool operator>(const Guid& lhs, const Guid& rhs);
        friend bool operator<=(const Guid& lhs, const Guid& rhs);
        friend bool operator>=(const Guid& lhs, const Guid& rhs);
    };
}

    template<>
    struct std::hash<DataTypes::Guid> {
        size_t operator()(const DataTypes::Guid& guid) const noexcept {
            size_t result = 0;

            if constexpr (requires { guid.GetData(); }) {
                const auto* data = guid.GetData();
                for (auto i = 0; i < DataTypes::GUID_SIZE; i++){
                    result ^= std::hash<UnsignedTinyInt>{}(data[i])
                        + 0x9e3779b97f4a7c15ULL + (result << 6) + (result >> 2);
                }
            }

            return result;
        }
    };