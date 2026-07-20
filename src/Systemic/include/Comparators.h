#pragma once
#include "DataTypes/DataTypes.h"

namespace DataTypes{
    class StringValue;
    class JsonBinary;
    class StringView;
    class DateTime;
    class Guid;
    class Decimal;
    class String;
}

class Value;

namespace Comparators{
    enum class Comparator : TinyInt{
        Less = -1,
        Equal = 0,
        Greater = 1
    };

    [[nodiscard]] inline constexpr Comparator BranchlessCompare(const bool isLess, const bool isGreater){
        return static_cast<Comparator>(isGreater - isLess);
    }

    template<DataTypes::IsInteger T>
    [[nodiscard]] inline constexpr Comparator Compare(T lhs, T rhs){
        return BranchlessCompare(lhs < rhs, lhs > rhs);
    }

    [[nodiscard]] Comparator Compare(const DataTypes::Decimal& lhs, const DataTypes::Decimal& rhs);
    [[nodiscard]] Comparator Compare(const DataTypes::Guid& lhs, const DataTypes::Guid& rhs);
    [[nodiscard]] Comparator Compare(const DataTypes::DateTime& lhs, const DataTypes::DateTime& rhs);
    [[nodiscard]] Comparator Compare(const DataTypes::String& lhs, const DataTypes::String& rhs);

    [[nodiscard]] bool Equals(const DataTypes::StringValue& lhs, const DataTypes::StringValue& rhs);
    [[nodiscard]] Comparator Compare(const DataTypes::StringValue& lhs, const DataTypes::StringValue& rhs);
    [[nodiscard]] Comparator CompareIgnoreCase(const DataTypes::StringValue& lhs, const DataTypes::StringValue& rhs);

    [[nodiscard]] Comparator Compare(const DataTypes::String& lhs, const char* rhs, Int size);

    [[nodiscard]] Comparator Compare(const DataTypes::StringView& lhs, const DataTypes::StringView& rhs);
    // [[nodiscard]] Comparator Compare(const DataTypes::JsonBinary& lhs, const DataTypes::JsonBinary& rhs);

    [[nodiscard]] Comparator CompareIgnoreOrdinalCase(const char* lhs, const char* rhs, Int size);

    [[nodiscard]] Comparator Compare(const Value& lhs, const Value& rhs);
}
