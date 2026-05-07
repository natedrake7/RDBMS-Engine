#pragma once
#include "DataTypes/DataTypes.h"
#include "DataTypes/JsonBinary.h"

namespace DataTypes{
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

    [[nodiscard]] inline Comparator BranchlessCompare(bool rhs, bool lhs);

    [[nodiscard]] inline Comparator Compare(bool lhs, bool rhs);
    [[nodiscard]] inline Comparator Compare(BigInt lhs, BigInt rhs);
    [[nodiscard]] inline Comparator Compare(const DataTypes::Decimal& lhs, const DataTypes::Decimal& rhs);
    [[nodiscard]] inline Comparator Compare(const DataTypes::Guid& lhs, const DataTypes::Guid& rhs);
    [[nodiscard]] inline Comparator Compare(const DataTypes::DateTime& lhs, const DataTypes::DateTime& rhs);
    [[nodiscard]] inline Comparator Compare(const DataTypes::String& lhs, const DataTypes::String& rhs);
    [[nodiscard]] inline Comparator Compare(const DataTypes::StringView& lhs, const DataTypes::StringView& rhs);
    [[nodiscard]] inline Comparator Compare(const DataTypes::JsonBinary& lhs, const DataTypes::JsonBinary& rhs);

    [[nodiscard]] Comparator CompareIgnoreOrdinalCase(const char* lhs, const char* rhs, Int size);

    [[nodiscard]] Comparator Compare(const Value& lhs, const Value& rhs);
}
