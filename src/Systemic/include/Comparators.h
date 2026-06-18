#pragma once
#include "DataTypes/DataTypes.h"

namespace DataTypes{
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

    [[nodiscard]] static constexpr Comparator BranchlessCompare(const bool lhs, const bool rhs){
        return static_cast<Comparator>(
            (rhs > lhs) - (rhs < lhs)
        );
    }

    template<DataTypes::IsInteger T>
    [[nodiscard]] static constexpr Comparator Compare(T lhs, T rhs){
        return BranchlessCompare(lhs < rhs, lhs > rhs);
    }

    [[nodiscard]] Comparator Compare(const DataTypes::Decimal& lhs, const DataTypes::Decimal& rhs);
    [[nodiscard]] Comparator Compare(const DataTypes::Guid& lhs, const DataTypes::Guid& rhs);
    [[nodiscard]] Comparator Compare(const DataTypes::DateTime& lhs, const DataTypes::DateTime& rhs);
    [[nodiscard]] Comparator Compare(const DataTypes::String& lhs, const DataTypes::String& rhs);

    [[nodiscard]] Comparator Compare(const DataTypes::String& lhs, const char* rhs, Int size);

    [[nodiscard]] Comparator Compare(const DataTypes::StringView& lhs, const DataTypes::StringView& rhs);
    // [[nodiscard]] Comparator Compare(const DataTypes::JsonBinary& lhs, const DataTypes::JsonBinary& rhs);

    [[nodiscard]] Comparator CompareIgnoreOrdinalCase(const char* lhs, const char* rhs, Int size);

    [[nodiscard]] Comparator Compare(const Value& lhs, const Value& rhs);
}
