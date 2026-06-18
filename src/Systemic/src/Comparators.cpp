#include "../include/Comparators.h"
#include "../include/DataTypes/Decimal.h"
#include "../include/DataTypes/Guid.h"
#include "../include/DataTypes/DateTime.h"
#include "../include/DataTypes/String.h"
#include "../include/DataTypes/StringView.h"
#include "../include/DataTypes/Value.h"

namespace Comparators{
    //Branchless Comparison
    //left < right	-1
    // equal	0
    // left > right	+1
    Comparator Compare(const DataTypes::Decimal& lhs, const DataTypes::Decimal& rhs){
        return BranchlessCompare(lhs < rhs, lhs > rhs);
    }

    Comparator Compare(const DataTypes::Guid& lhs, const DataTypes::Guid& rhs){
        return BranchlessCompare(lhs < rhs, lhs > rhs);
    }

    Comparator Compare(const DataTypes::DateTime& lhs, const DataTypes::DateTime& rhs){
        return BranchlessCompare(lhs < rhs, lhs > rhs);
    }

    Comparator Compare(const DataTypes::String& lhs, const DataTypes::String& rhs){
        return Compare(lhs, rhs.Data(), rhs.Size());
    }

    Comparator Compare(const DataTypes::String& lhs, const char* rhs, const Int size){
        const auto minSize = std::min(lhs.Size(), size);

        const auto cmp = std::memcmp(lhs.Data(), rhs, minSize);

        const auto result = cmp != 0
            ? cmp
            : (lhs.Size() > size) - (lhs.Size() < size);

        return static_cast<Comparator>(
            (result > 0) - (result < 0)
        );
    }

    Comparator Compare(const DataTypes::StringView& lhs, const DataTypes::StringView& rhs){
        const auto minSize = std::min(lhs.Size(), rhs.Size());

        const auto cmp = std::memcmp(lhs.Data(), rhs.Data(), minSize);

        const int result = cmp != 0
            ? cmp
            : (lhs.Size() > rhs.Size()) - (lhs.Size() < rhs.Size());

        return static_cast<Comparator>(
            (result > 0) - (result < 0)
        );
    }

    // Comparator Compare(const DataTypes::JsonBinary& lhs, const DataTypes::JsonBinary& rhs){
    //     return C
    //     return Compare(lhs.Data(), rhs.Data(), lhs.Size());
    // }

    Comparator CompareIgnoreOrdinalCase(const char* lhs, const char* rhs, const Int size){
        const auto cmp = strncasecmp(lhs, rhs, size);
        return static_cast<Comparator>(
            (cmp > 0) - (cmp < 0)
        );
    }

    Comparator Compare(const Value& lhs, const Value& rhs){
        switch (PromoteType(lhs.GetType(), rhs.GetType())) {
        case DataType::String:
            return Compare(lhs.AsStringView(), rhs.AsStringView());
        case DataType::Bool:
            return Compare(lhs.AsBool(), rhs.AsBool());
        case DataType::TinyInt:
        case DataType::SmallInt:
        case DataType::Int:
        case DataType::BigInt:
            return Compare(lhs.AsBigInt(), rhs.AsBigInt());
        case DataType::Decimal:
            return Compare(lhs.AsDecimal(), rhs.AsDecimal());
        case DataType::DateTime:
            return Compare(lhs.AsDateTime(), rhs.AsDateTime());
        case DataType::Guid:
            return Compare(lhs.AsGuid(), rhs.AsGuid());
        case DataType::Null:
            return Compare(lhs.IsNull(), rhs.IsNull());
        case DataType::Json:
        case DataType::RowIdentifier:
        default:
            throw std::runtime_error("RowIdentifier cannot be compared");
        }
    }
}
