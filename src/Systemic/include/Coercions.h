#pragma once
#include "DataStructures/ConstexprHashSet.h"
#include "DataTypes/Value.h"
#include "DataTypes/Guid.h"

namespace DataTypes{
    enum class CoercionType : UnsignedTinyInt {
        Implicit = 0,   // Allowed automatically (safe)
        Explicit = 1,    // Allowed but requires CAST
        None = 2      // Not allowed
    };

    class Coercions {
        static constexpr CoercionType TypeCoercionMatrix[][12] = {
            // To:        TinyInt  SmallInt Int     BigInt
            //            Decimal  String   UString  Bool
            //            DateTime Guid    RowId   Invalid
            /* TinyInt */ {
                CoercionType::Implicit, CoercionType::Implicit, CoercionType::Implicit, CoercionType::Implicit,
                CoercionType::Implicit, CoercionType::Implicit, CoercionType::Implicit, CoercionType::Explicit,
                CoercionType::None,     CoercionType::None,    CoercionType::None,    CoercionType::None
            },
            /* SmallInt */ {
                CoercionType::Explicit, CoercionType::Implicit, CoercionType::Implicit, CoercionType::Implicit,
                CoercionType::Implicit, CoercionType::Implicit, CoercionType::Implicit, CoercionType::Explicit,
                CoercionType::None,     CoercionType::None,    CoercionType::None,    CoercionType::None
            },
            /* Int */ {
                CoercionType::Explicit, CoercionType::Explicit, CoercionType::Implicit, CoercionType::Implicit,
                CoercionType::Implicit, CoercionType::Implicit, CoercionType::Implicit, CoercionType::Explicit,
                CoercionType::None,     CoercionType::None,    CoercionType::None,    CoercionType::None
            },
            /* BigInt */ {
                CoercionType::Explicit, CoercionType::Explicit, CoercionType::Explicit, CoercionType::Implicit,
                CoercionType::Implicit, CoercionType::Implicit, CoercionType::Implicit, CoercionType::Explicit,
                CoercionType::None,     CoercionType::None,    CoercionType::None,    CoercionType::None
            },
            /* Decimal */ {
                CoercionType::Explicit, CoercionType::Explicit, CoercionType::Explicit, CoercionType::Explicit,
                CoercionType::Implicit, CoercionType::Implicit, CoercionType::Implicit, CoercionType::None,
                CoercionType::None,     CoercionType::None,    CoercionType::None,    CoercionType::None
            },
            /* String */ {
                CoercionType::Explicit, CoercionType::Explicit, CoercionType::Explicit, CoercionType::Explicit,
                CoercionType::Explicit, CoercionType::Implicit, CoercionType::Explicit, CoercionType::Explicit,
                CoercionType::Explicit, CoercionType::Explicit, CoercionType::None,    CoercionType::None
            },
            /* Bool */ {
                CoercionType::Implicit, CoercionType::Implicit, CoercionType::Implicit, CoercionType::Implicit,
                CoercionType::Implicit, CoercionType::Implicit, CoercionType::Implicit, CoercionType::Implicit,
                CoercionType::None,     CoercionType::None,    CoercionType::None,    CoercionType::None
            },
            /* DateTime */ {
                CoercionType::None,     CoercionType::None,    CoercionType::None,    CoercionType::None,
                CoercionType::None,     CoercionType::Implicit, CoercionType::Implicit, CoercionType::None,
                CoercionType::Implicit, CoercionType::None,    CoercionType::None,    CoercionType::None
            },
            /* Guid */ {
                CoercionType::None,     CoercionType::None,    CoercionType::None,    CoercionType::None,
                CoercionType::None,     CoercionType::Implicit, CoercionType::Implicit, CoercionType::None,
                CoercionType::None,     CoercionType::Implicit, CoercionType::None,    CoercionType::None
            },
            /* RowId */ {
                CoercionType::None,     CoercionType::None,    CoercionType::None,    CoercionType::None,
                CoercionType::None,     CoercionType::None,    CoercionType::None,    CoercionType::None,
                CoercionType::None,     CoercionType::None,    CoercionType::Implicit, CoercionType::None
            },
            /* Invalid */ {
                CoercionType::None,     CoercionType::None,    CoercionType::None,    CoercionType::None,
                CoercionType::None,     CoercionType::None,    CoercionType::None,    CoercionType::None,
                CoercionType::None,     CoercionType::None,    CoercionType::None,    CoercionType::None
            }
        };

        static constexpr ConstexprHashSet<StringView, 5> TrueStrings = {
            StringView("true"),
            StringView("1"),
            StringView("yes"),
            StringView("y"),
            StringView("on")
        };

        static constexpr ConstexprHashSet<StringView, 5> FalseStrings = {
            StringView("false"),
            StringView("0"),
            StringView("no"),
            StringView("n"),
            StringView("off")
        };

        static void ThrowException(DataType type);

    [[nodiscard]] static constexpr CoercionType GetCoercionType(DataType fromType, DataType toType);
    [[nodiscard]] static bool ParseAsBoolFromString(const Value& value);
    [[nodiscard]] static bool ParseAsBoolFromString(const Value& value, bool& outVal);

    [[nodiscard]] static bool CanGetTinyInt(const Value& value);
    [[nodiscard]] static bool CanGetBool(const Value& value);
    [[nodiscard]] static bool CanGetSmallInt(const Value& value);
    [[nodiscard]] static bool CanGetInt(const Value& value);
    [[nodiscard]] static bool CanGetBigInt(const Value& value);
    [[nodiscard]] static bool CanGetString(const Value& value);
    [[nodiscard]] static bool CanGetGuid(const Value& value);
    [[nodiscard]] static bool CanGetDateTime(const Value& value);
    [[nodiscard]] static bool CanGetDecimal(const Value& value);

    static void DownCastFromSmallInt(Value& value);
    static void DownCastFromInt(Value& value);
    static void DownCastFromBigInt(Value& value);

    public:
        [[nodiscard]] static bool IsCoercionAllowed(DataType fromType, DataType toType, bool explicitCast = false);
        [[nodiscard]] static bool ToBool(const Value& value, bool explicitCast = false);
        [[nodiscard]] static TinyInt ToTinyInt(const Value& value, bool explicitCast = false);
        [[nodiscard]] static SmallInt ToSmallInt(const Value& value, bool explicitCast = false);
        [[nodiscard]] static Int ToInt(const Value& value, bool explicitCast = false);
        [[nodiscard]] static BigInt ToBigInt(const Value& value, bool explicitCast = false);
        [[nodiscard]] static String ToString(const Value& value, bool explicitCast = false);
        [[nodiscard]] static StringView ToStringView(const Value& value, bool explicitCast = false);
        [[nodiscard]] static Guid ToGuid(const Value& value, bool explicitCast = false);
        [[nodiscard]] static DateTime ToDateTime(const Value& value, bool explicitCast = false);
        [[nodiscard]] static Decimal ToDecimal(const Value& value, bool explicitCast = false);

        [[nodiscard]] static bool CanBeParsedToType(DataType toType, const Value& value);
        static void DeduceIntegerType(Value& value);

    };
}