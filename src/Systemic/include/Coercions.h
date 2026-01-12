#pragma once
#include <cstdint>
#include "DataTypes/Value.h"
#include "DataStructures/HashSet.h"
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
            /* UString */ {
                CoercionType::Explicit, CoercionType::Explicit, CoercionType::Explicit, CoercionType::Explicit,
                CoercionType::Explicit, CoercionType::Implicit, CoercionType::Implicit, CoercionType::Explicit,
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

        static inline const HashSet<std::string> TrueStrings = {
            "true", "1", "yes", "y", "on"
        };
        static inline const HashSet<std::string> FalseStrings = {
            "false", "0", "no", "n", "off"
        };

    [[nodiscard]] static constexpr CoercionType GetCoercionType(const DataType& fromType, const DataType& toType);
    [[nodiscard]] static bool ParseAsBoolFromString(const Value& value);
    [[nodiscard]] static bool ParseAsBoolFromString(const Value& value, bool& outVal);

    [[nodiscard]] static bool CanGetTinyInt(const Value& value);
    [[nodiscard]] static bool CanGetBool(const Value& value);
    [[nodiscard]] static bool CanGetSmallInt(const Value& value);
    [[nodiscard]] static bool CanGetInt(const Value& value);
    [[nodiscard]] static bool CanGetBigInt(const Value& value);
    [[nodiscard]] static bool CanGetString(const Value& value);
    [[nodiscard]] static bool CanGetUnicodeString(const Value& value);
    [[nodiscard]] static bool CanGetGuid(const Value& value);
    [[nodiscard]] static bool CanGetDateTime(const Value& value);
    [[nodiscard]] static bool CanGetDecimal(const Value& value);

    static void DownCastFromSmallInt(Value& value);
    static void DownCastFromInt(Value& value);
    static void DownCastFromBigInt(Value& value);


    public:
        [[nodiscard]] static bool IsCoercionAllowed(const DataType& fromType, const DataType& toType, const bool& explicitCast = false);

        [[nodiscard]] static bool ToBool(const Value& value, const bool& explicitCast = false);
        [[nodiscard]] static int8_t ToTinyInt(const Value& value, const bool& explicitCast = false);
        [[nodiscard]] static int16_t ToSmallInt(const Value& value, const bool& explicitCast = false);
        [[nodiscard]] static int32_t ToInt(const Value& value, const bool& explicitCast = false);
        [[nodiscard]] static int64_t ToBigInt(const Value& value, const bool& explicitCast = false);
        [[nodiscard]] static std::string ToString(const Value& value, const bool& explicitCast = false);
        [[nodiscard]] static u16string ToUnicodeString(const Value& value, const bool& explicitCast = false);
        [[nodiscard]] static Guid ToGuid(const Value& value, const bool& explicitCast = false);
        [[nodiscard]] static DateTime ToDateTime(const Value& value, const bool& explicitCast = false);
        [[nodiscard]] static Decimal ToDecimal(const Value& value, const bool& explicitCast = false);

        [[nodiscard]] static bool CanBeParsedToType(const DataType& toType, const Value& value);
        static void DeduceIntegerType(Value& value);

    };
}