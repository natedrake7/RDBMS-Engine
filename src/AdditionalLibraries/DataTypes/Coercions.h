#pragma once
#include <cstdint>
#include "../../Database/Constants.h"

namespace DataTypes::Coercions {
    enum class CoercionType : uint8_t {
        Implicit = 0,   // Allowed automatically (safe)
        Explicit = 1,    // Allowed but requires CAST
        None = 2      // Not allowed
    };

    constexpr CoercionType TypeCoercionMatrix[][12] = {
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

    constexpr CoercionType GetCoercionType(const Constants::DataType& fromType, const Constants::DataType& toType) {
        return TypeCoercionMatrix[static_cast<int>(fromType)][static_cast<int>(toType)];
    }

    inline bool IsCoercionAllowed(const Constants::DataType& fromType, const Constants::DataType& toType, const bool& explicitCast = false) {
        const auto coercionType = GetCoercionType(fromType, toType);
        return explicitCast
            ? (coercionType == CoercionType::Implicit || coercionType == CoercionType::Explicit)
            : (coercionType == CoercionType::Implicit);
    }

}