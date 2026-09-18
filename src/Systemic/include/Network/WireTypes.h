#pragma once
#include "../DataTypes/DateTime.h"
#include "../DataTypes/Guid.h"
#include "../DataTypes/DataTypes.h"
#include "../DataTypes/PackedWord.h"

namespace Network{
    inline constexpr protocol_version_t MIN_PROTOCOL_VERSION = 1;
    inline constexpr protocol_version_t PROTOCOL_VERSION = 1;

    // Explicit, stable codes. Never renumber; only append.
    enum class WireType : UnsignedTinyInt{
        Null      = 0,
        Bool      = 1,   // 1 byte, 0 or 1
        TinyInt   = 2,   // i8
        SmallInt  = 3,   // i16 LE
        Int       = 4,   // i32 LE
        BigInt    = 5,   // i64 LE
        Float     = 6,   // IEEE-754 f32 LE
        Double    = 7,   // IEEE-754 f64 LE
        DateTime  = 8,   // i64 LE, milliseconds since Unix epoch, UTC
        Guid      = 9,   // 16 bytes, RFC 4122 byte order
        String    = 10,  // variable: UTF-8
        Decimal   = 11,  // variable: invariant-culture text, e.g. "-123.4500"
        Json      = 12,  // variable: UTF-8 JSON text
        Invalid
    };

    [[nodiscard]] inline constexpr bool IsVariableLength(const WireType type){
        return
            type == WireType::String
            || type == WireType::Json
            || type == WireType::Decimal;
    }

    [[nodiscard]] inline constexpr bool IsFixedLength(const WireType type){
        return !IsVariableLength(type);
    }

    [[nodiscard]] inline constexpr UnsignedSmallInt FixedWidth(const WireType type){
        switch (type){
        case WireType::Bool:
            return sizeof(bool);
        case WireType::TinyInt:
            return sizeof(TinyInt);
        case WireType::SmallInt:
            return sizeof(SmallInt);
        case WireType::Int:
            return sizeof(Int);
        case WireType::Float:
            return 4;
        case WireType::BigInt:
            return sizeof(BigInt);
        case WireType::Double:
            return sizeof(double);
        case WireType::DateTime:
            return sizeof(DataTypes::DateTime);
        case WireType::Guid:
            return sizeof(DataTypes::Guid);
        case WireType::Null:
        case WireType::String:
        case WireType::Decimal:
        case WireType::Json:
        case WireType::Invalid:
            return 0;
        }
        return 0;
    }

    enum class ColumnFlags : UnsignedTinyInt{
        None     = 0,
        Constant = 1 << 0,   // encodedRows == 1, applies to every row of the batch
    };

    [[nodiscard]] inline constexpr UnsignedInt ValidityBytes(const UnsignedInt rows){
        return PackedByte::WordsFor(rows);
    }
}
