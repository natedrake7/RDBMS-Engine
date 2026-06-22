#pragma once
#include "../Converter.h"
#include "../DataStructures/ConstexprHashSet.h"
#include "../DataTypes/Value.h"
#include "../DataTypes/Guid.h"
#include "../DataTypes/JsonBinary.h"
#include "../DataTypes/DataTypes.StaticData.h"

namespace DataTypes{
    enum class CoercionType : UnsignedTinyInt {
        Implicit = 0,   // Allowed automatically (safe)
        Explicit = 1,  // Allowed but requires CAST
        None = 2      // Not allowed
    };

    struct CoercionMatrix {
        CoercionType cells[DATATYPE_COUNT][DATATYPE_COUNT];
        [[nodiscard]] constexpr CoercionType At(DataType from, DataType to) const {
            return cells[static_cast<Int>(from)][static_cast<Int>(to)];
        }
    };

    class Coercions {
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

    static inline void ThrowException(DataType type, DataType toType);

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
    [[nodiscard]] static bool CanGetJsonBinary(const Value& value);

    static void DownCastFromSmallInt(Value& value);
    static void DownCastFromInt(Value& value);
    static void DownCastFromBigInt(Value& value);

    public:
        [[nodiscard]] constexpr static bool IsCoercionAllowed(DataType fromType, DataType toType, bool explicitCast = false);

        constexpr static CoercionMatrix BuildCoercionMatrix();

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
        [[nodiscard]] static JsonBinary ToJsonBinary(const Value& value, bool explicitCast = false);

        template<typename TFrom, typename TTo>
        [[nodiscard]] static TTo To(TFrom& input, const Memory::IAllocator* allocator);

        [[nodiscard]] static bool CanBeParsedToType(DataType toType, const Value& value);
        static void DeduceIntegerType(Value& value);
    };

    constexpr CoercionMatrix Coercions::BuildCoercionMatrix(){
        using DT = DataType;
        using CT = CoercionType;

        CoercionMatrix matrix = {};

        // 1. Default everything to NONE
        for (Int i = 0; i < DATATYPE_COUNT; i++) {
            for (Int j = 0; j < DATATYPE_COUNT; j++) {
                matrix.cells[i][j] = CT::None;
            }
        }

        // 2. Identity conversions (T → T)
        for (Int i = 0; i < DATATYPE_COUNT; i++)
            matrix.cells[i][i] = CT::Implicit;

        // ---- Numeric ladder ----
        // TinyInt → ...
        matrix.cells[static_cast<Int>(DT::TinyInt)][static_cast<Int>(DT::SmallInt)] = CT::Implicit;
        matrix.cells[static_cast<Int>(DT::TinyInt)][static_cast<Int>(DT::Int)]      = CT::Implicit;
        matrix.cells[static_cast<Int>(DT::TinyInt)][static_cast<Int>(DT::BigInt)]   = CT::Implicit;
        matrix.cells[static_cast<Int>(DT::TinyInt)][static_cast<Int>(DT::Decimal)]  = CT::Implicit;

        // SmallInt →
        matrix.cells[static_cast<Int>(DT::SmallInt)][static_cast<Int>(DT::TinyInt)] = CT::Explicit;
        matrix.cells[static_cast<Int>(DT::SmallInt)][static_cast<Int>(DT::Int)]     = CT::Implicit;
        matrix.cells[static_cast<Int>(DT::SmallInt)][static_cast<Int>(DT::BigInt)]  = CT::Implicit;
        matrix.cells[static_cast<Int>(DT::SmallInt)][static_cast<Int>(DT::Decimal)] = CT::Implicit;

        // Int →
        matrix.cells[static_cast<Int>(DT::Int)][static_cast<Int>(DT::TinyInt)]  = CT::Explicit;
        matrix.cells[static_cast<Int>(DT::Int)][static_cast<Int>(DT::SmallInt)] = CT::Explicit;
        matrix.cells[static_cast<Int>(DT::Int)][static_cast<Int>(DT::BigInt)]   = CT::Implicit;
        matrix.cells[static_cast<Int>(DT::Int)][static_cast<Int>(DT::Decimal)]  = CT::Implicit;

        // BigInt →
        matrix.cells[static_cast<Int>(DT::BigInt)][static_cast<Int>(DT::TinyInt)]  = CT::Explicit;
        matrix.cells[static_cast<Int>(DT::BigInt)][static_cast<Int>(DT::SmallInt)] = CT::Explicit;
        matrix.cells[static_cast<Int>(DT::BigInt)][static_cast<Int>(DT::Int)]      = CT::Explicit;
        matrix.cells[static_cast<Int>(DT::BigInt)][static_cast<Int>(DT::Decimal)]  = CT::Implicit;

        // Decimal →
        //TOOD make explicit
        matrix.cells[static_cast<Int>(DT::Decimal)][static_cast<Int>(DT::TinyInt)]  = CT::None;
        matrix.cells[static_cast<Int>(DT::Decimal)][static_cast<Int>(DT::SmallInt)] = CT::None;
        matrix.cells[static_cast<Int>(DT::Decimal)][static_cast<Int>(DT::Int)]      = CT::None;
        matrix.cells[static_cast<Int>(DT::Decimal)][static_cast<Int>(DT::BigInt)]   = CT::None;

        // ---- String conversions ----
        for (Int t = static_cast<Int>(DT::TinyInt); t <= static_cast<Int>(DT::Decimal); t++) {
            matrix.cells[static_cast<Int>(DT::String)][t] = CT::Explicit; // parse
            matrix.cells[t][static_cast<Int>(DT::String)] = CT::Implicit; // stringify
        }

        matrix.cells[static_cast<Int>(DT::String)][static_cast<Int>(DT::Bool)]     = CT::Explicit;
        matrix.cells[static_cast<Int>(DT::Bool)][static_cast<Int>(DT::String)]     = CT::Implicit;
        matrix.cells[static_cast<Int>(DT::String)][static_cast<Int>(DT::DateTime)] = CT::Explicit;
        matrix.cells[static_cast<Int>(DT::DateTime)][static_cast<Int>(DT::String)] = CT::Implicit;
        matrix.cells[static_cast<Int>(DT::String)][static_cast<Int>(DT::Guid)]     = CT::Explicit;
        matrix.cells[static_cast<Int>(DT::Guid)][static_cast<Int>(DT::String)]     = CT::Implicit;

        // ---- Bool conversions ----
        matrix.cells[static_cast<Int>(DT::Bool)][static_cast<Int>(DT::TinyInt)]  = CT::Implicit;
        matrix.cells[static_cast<Int>(DT::Bool)][static_cast<Int>(DT::SmallInt)] = CT::Implicit;
        matrix.cells[static_cast<Int>(DT::Bool)][static_cast<Int>(DT::Int)]      = CT::Implicit;
        matrix.cells[static_cast<Int>(DT::Bool)][static_cast<Int>(DT::BigInt)]   = CT::Implicit;

        // ---- JSON rules ----
        for (Int t = 0; t < DATATYPE_COUNT; t++) {
            if (t == static_cast<Int>(DT::Json)) continue;
            matrix.cells[t][static_cast<Int>(DT::Json)] = CT::None;
        }
        matrix.cells[static_cast<Int>(DT::Json)][static_cast<Int>(DT::String)] = CT::Explicit;

        return matrix;
    }

    inline constexpr CoercionMatrix TYPE_COERCION_MATRIX = Coercions::BuildCoercionMatrix();

    constexpr CoercionType Coercions::GetCoercionType(const DataType fromType, const DataType toType){
        return TYPE_COERCION_MATRIX.At(fromType, toType);
    }

    constexpr bool Coercions::IsCoercionAllowed(
        const DataType fromType,
        const DataType toType,
        const bool explicitCast
    ){
        const auto coercionType = GetCoercionType(fromType, toType);
        return explicitCast
            ? (coercionType == CoercionType::Implicit || coercionType == CoercionType::Explicit)
            : (coercionType == CoercionType::Implicit);
    }

    template <typename TFrom, typename TTo>
    TTo Coercions::To(TFrom& input, const Memory::IAllocator* allocator){
        // identity (covers String->String, Decimal->Decimal, Guid->Guid, ...)
        if constexpr (std::is_same_v<TFrom, TTo>)
            return input;

        // --- bool needs special-casing BEFORE the generic numeric/string arms ---
        else if constexpr (std::is_same_v<TFrom, bool> && std::is_same_v<TTo, String>)   // Bool -> String
            return input ? String(TRUE_STRING, allocator) : String(FALSE_STRING, allocator);

        else if constexpr (IsString<TFrom> && std::is_same_v<TTo, bool>) {            // String -> Bool
            const auto view = input.ToView();
            if (TrueStrings.Contains(view))  return true;
            if (FalseStrings.Contains(view)) return false;
            return false;
        }

        // --- numeric ladder ---
        else if constexpr (DataTypes::IsInteger<TFrom> && std::is_arithmetic_v<TTo>)      // num <-> num
            return static_cast<TTo>(input);

        // --- string <-> numeric ---
        else if constexpr (DataTypes::IsInteger<TFrom> && std::is_same_v<TTo, String>)    // num -> String
            return Converter::IntToStr<TFrom>(input, allocator);

        else if constexpr (IsString<TFrom> && std::is_arithmetic_v<TTo>)              // String -> num
            return Converter::StrToInt<TTo>(input);

        // --- Decimal ---
        else if constexpr (DataTypes::IsInteger<TFrom> && std::is_same_v<TTo, Decimal>)   // num -> Decimal
            return Decimal(input);

        else if constexpr (IsString<TFrom> && std::is_same_v<TTo, Decimal>)           // String -> Decimal
            return Decimal(input.ToView());

        else if constexpr (std::is_same_v<TFrom, Decimal> && std::is_same_v<TTo, String>) // Decimal -> String
            return input.ToString(allocator);

        // --- DateTime ---
        else if constexpr (IsString<TFrom> && std::is_same_v<TTo, DateTime>) {        // String -> DateTime
            DateTime out;
            DateTime::FromString(out, input.ToView());
            return out;
        }
        else if constexpr (std::is_same_v<TFrom, DateTime> && std::is_same_v<TTo, String>)// DateTime -> String
            return input.ToString(allocator);

        // --- Guid ---
        else if constexpr (IsString<TFrom> && std::is_same_v<TTo, Guid>)              // String -> Guid
            return Guid::Parse(input);                                                    // has String & StringView overloads
        else if constexpr (std::is_same_v<TFrom, Guid> && std::is_same_v<TTo, String>)    // Guid -> String
            return input.ToString(allocator);

        // --- JSON ---
        else if constexpr (IsString<TFrom> && std::is_same_v<TTo, JsonBinary>) {      // String -> Json
            Serialization::JsonParser parser(allocator, input.ToView());
            return parser.Parse();
        }
        else if constexpr (std::is_same_v<TFrom, JsonBinary> && std::is_same_v<TTo, String>) // Json -> String
            return input.ToString();

        else
            static_assert(AlwaysFalse<TFrom>, "Coercions::To<TFrom,TTo>: unsupported pair");
    }
}
