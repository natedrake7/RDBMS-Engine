#include "../include/Coercions.h"

#include "../include/Converter.h"
#include "../include/Functions/StringFunctions.h"
#include "DataTypes/DataTypes.StaticData.h"
#include "DataTypes/DateTime.h"

namespace DataTypes {
    constexpr CoercionType Coercions::GetCoercionType(
        const DataType fromType,
        const DataType toType
    ) {
        return Coercions::TypeCoercionMatrix[static_cast<Int>(fromType)][static_cast<Int>(toType)];
    }

    void Coercions::ThrowException(const DataType type) {
        if (type == DataType::Unknown)
            throw std::invalid_argument("Invalid Field Type");

        const auto& typeName = DataTypeToStringDictionary.Get(type);
        throw std::invalid_argument("Field type " + std::string(typeName.Data(), typeName.Size()) + " cannot be coerced to Bool");
    }

    bool Coercions::ParseAsBoolFromString(const Value& value) {
        const auto strView = value.AsStringView();

        if (TrueStrings.Contains(strView))
            return true;

        if (FalseStrings.Contains(strView))
            return false;

        return false;
    }

    bool Coercions::ParseAsBoolFromString(const Value& value, bool& outVal) {
        const auto strView = value.AsStringView();

        if (TrueStrings.Contains(strView)) {
            outVal = true;
            return true;
        }

        if (FalseStrings.Contains(strView)) {
            outVal = false;
            return true;
        }

        return false;
    }

    bool Coercions::CanGetTinyInt(const Value& value) {
        switch (value.GetType()) {
            case DataType::Bool:
            case DataType::TinyInt:
                return true;
            case DataType::SmallInt:
                return Converter<TinyInt>::TryStoi(value.AsSmallInt());
            case DataType::Int:
                return Converter<TinyInt>::TryStoi(value.AsInt());
            case DataType::BigInt:
                return Converter<TinyInt>::TryStoi(value.AsBigInt());
            case DataType::Decimal:
                return false;
            case DataType::String:
                return Converter<TinyInt>::TryStoi(value.AsStringView());
            default:
                return false;
        }
    }

    bool Coercions::CanGetBool(const Value& value) {
        switch (value.GetType()) {
            case DataType::TinyInt:
                return Converter<bool>::TryStoi(value.AsTinyInt());
            case DataType::SmallInt:
                return Converter<bool>::TryStoi(value.AsSmallInt());
            case DataType::Int:
                return Converter<bool>::TryStoi(value.AsInt());
            case DataType::BigInt:
                return Converter<bool>::TryStoi(value.AsBigInt());
            case DataType::Decimal:
                return false;
            case DataType::String: {
                bool outVal = false;
                return Coercions::ParseAsBoolFromString(value, outVal);
            }
            case DataType::Bool:
                return true;
            default:
                return false;
        }
    }

    bool Coercions::CanGetSmallInt(const Value& value) {
        switch (value.GetType()) {
            case DataType::Bool:
            case DataType::TinyInt:
            case DataType::SmallInt:
                return true;
            case DataType::Int:
                return Converter<SmallInt>::TryStoi(value.AsInt());
            case DataType::BigInt:
                return Converter<SmallInt>::TryStoi(value.AsBigInt());
            case DataType::Decimal:
                return false;
            case DataType::String:
                return Converter<SmallInt>::TryStoi(value.AsStringView());
            default:
                return false;
        }
    }

    bool Coercions::CanGetInt(const Value& value) {
        switch (value.GetType()) {
            case DataType::Bool:
            case DataType::TinyInt:
            case DataType::SmallInt:
            case DataType::Int:
                return true;
            case DataType::BigInt:
                return Converter<Int>::TryStoi(value.AsBigInt());
            case DataType::Decimal:
                return false;
            case DataType::String:
                return Converter<Int>::TryStoi(value.AsStringView());
            default:
                return false;
        }
    }

    bool Coercions::CanGetBigInt(const Value& value) {
        switch (value.GetType()) {
            case DataType::Bool:
            case DataType::TinyInt:
            case DataType::SmallInt:
            case DataType::Int:
            case DataType::BigInt:
                return true;
            case DataType::Decimal:
                return false;
            case DataType::String:
                return Converter<BigInt>::TryStoi(value.AsStringView());
            default:
                return false;
        }
    }

    bool Coercions::CanGetString(const Value& value) { return true; }

    bool Coercions::CanGetGuid(const Value& value) {
        switch (value.GetType()) {
            case DataType::Guid:
                return true;
            case DataType::String:
                return Guid::Validate(value.AsString());
            default:
                return false;
        }
    }

    bool Coercions::CanGetDateTime(const Value& value) {
        switch (value.GetType()) {
            case DataType::DateTime:
                return true;
            case DataType::String:
                return DateTime::FromString(value.AsStringView());
            default:
                return false;
        }
    }

    bool Coercions::CanGetDecimal(const Value& value) {
        return false;
    }

    bool Coercions::CanGetJsonBinary(const Value& value){
        switch (value.GetType()){
        case DataType::Json:
            return true;
        case DataType::String:
            //TODO maybe convert to json binary format
            return Serialization::JsonParser::IsJson(value.AsStringView());
        default:
            return false;
        }
    }

    void Coercions::DownCastFromSmallInt(Value& value) {
        const auto smallInt = value.AsSmallInt();
        if (!Converter<TinyInt>::TryStoi(smallInt))
            return;

        const auto tinyInt = Converter<TinyInt>::Stoi(smallInt);
        value.SetData(tinyInt);
    }

    void Coercions::DownCastFromInt(Value& value) {
        const auto integer = value.AsInt();

        if (Converter<TinyInt>::TryStoi(integer)) {
            const auto tinyInt = Converter<TinyInt>::Stoi(integer);
            value.SetData(tinyInt);
            return;
        }

        if (!Converter<SmallInt>::TryStoi(integer))
            return;

        const auto smallInt = Converter<SmallInt>::Stoi(integer);
        value.SetData(smallInt);
    }

    void Coercions::DownCastFromBigInt(Value& value) {
        const auto bigInt = value.AsBigInt();

        if (Converter<TinyInt>::TryStoi(bigInt)) {
            const auto tinyInt = Converter<TinyInt>::Stoi(bigInt);
            value.SetData(tinyInt);
            return;
        }

        if (Converter<SmallInt>::TryStoi(bigInt)) {
            const auto smallInt = Converter<SmallInt>::Stoi(bigInt);
            value.SetData(smallInt);
            return;
        }

        if (!Converter<Int>::TryStoi(bigInt))
            return;

        const auto integer = Converter<Int>::Stoi(bigInt);
        value.SetData(integer);
    }

    CoercionType Coercions::TypeCoercionMatrix[Coercions::TYPE_COUNT][Coercions::TYPE_COUNT] = {};

    void Coercions::InitializeTypeCoercionMatrix() {
        using DT = DataType;
        using CT = CoercionType;

        // 1. Default everything to NONE
        for (int i = 0; i < TYPE_COUNT; i++) {
            for (int j = 0; j < TYPE_COUNT; j++) {
                TypeCoercionMatrix[i][j] = CT::None;
            }
        }

        // 2. Identity conversions (T → T)
        for (int i = 0; i < TYPE_COUNT; i++) {
            TypeCoercionMatrix[i][i] = CT::Implicit;
        }

        // ---- Numeric ladder ----
        // TinyInt → ...
        TypeCoercionMatrix[static_cast<int>(DT::TinyInt)][static_cast<int>(DT::SmallInt)] = CT::Implicit;
        TypeCoercionMatrix[static_cast<int>(DT::TinyInt)][static_cast<int>(DT::Int)]      = CT::Implicit;
        TypeCoercionMatrix[static_cast<int>(DT::TinyInt)][static_cast<int>(DT::BigInt)]   = CT::Implicit;
        TypeCoercionMatrix[static_cast<int>(DT::TinyInt)][static_cast<int>(DT::Decimal)]  = CT::Implicit;

        // SmallInt →
        TypeCoercionMatrix[static_cast<int>(DT::SmallInt)][static_cast<int>(DT::TinyInt)] = CT::Explicit;
        TypeCoercionMatrix[static_cast<int>(DT::SmallInt)][static_cast<int>(DT::Int)]     = CT::Implicit;
        TypeCoercionMatrix[static_cast<int>(DT::SmallInt)][static_cast<int>(DT::BigInt)]  = CT::Implicit;
        TypeCoercionMatrix[static_cast<int>(DT::SmallInt)][static_cast<int>(DT::Decimal)] = CT::Implicit;

        // Int →
        TypeCoercionMatrix[static_cast<int>(DT::Int)][static_cast<int>(DT::TinyInt)]  = CT::Explicit;
        TypeCoercionMatrix[static_cast<int>(DT::Int)][static_cast<int>(DT::SmallInt)] = CT::Explicit;
        TypeCoercionMatrix[static_cast<int>(DT::Int)][static_cast<int>(DT::BigInt)]   = CT::Implicit;
        TypeCoercionMatrix[static_cast<int>(DT::Int)][static_cast<int>(DT::Decimal)]  = CT::Implicit;

        // BigInt →
        TypeCoercionMatrix[static_cast<int>(DT::BigInt)][static_cast<int>(DT::TinyInt)]  = CT::Explicit;
        TypeCoercionMatrix[static_cast<int>(DT::BigInt)][static_cast<int>(DT::SmallInt)] = CT::Explicit;
        TypeCoercionMatrix[static_cast<int>(DT::BigInt)][static_cast<int>(DT::Int)]      = CT::Explicit;
        TypeCoercionMatrix[static_cast<int>(DT::BigInt)][static_cast<int>(DT::Decimal)]  = CT::Implicit;

        // Decimal →
        TypeCoercionMatrix[static_cast<int>(DT::Decimal)][static_cast<int>(DT::TinyInt)]  = CT::Explicit;
        TypeCoercionMatrix[static_cast<int>(DT::Decimal)][static_cast<int>(DT::SmallInt)] = CT::Explicit;
        TypeCoercionMatrix[static_cast<int>(DT::Decimal)][static_cast<int>(DT::Int)]      = CT::Explicit;
        TypeCoercionMatrix[static_cast<int>(DT::Decimal)][static_cast<int>(DT::BigInt)]   = CT::Explicit;

        // ---- String conversions ----
        for (int t = static_cast<int>(DT::TinyInt); t <= static_cast<int>(DT::Decimal); t++) {
            TypeCoercionMatrix[static_cast<int>(DT::String)][t] = CT::Explicit; // parse
            TypeCoercionMatrix[t][static_cast<int>(DT::String)] = CT::Implicit; // stringify
        }

        TypeCoercionMatrix[static_cast<int>(DT::String)][static_cast<int>(DT::Bool)]     = CT::Explicit;
        TypeCoercionMatrix[static_cast<int>(DT::Bool)][static_cast<int>(DT::String)]     = CT::Implicit;
        TypeCoercionMatrix[static_cast<int>(DT::String)][static_cast<int>(DT::DateTime)] = CT::Explicit;
        TypeCoercionMatrix[static_cast<int>(DT::DateTime)][static_cast<int>(DT::String)] = CT::Implicit;
        TypeCoercionMatrix[static_cast<int>(DT::String)][static_cast<int>(DT::Guid)]     = CT::Explicit;
        TypeCoercionMatrix[static_cast<int>(DT::Guid)][static_cast<int>(DT::String)]     = CT::Implicit;

        // ---- Bool conversions ----
        TypeCoercionMatrix[static_cast<int>(DT::Bool)][static_cast<int>(DT::TinyInt)]  = CT::Implicit;
        TypeCoercionMatrix[static_cast<int>(DT::Bool)][static_cast<int>(DT::SmallInt)] = CT::Implicit;
        TypeCoercionMatrix[static_cast<int>(DT::Bool)][static_cast<int>(DT::Int)]      = CT::Implicit;
        TypeCoercionMatrix[static_cast<int>(DT::Bool)][static_cast<int>(DT::BigInt)]   = CT::Implicit;

        // ---- JSON rules ----
        for (int t = 0; t < TYPE_COUNT; t++) {
            if (t == static_cast<int>(DT::Json)) continue;
            TypeCoercionMatrix[t][static_cast<int>(DT::Json)] = CT::Implicit;
        }
        TypeCoercionMatrix[static_cast<int>(DT::Json)][static_cast<int>(DT::String)] = CT::Explicit;
    }

    void Coercions::Initialize() {
        Coercions::InitializeTypeCoercionMatrix();
    }

    bool Coercions::IsCoercionAllowed(
        const DataType fromType,
        const DataType toType,
        const bool explicitCast
    ) {
        const auto coercionType = GetCoercionType(fromType, toType);
        return explicitCast
            ? (coercionType == CoercionType::Implicit || coercionType == CoercionType::Explicit)
            : (coercionType == CoercionType::Implicit);
    }

    bool Coercions::ToBool(const Value& value, const bool explicitCast) {
        const auto valueType = value.GetType();
        switch (valueType) {
        case DataType::TinyInt:
            return Converter<bool>::Stoi(value.AsTinyInt());
        case DataType::SmallInt:
            return Converter<bool>::Stoi(value.AsSmallInt());
        case DataType::Int:
            return Converter<bool>::Stoi(value.AsInt());
        case DataType::BigInt:
            return Converter<bool>::Stoi(value.AsBigInt());
        case DataType::Decimal:
            return false;
        case DataType::String:
            return value.ParseAsBoolFromString();
        case DataType::Bool:
            return *reinterpret_cast<const bool*>(value.Data());
        default:
            Coercions::ThrowException(valueType);
        }
        return false;
    }

    TinyInt Coercions::ToTinyInt(const Value& value, const bool explicitCast) {
        const auto valueType = value.GetType();
        switch (valueType) {
        case DataType::TinyInt:
            return *reinterpret_cast<const TinyInt*>(value.Data());
        case DataType::SmallInt:
            return Converter<TinyInt>::Stoi(value.AsSmallInt());
        case DataType::Int:
            return Converter<TinyInt>::Stoi(value.AsInt());
        case DataType::BigInt:
            return Converter<TinyInt>::Stoi(value.AsBigInt());
        case DataType::Decimal:
            return 0;
        case DataType::String:
            return Converter<TinyInt>::Stoi(value.AsString());
        case DataType::Bool:
            return value.AsBool() ? 1 : 0;
        default:
            Coercions::ThrowException(valueType);
        }
        return -1;
    }

    SmallInt Coercions::ToSmallInt(const Value& value, const bool explicitCast) {
        const auto valueType = value.GetType();
        switch (valueType) {
        case DataType::TinyInt:
            return *reinterpret_cast<const TinyInt*>(value.Data());
        case DataType::SmallInt:
            return *reinterpret_cast<const SmallInt*>(value.Data());
        case DataType::Int:
            return Converter<SmallInt>::Stoi(value.AsInt());
        case DataType::BigInt:
            return Converter<SmallInt>::Stoi(value.AsBigInt());
        case DataType::Decimal:
            return 0;
        case DataType::String:
            return Converter<SmallInt>::Stoi(value.AsString());
        case DataType::Bool:
            return value.AsBool() ? 1 : 0;
        default:
            Coercions::ThrowException(valueType);
        }
        return -1;
    }

    Int Coercions::ToInt(const Value& value, const bool explicitCast) {
        const auto valueType = value.GetType();
        switch (valueType) {
        case DataType::TinyInt:
            return *reinterpret_cast<const TinyInt*>(value.Data());
        case DataType::SmallInt:
            return *reinterpret_cast<const SmallInt*>(value.Data());
        case DataType::Int:
            return *reinterpret_cast<const Int*>(value.Data());
        case DataType::BigInt:
            return Converter<Int>::Stoi(value.AsBigInt());
        case DataType::Decimal:
            return 0;
        case DataType::String:
            return Converter<Int>::Stoi(value.AsStringView());
        case DataType::Bool:
            return value.AsBool() ? 1 : 0;
        default:
            Coercions::ThrowException(valueType);
        }
        return -1;
    }

    BigInt Coercions::ToBigInt(const Value& value, const bool explicitCast) {
        const auto valueType = value.GetType();
        switch (valueType) {
        case DataType::TinyInt:
            return *reinterpret_cast<const TinyInt*>(value.Data());
        case DataType::SmallInt:
            return *reinterpret_cast<const SmallInt*>(value.Data());
        case DataType::Int:
            return *reinterpret_cast<const Int*>(value.Data());
        case DataType::BigInt:
            return *reinterpret_cast<const BigInt*>(value.Data());
        case DataType::Decimal:
            return 0;
        case DataType::String:
            return Converter<BigInt>::Stoi(value.AsStringView());
        case DataType::Bool:
            return value.AsBool() ? 1 : 0;
        default:
            Coercions::ThrowException(valueType);
        }
        return -1;
    }

    String Coercions::ToString(const Value& value, const bool explicitCast) {
        const auto valueType = value.GetType();
        switch (valueType) {
        case DataType::TinyInt:
            return Converter<TinyInt>::Itos(value.AsTinyInt(), value.GetAllocator());
        case DataType::SmallInt:
            return Converter<SmallInt>::Itos(value.AsSmallInt(), value.GetAllocator());
        case DataType::Int:
            return Converter<Int>::Itos(value.AsInt(), value.GetAllocator());
        case DataType::BigInt:
            return Converter<BigInt>::Itos(value.AsBigInt(), value.GetAllocator());
        case DataType::Decimal:
            return value.AsDecimal().ToString(value.GetAllocator());
        case DataType::String:
            return String(value.Data(), value.Size(), value.GetAllocator());
        case DataType::Bool: {
            const auto* str = value.AsBool() ? "true" : "false";
            return String(str, value.GetAllocator());
        }
        case DataType::DateTime:
            return value.AsDateTime().ToString(value.GetAllocator());
        case DataType::Guid:
            return value.AsGuid().ToString(value.GetAllocator());
        case DataType::RowIdentifier:
        case DataType::Unknown:
        default:
            Coercions::ThrowException(valueType);
        }
        return String(nullptr);
    }

    StringView Coercions::ToStringView(const Value& value, bool explicitCast) {
        const auto valueType = value.GetType();
        switch (valueType) {
        case DataType::String:
            return StringView(reinterpret_cast<const char*>(value.Data()), value.Size());
        case DataType::TinyInt:
        case DataType::SmallInt:
        case DataType::Int:
        case DataType::BigInt:
        case DataType::Decimal:
        case DataType::Unknown:
        case DataType::Bool:
        case DataType::DateTime:
        case DataType::Guid:
        case DataType::RowIdentifier:
        default:
            Coercions::ThrowException(valueType);
        }
        return StringView(nullptr);
    }

    Guid Coercions::ToGuid(const Value& value, const bool explicitCast) {
        const auto valueType = value.GetType();
        switch (valueType) {
        case DataType::Guid:
            return Guid(value.Data(), value.Size());
        case DataType::String:
            return Guid::Parse(value.AsStringView());
        default:
            Coercions::ThrowException(valueType);
        }
        return Guid::Empty();
    }

    DateTime Coercions::ToDateTime(const Value& value, const bool explicitCast) {
        const auto valueType = value.GetType();
        switch (valueType) {
        case DataType::String: {
            DateTime date;
            DateTime::FromString(date, value.AsStringView());
            return date;
        }
        case DataType::DateTime:
            return DateTime(*reinterpret_cast<const BigInt*>(value.Data()));
        default:
            Coercions::ThrowException(valueType);
        }
        return DateTime::Now();
    }

    Decimal Coercions::ToDecimal(const Value& value, const bool explicitCast) {
        const auto valueType = value.GetType();
        switch (valueType) {
        case DataType::TinyInt:
            return Decimal(value.AsTinyInt());
        case DataType::SmallInt:
            return Decimal(value.AsSmallInt());
        case DataType::Int:
            return Decimal(value.AsInt());
        case DataType::BigInt:
            return Decimal(value.AsBigInt());
        case DataType::Decimal:
            return Decimal(value.Data(), value.Size());
        case DataType::String:
            return Decimal(value.AsStringView());
        case DataType::Bool:
            return Decimal(value.AsBool());
        case DataType::DateTime:
        case DataType::Guid:
        case DataType::RowIdentifier:
        case DataType::Unknown:
        default:
            Coercions::ThrowException(valueType);
        }
        return Decimal();
    }

    JsonBinary Coercions::ToJsonBinary(const Value& value, bool explicitCast){
        const auto valueType = value.GetType();
        switch (valueType){
        case DataType::Json:
            return JsonBinary(value.GetAllocator(), value.Data(), value.Size());
        case DataType::String:{
            const auto view = value.AsStringView();
            Serialization::JsonParser parser(value.GetAllocator(), view);
            return parser.Parse();
        }
        default:
            Coercions::ThrowException(valueType);
        }

        return JsonBinary(value.GetAllocator());
    }

    bool Coercions::CanBeParsedToType(const DataType toType, const Value& value) {
        const auto valueType = value.GetType();
        const auto coercionType = GetCoercionType(valueType, toType);

        if (coercionType == CoercionType::None)
            return false;

        if (coercionType == CoercionType::Implicit)
            return true;

        switch (toType) {
        case DataType::TinyInt:
            return Coercions::CanGetTinyInt(value);
        case DataType::SmallInt:
            return Coercions::CanGetSmallInt(value);
        case DataType::Int:
            return Coercions::CanGetInt(value);
        case DataType::BigInt:
            return Coercions::CanGetBigInt(value);
        case DataType::Decimal:
            return Coercions::CanGetDecimal(value);
        case DataType::String:
            return Coercions::CanGetString(value);
        case DataType::Json:
            return Coercions::CanGetJsonBinary(value);
        case DataType::Bool:
            return Coercions::CanGetBool(value);
        case DataType::DateTime:
            return Coercions::CanGetDateTime(value);
        case DataType::Guid:
            return Coercions::CanGetGuid(value);
        default:
            return false;
        }
    }

    void Coercions::DeduceIntegerType(Value& value) {
        switch (value.GetType()) {
        case DataType::SmallInt:
            return Coercions::DownCastFromSmallInt(value);
        case DataType::Int:
            return Coercions::DownCastFromInt(value);
        case DataType::BigInt:
            return Coercions::DownCastFromBigInt(value);
        case DataType::TinyInt:
        default:
            break;
        }
    }
}
