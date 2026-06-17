#include "../../include/Coercions/Coercions.h"

#include "../../include/Converter.h"
#include "../../include/Functions/StringFunctions.h"
#include "DataTypes/DataTypes.StaticData.h"
#include "DataTypes/DateTime.h"

namespace DataTypes {
    void Coercions::ThrowException(const DataType type, const DataType toType) {
        if (type == DataType::Null)
            throw std::invalid_argument("Invalid Field Type");

        const auto& typeName = SQL_TYPES_NAMES[static_cast<Int>(type)];
        const auto& toTypeName = SQL_TYPES_NAMES[static_cast<Int>(toType)];
        throw std::invalid_argument(
            "Field type " + std::string(typeName.Data(), typeName.Size())
            + " cannot be coerced to " + std::string(toTypeName.Data(), toTypeName.Size())
        );
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
        value.Set(tinyInt);
    }

    void Coercions::DownCastFromInt(Value& value) {
        const auto integer = value.AsInt();

        if (Converter<TinyInt>::TryStoi(integer)) {
            const auto tinyInt = Converter<TinyInt>::Stoi(integer);
            value.Set(tinyInt);
            return;
        }

        if (!Converter<SmallInt>::TryStoi(integer))
            return;

        const auto smallInt = Converter<SmallInt>::Stoi(integer);
        value.Set(smallInt);
    }

    void Coercions::DownCastFromBigInt(Value& value) {
        const auto bigInt = value.AsBigInt();

        if (Converter<TinyInt>::TryStoi(bigInt)) {
            const auto tinyInt = Converter<TinyInt>::Stoi(bigInt);
            value.Set(tinyInt);
            return;
        }

        if (Converter<SmallInt>::TryStoi(bigInt)) {
            const auto smallInt = Converter<SmallInt>::Stoi(bigInt);
            value.Set(smallInt);
            return;
        }

        if (!Converter<Int>::TryStoi(bigInt))
            return;

        const auto integer = Converter<Int>::Stoi(bigInt);
        value.Set(integer);
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
            Coercions::ThrowException(valueType, DataType::Bool);
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
            return value.AsBool();
        default:
            Coercions::ThrowException(valueType, DataType::TinyInt);
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
            Coercions::ThrowException(valueType, DataType::SmallInt);
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
            Coercions::ThrowException(valueType, DataType::Int);
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
            Coercions::ThrowException(valueType, DataType::BigInt);
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
        case DataType::Null:
        default:
            Coercions::ThrowException(valueType, DataType::String);
        }
        return String(nullptr);
    }

    StringView Coercions::ToStringView(const Value& value, bool explicitCast) {
        const auto valueType = value.GetType();
        switch (valueType) {
        case DataType::String:
            return StringView(reinterpret_cast<const char*>(value.Data()), value.Size());
        case DataType::Json:{
            return JsonBinary(value.GetAllocator(), value.Data(), value.Size())
                    .ToString()
                    .ToView();
        }
        case DataType::TinyInt:
        case DataType::SmallInt:
        case DataType::Int:
        case DataType::BigInt:
        case DataType::Decimal:
        case DataType::Null:
        case DataType::Bool:
        case DataType::DateTime:
        case DataType::Guid:
        case DataType::RowIdentifier:
        default:
            Coercions::ThrowException(valueType, DataType::String);
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
            Coercions::ThrowException(valueType, DataType::Guid);
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
            Coercions::ThrowException(valueType, DataType::DateTime);
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
        case DataType::Null:
        default:
            Coercions::ThrowException(valueType, DataType::Decimal);
        }
        return Decimal();
    }

    JsonBinary Coercions::ToJsonBinary(const Value& value, bool explicitCast){
        const auto valueType = value.GetType();
        switch (valueType){
        case DataType::Json:
            return JsonBinary(value.GetAllocator(), value.Data(), value.Size());
        case DataType::String:{
            auto view = value.AsStringView();
            Serialization::JsonParser parser(value.GetAllocator(), std::move(view));
            return parser.Parse();
        }
        default:
            Coercions::ThrowException(valueType, DataType::Json);
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
