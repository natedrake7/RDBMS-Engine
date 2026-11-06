#include <Systemic/Coercions/Coercions.h>

#include <Systemic/IntegerConverter.h>
#include <Systemic/Functions/StringFunctions.h>
#include <Systemic/DataTypes/DataTypes.StaticData.h>
#include <Systemic/DataTypes/DateTime.h>

namespace DataTypes {
    void Coercions::ThrowNonExistentType(const DataType type){
        if (type == DataType::Null)
            throw std::invalid_argument("Invalid Field Type");
        throw std::invalid_argument(
            "Field type "
                + std::string(Reflection::EnumIdentifier(type))
                + " does not exist"
        );
    }

    void Coercions::ThrowException(const DataType type, const DataType toType) {
        if (type == DataType::Null)
            throw std::invalid_argument("Invalid Field Type");
        throw std::invalid_argument(
            "Field type " + std::string(Reflection::EnumIdentifier(type))
            + " cannot be coerced to " + std::string(Reflection::EnumIdentifier(toType))
        );
    }

    bool Coercions::ParseAsBoolFromString(const Value& value) {
        return DataTypes::ParseBool(value.AsStringView()).value_or(false);
    }

    bool Coercions::ParseAsBoolFromString(const Value& value, bool& outVal) {
        const auto parsed = ParseBool(value.AsStringView());
        if (parsed)
            outVal = *parsed;
        return parsed.has_value();
    }

    bool Coercions::CanGetTinyInt(const Value& value) {
        switch (value.TypeInfo()) {
            case DataType::Bool:
            case DataType::TinyInt:
                return true;
            case DataType::SmallInt:
                return IntegerConverter::TryDownCast<Int8_t>(value.AsSmallInt());
            case DataType::Int:
                return IntegerConverter::TryDownCast<Int8_t>(value.AsInt());
            case DataType::BigInt:
                return IntegerConverter::TryDownCast<Int8_t>(value.AsBigInt());
            case DataType::Decimal:
                return false;
            case DataType::String:
                return IntegerConverter::TryStrToInt<Int8_t>(value.AsStringView());
            default:
                return false;
        }
    }

    bool Coercions::CanGetBool(const Value& value) {
        switch (value.TypeInfo()) {
            case DataType::TinyInt:
                return IntegerConverter::TryDownCast<bool>(value.AsTinyInt());
            case DataType::SmallInt:
                return IntegerConverter::TryDownCast<bool>(value.AsSmallInt());
            case DataType::Int:
                return IntegerConverter::TryDownCast<bool>(value.AsInt());
            case DataType::BigInt:
                return IntegerConverter::TryDownCast<bool>(value.AsBigInt());
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
        switch (value.TypeInfo()) {
            case DataType::Bool:
            case DataType::TinyInt:
            case DataType::SmallInt:
                return true;
            case DataType::Int:
                return IntegerConverter::TryDownCast<Int16_t>(value.AsInt());
            case DataType::BigInt:
                return IntegerConverter::TryDownCast<Int16_t>(value.AsBigInt());
            case DataType::Decimal:
                return false;
            case DataType::String:
                return IntegerConverter::TryStrToInt<Int16_t>(value.AsStringView());
            default:
                return false;
        }
    }

    bool Coercions::CanGetInt(const Value& value) {
        switch (value.TypeInfo()) {
            case DataType::Bool:
            case DataType::TinyInt:
            case DataType::SmallInt:
            case DataType::Int:
                return true;
            case DataType::BigInt:
                return IntegerConverter::TryDownCast<Int32_t>(value.AsBigInt());
            case DataType::Decimal:
                return false;
            case DataType::String:
                return IntegerConverter::TryStrToInt<Int32_t>(value.AsStringView());
            default:
                return false;
        }
    }

    bool Coercions::CanGetBigInt(const Value& value) {
        switch (value.TypeInfo()) {
            case DataType::Bool:
            case DataType::TinyInt:
            case DataType::SmallInt:
            case DataType::Int:
            case DataType::BigInt:
                return true;
            case DataType::Decimal:
                return false;
            case DataType::String:
                return IntegerConverter::TryStrToInt<Int64_t>(value.AsStringView());
            default:
                return false;
        }
    }

    bool Coercions::CanGetString(const Value& value) { return true; }

    bool Coercions::CanGetGuid(const Value& value) {
        switch (value.TypeInfo()) {
            case DataType::Guid:
                return true;
            case DataType::String:
                return Guid::Validate(value.AsStringView());
            default:
                return false;
        }
    }

    bool Coercions::CanGetDateTime(const Value& value) {
        switch (value.TypeInfo()) {
            case DataType::DateTime:
                return true;
            case DataType::String:
                return DateTime::FromStringView(value.AsStringView());
            default:
                return false;
        }
    }

    bool Coercions::CanGetDecimal(const Value& value) {
        return false;
    }

    bool Coercions::CanGetJsonBinary(const Value& value){
        switch (value.TypeInfo()){
        case DataType::Json:
            return true;
        case DataType::String:
            //TODO maybe convert to json binary format
            return Serialization::JsonParser::IsJson(value.AsStringView());
        default:
            return false;
        }
    }

    void Coercions::DeduceSignedIntegerType(Value& value){

    }

    bool Coercions::ToBool(const Value& value, const bool explicitCast) {
        const auto valueType = value.TypeInfo();
        switch (valueType._type) {
        case DataType::TinyInt:
            return IntegerConverter::DownCast<Int8_t, bool>(value.Data<Int8_t>());
        case DataType::SmallInt:
            return IntegerConverter::DownCast<Int16_t, bool>(value.Data<Int16_t>());
        case DataType::Int:
            return IntegerConverter::DownCast<Int32_t, bool>(value.Data<Int32_t>());
        case DataType::BigInt:
            return IntegerConverter::DownCast<Int64_t, bool>(value.Data<Int64_t>());
        case DataType::HugeInt:
            return IntegerConverter::DownCast<Int128_t, bool>(value.Data<Int128_t>());

        case DataType::UnsignedTinyInt:
            return IntegerConverter::DownCast<UInt8_t, bool>(value.Data<UInt8_t>());
        case DataType::UnsignedSmallInt:
            return IntegerConverter::DownCast<UInt16_t, bool>(value.Data<UInt16_t>());
        case DataType::UnsignedInt:
            return IntegerConverter::DownCast<UInt32_t, bool>(value.Data<UInt32_t>());
        case DataType::UnsignedBigInt:
            return IntegerConverter::DownCast<UInt64_t, bool>(value.Data<UInt64_t>());
        case DataType::UnsignedHugeInt:
            return IntegerConverter::DownCast<UInt128_t, bool>(value.Data<UInt128_t>());

        case DataType::Float:
            return IntegerConverter::DownCast<Float, bool>(value.Data<Float>());
        case DataType::Double:
            return IntegerConverter::DownCast<Double, bool>(value.Data<Double>());

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

    Int8_t Coercions::ToTinyInt(const Value& value, const bool explicitCast) {
        const auto valueType = value.TypeInfo();
        switch (valueType) {
        case DataType::TinyInt:
            return *reinterpret_cast<const Int8_t*>(value.Data());
        case DataType::SmallInt:
            return IntegerConverter::DownCast<Int8_t>(*reinterpret_cast<const Int16_t*>(value.Data()));
        case DataType::Int:
            return IntegerConverter::DownCast<Int8_t>(*reinterpret_cast<const Int32_t*>(value.Data()));
        case DataType::BigInt:
            return IntegerConverter::DownCast<Int8_t>(*reinterpret_cast<const Int64_t*>(value.Data()));
        case DataType::Decimal:
            return value.Data<Decimal>().ToInt<Int8_t>();
        case DataType::String:
            return IntegerConverter::StrToInt<Int8_t>(value.AsStringView());
        case DataType::Bool:
            return value.AsBool();
        default:
            Coercions::ThrowException(valueType, DataType::TinyInt);
        }
        return -1;
    }

    Int16_t Coercions::ToSmallInt(const Value& value, const bool explicitCast) {
        const auto valueType = value.TypeInfo();
        switch (valueType) {
        case DataType::TinyInt:
            return value.Data<Int8_t>();
        case DataType::SmallInt:
            return value.Data<Int16_t>();
        case DataType::Int:
            return value.Data<Int32_t>();
        case DataType::BigInt:
            return IntegerConverter::DownCast<Int16_t>(value.Data<Int64_t>());
        case DataType::Decimal:
            return value.Data<Decimal>().ToInt<Int16_t>();
        case DataType::String:
            return IntegerConverter::StrToInt<Int16_t>(value.AsStringView());
        case DataType::Bool:
            return value.AsBool();
        default:
            Coercions::ThrowException(valueType, DataType::SmallInt);
        }
        return -1;
    }

    Int32_t Coercions::ToInt(const Value& value, const bool explicitCast) {
        const auto valueType = value.TypeInfo();
        switch (valueType) {
        case DataType::TinyInt:
            return value.Data<Int8_t>();
        case DataType::SmallInt:
            return value.Data<Int16_t>();
        case DataType::Int:
            return value.Data<Int32_t>();
        case DataType::BigInt:
            return IntegerConverter::DownCast<Int32_t>(value.Data<Int64_t>());
        case DataType::Decimal:
            return value.Data<Decimal>().ToInt<Int32_t>();
        case DataType::String:
            return IntegerConverter::StrToInt<Int32_t>(value.AsStringView());
        case DataType::Bool:
            return value.AsBool();
        default:
            Coercions::ThrowException(valueType, DataType::Int);
        }
        return -1;
    }

    Int64_t Coercions::ToBigInt(const Value& value, const bool explicitCast) {
        const auto valueType = value.TypeInfo();
        switch (valueType) {
        case DataType::TinyInt:
            return value.Data<Int8_t>();
        case DataType::SmallInt:
            return value.Data<Int16_t>();
        case DataType::Int:
            return value.Data<Int32_t>();
        case DataType::BigInt:
            return value.Data<Int64_t>();
        case DataType::Decimal:
            return value.Data<Decimal>().ToInt<Int64_t>();
        case DataType::String:
            return IntegerConverter::StrToInt<Int64_t>(value.AsStringView());
        case DataType::Bool:
            return value.AsBool();
        default:
            Coercions::ThrowException(valueType, DataType::BigInt);
        }
        return -1;
    }

    String Coercions::ToString(const ::Memory::IAllocator* allocator, const Value& value, const bool explicitCast) {
        const auto valueType = value.TypeInfo();
        switch (valueType) {
        case DataType::TinyInt:
            return IntegerConverter::IntToStr<Int8_t>(value.Data<Int8_t>(), allocator);
        case DataType::SmallInt:
            return IntegerConverter::IntToStr<Int16_t>(value.Data<Int16_t>(), allocator);
        case DataType::Int:
            return IntegerConverter::IntToStr<Int32_t>(value.Data<Int32_t>(), allocator);
        case DataType::BigInt:
            return IntegerConverter::IntToStr<Int64_t>(value.Data<Int64_t>(), allocator);
        case DataType::Decimal:
            return value.Data<Decimal>().ToString(allocator);
        case DataType::String:
            return String(value.Data(), value.Size(), allocator);
        case DataType::Bool: {
            const auto* str = value.Data<bool>()
                    ? "true"
                    : "false";

            return String(str, allocator);
        }
        case DataType::DateTime:
            return value.Data<DateTime>().ToString(allocator);
        case DataType::Guid:
            return value.Data<Guid>().ToString(allocator);
        case DataType::Null:
        default:
            Coercions::ThrowException(valueType, DataType::String);
        }
        return String(nullptr);
    }

    StringView Coercions::ToStringView(const Value& value, bool explicitCast) {
        const auto valueType = value.TypeInfo();
        switch (valueType) {
        case DataType::String:
            return StringView(reinterpret_cast<const char*>(value.Data()), value.Size());
        case DataType::Json:
            // return StringView::ViewOf(JsonBinary(allocator, value.Data(), value.Size())
            //         .ToString()
            // );
        case DataType::TinyInt:
        case DataType::SmallInt:
        case DataType::Int:
        case DataType::BigInt:
        case DataType::Decimal:
        case DataType::Null:
        case DataType::Bool:
        case DataType::DateTime:
        case DataType::Guid:
        default:
            Coercions::ThrowException(valueType, DataType::String);
        }
        return StringView(nullptr);
    }

    Guid Coercions::ToGuid(const Value& value, const bool explicitCast) {
        const auto valueType = value.TypeInfo();
        switch (valueType) {
        case DataType::Guid:
            return Guid(value.Data());
        case DataType::String:
            return Guid::Parse(value.AsStringView());
        default:
            Coercions::ThrowException(valueType, DataType::Guid);
        }
        return Guid::Empty();
    }

    DateTime Coercions::ToDateTime(const Value& value, const bool explicitCast) {
        const auto valueType = value.TypeInfo();
        switch (valueType) {
        case DataType::String: {
            DateTime date;
            DateTime::FromStringView(date, value.AsStringView());
            return date;
        }
        case DataType::DateTime:
            return DateTime(*reinterpret_cast<const Int64_t*>(value.Data()));
        default:
            Coercions::ThrowException(valueType, DataType::DateTime);
        }
        return DateTime::Now();
    }

    Decimal Coercions::ToDecimal(const Value& value, const bool explicitCast) {
        const auto valueType = value.TypeInfo();
        switch (valueType) {
        case DataType::TinyInt:
            return Decimal::FromMantissa(value.Data<Int8_t>());
        case DataType::SmallInt:
            return Decimal::FromMantissa(value.Data<Int16_t>());
        case DataType::Int:
            return Decimal::FromMantissa(value.Data<Int32_t>());
        case DataType::BigInt:
            return Decimal::FromMantissa(value.Data<Int64_t>());
        case DataType::Decimal:
            return value.Data<Decimal>();
        case DataType::String:
            return Decimal::(value.AsStringView());
        case DataType::Bool:
            return Decimal::FromMantissa(value.AsBool());
        case DataType::DateTime:
        case DataType::Guid:
        case DataType::Null:
        default:
            Coercions::ThrowException(valueType, DataType::Decimal);
        }
        return Decimal();
    }

    JsonBinary Coercions::ToJsonBinary(const ::Memory::IAllocator* allocator, const Value& value, bool explicitCast){
        const auto valueType = value.TypeInfo();
        switch (valueType){
        case DataType::Json:
            return JsonBinary(allocator, value.Data(), value.Size());
        case DataType::String:{
            auto view = value.AsStringView();
            Serialization::JsonParser parser(allocator, std::move(view));
            return parser.Parse();
        }
        default:
            Coercions::ThrowException(valueType, DataType::Json);
        }

        return JsonBinary(allocator);
    }

    bool Coercions::CanBeParsedToType(const DataType toType, const Value& value) {
        const auto valueType = value.TypeInfo();
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
        const auto dataType = value.TypeInfo()._type;
        if (Detail::IsSignedIntegerType(dataType))
            DeduceSignedIntegerType(value);
        else if (Detail::IsUnsignedIntegerType(dataType))
            DeduceUnsignedIntegerType(value);
    }
}
