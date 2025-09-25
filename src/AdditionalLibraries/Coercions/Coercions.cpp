#include "Coercions.h"

#include "../Converter/Converter.h"
#include "../Functions/StringFunctions.h"

namespace DataTypes{
  constexpr CoercionType Coercions::GetCoercionType(
    const Constants::DataType &fromType,
    const Constants::DataType &toType){
    return Coercions::TypeCoercionMatrix[static_cast<int>(fromType)][static_cast<int>(toType)];
  }

  bool Coercions::ParseAsBoolFromString(const Value &value){
    const auto strData = AdditionalLibraries::StringFunctions::Lower(value.GetString());

    if (TrueStrings.Contains(strData))
      return true;

    if (FalseStrings.Contains(strData))
      return false;

    return false;
  }

  bool Coercions::ParseAsBoolFromString(const Value &value, bool &outVal){
    const auto strData = AdditionalLibraries::StringFunctions::Lower(value.GetString());

    if (TrueStrings.Contains(strData)) {
      outVal = true;
      return true;
    }

    if (FalseStrings.Contains(strData)) {
      outVal = false;
      return true;
    }

    return false;
  }

  bool Coercions::CanGetBool(const Value &value){
    switch (value.GetType()) {
      case DataType::TinyInt:
        return Converter<bool>::TryStoi(value.GetTinyInt());
      case DataType::SmallInt:
        return Converter<bool>::TryStoi(value.GetSmallInt());
      case DataType::Int:
        return Converter<bool>::TryStoi(value.GetInt());
      case DataType::BigInt:
        return Converter<bool>::TryStoi(value.GetBigInt());
      case DataType::Decimal:
        return false;
      case DataType::String: {
      case DataType::UnicodeString:
        bool outVal = false;
        return Coercions::ParseAsBoolFromString(value, outVal);
      }
      case DataType::Bool:
        return true;
      default:
        return false;
    }
  }

  bool Coercions::CanGetTinyInt(const Value &value){
    switch (value.GetType()) {
      case DataType::Bool:
      case DataType::TinyInt:
        return true;
      case DataType::SmallInt:
        return Converter<int8_t>::TryStoi(value.GetSmallInt());
      case DataType::Int:
        return Converter<int8_t>::TryStoi(value.GetInt());
      case DataType::BigInt:
        return Converter<int8_t>::TryStoi(value.GetBigInt());
      case DataType::Decimal:
        return false;
      case DataType::String:
        return Converter<int8_t>::TryStoi(value.GetString());
      case DataType::UnicodeString:
        return Converter<int8_t>::TryStoi(value.GetUnicodeString());
      default:
        return false;
    }
  }

  bool Coercions::CanGetSmallInt(const Value &value){
    switch (value.GetType()) {
      case DataType::Bool:
      case DataType::TinyInt:
      case DataType::SmallInt:
        return true;
      case DataType::Int:
        return Converter<int16_t>::TryStoi(value.GetInt());
      case DataType::BigInt:
        return Converter<int16_t>::TryStoi(value.GetBigInt());
      case DataType::Decimal:
        return false;
      case DataType::String:
        return Converter<int16_t>::TryStoi(value.GetString());
      case DataType::UnicodeString:
        return Converter<int16_t>::TryStoi(value.GetUnicodeString());
      default:
        return false;
      }
  }

  bool Coercions::CanGetInt(const Value &value){
    switch (value.GetType()) {
      case DataType::Bool:
      case DataType::TinyInt:
      case DataType::SmallInt:
      case DataType::Int:
        return true;
      case DataType::BigInt:
        return Converter<int32_t>::TryStoi(value.GetBigInt());
      case DataType::Decimal:
        return false;
      case DataType::String:
        return Converter<int32_t>::TryStoi(value.GetString());
      case DataType::UnicodeString:
        return Converter<int32_t>::TryStoi(value.GetUnicodeString());
      default:
        return false;
    }
  }

  bool Coercions::CanGetBigInt(const Value &value){
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
        return Converter<int64_t>::TryStoi(value.GetString());
      case DataType::UnicodeString:
        return Converter<int64_t>::TryStoi(value.GetUnicodeString());
      default:
        return false;
    }
  }

  bool Coercions::CanGetString(const Value &value){ return true;}

  bool Coercions::CanGetUnicodeString(const Value &value){ return true;}

  bool Coercions::CanGetGuid(const Value &value){
    switch (value.GetType()) {
      case DataType::Guid:
          return true;
      case DataType::String:
      case DataType::UnicodeString:
        return Guid::Validate(value.GetString());
      default:
        return false;
    }
  }

  bool Coercions::CanGetDateTime(const Value &value){
    switch (value.GetType()) {
      case DataType::DateTime:
        return true;
      case DataType::String:
      case DataType::UnicodeString:
        return DateTime::FromString(value.GetString());
      default:
        return false;
    }
  }

  bool Coercions::CanGetDecimal(const Value &value){
    return false;
  }

  bool Coercions::IsCoercionAllowed(
    const DataType &fromType,
    const DataType &toType,
    const bool &explicitCast) {
      const auto coercionType = GetCoercionType(fromType, toType);
      return explicitCast
          ? (coercionType == CoercionType::Implicit || coercionType == CoercionType::Explicit)
          : (coercionType == CoercionType::Implicit);
  }

  bool Coercions::ToBool(const Value &value, const bool &explicitCast){
    const auto valueType = value.GetType();
    switch (valueType) {
      case DataType::TinyInt:
        return Converter<bool>::Stoi(value.GetTinyInt());
      case DataType::SmallInt:
        return Converter<bool>::Stoi(value.GetSmallInt());
      case DataType::Int:
        return Converter<bool>::Stoi(value.GetInt());
      case DataType::BigInt:
        return Converter<bool>::Stoi(value.GetBigInt());
      case DataType::Decimal:
        return false;
      case DataType::String:
        return value.ParseAsBoolFromString();
      case DataType::UnicodeString:
        return Converter<int8_t>::Stoi(value.GetUnicodeString());
      case DataType::Bool:
        return *reinterpret_cast<const bool*>(value.GetRawData());
      default:
        throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(valueType) + " cannot be coerced to Bool");
    }
  }

  int8_t Coercions::ToTinyInt(const Value &value, const bool &explicitCast){
    const auto valueType = value.GetType();
    switch (valueType) {
      case DataType::TinyInt:
        return *reinterpret_cast<const int8_t *>(value.GetRawData());
      case DataType::SmallInt:
        return Converter<int8_t>::Stoi(value.GetSmallInt());
      case DataType::Int:
        return Converter<int8_t>::Stoi(value.GetInt());
      case DataType::BigInt:
        return Converter<int8_t>::Stoi(value.GetBigInt());
      case DataType::Decimal:
        return 0;
      case DataType::String:
        return Converter<int8_t>::Stoi(value.GetString());
      case DataType::UnicodeString:
        return Converter<int8_t>::Stoi(value.GetUnicodeString());
      case DataType::Bool:
        return value.GetBool() ? 1 : 0;
      default:
        throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(valueType) + " cannot be coerced to Tiny Int");
    }
  }

  int16_t Coercions::ToSmallInt(const Value &value, const bool &explicitCast){
    const auto valueType = value.GetType();
    switch (valueType) {
      case DataType::TinyInt:
        return *reinterpret_cast<const int8_t *>(value.GetRawData());
      case DataType::SmallInt:
        return *reinterpret_cast<const int16_t *>(value.GetRawData());
      case DataType::Int:
        return Converter<int16_t>::Stoi(value.GetInt());
      case DataType::BigInt:
        return Converter<int16_t>::Stoi(value.GetBigInt());
      case DataType::Decimal:
        return 0;
      case DataType::String:
        return Converter<int16_t>::Stoi(value.GetString());
      case DataType::UnicodeString:
        return Converter<int16_t>::Stoi(value.GetUnicodeString());
      case DataType::Bool:
        return value.GetBool() ? 1 : 0;
      default:
        throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(valueType) + " cannot be coerced to Small Int");
    }
  }

  int32_t Coercions::ToInt(const Value &value, const bool &explicitCast){
    const auto valueType = value.GetType();
    switch (valueType) {
      case DataType::TinyInt:
        return *reinterpret_cast<const int8_t *>(value.GetRawData());
      case DataType::SmallInt:
        return *reinterpret_cast<const int16_t *>(value.GetRawData());
      case DataType::Int:
        return *reinterpret_cast<const int32_t *>(value.GetRawData());
      case DataType::BigInt:
        return Converter<int32_t>::Stoi(value.GetBigInt());
      case DataType::Decimal:
        return 0;
      case DataType::String:
        return Converter<int32_t>::Stoi(value.GetString());
      case DataType::UnicodeString:
        return Converter<int32_t>::Stoi(value.GetUnicodeString());
      case DataType::Bool:
        return value.GetBool() ? 1 : 0;
      default:
        throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(valueType) + " cannot be coerced to Int");
    }
  }

int64_t Coercions::ToBigInt(const Value &value, const bool &explicitCast){
    const auto valueType = value.GetType();
    switch (valueType) {
      case DataType::TinyInt:
        return *reinterpret_cast<const int8_t *>(value.GetRawData());
      case DataType::SmallInt:
        return *reinterpret_cast<const int16_t *>(value.GetRawData());
      case DataType::Int:
        return *reinterpret_cast<const int32_t *>(value.GetRawData());
      case DataType::BigInt:
        return *reinterpret_cast<const int64_t *>(value.GetRawData());
      case DataType::Decimal:
        return 0;
      case DataType::String:
        return Converter<int64_t>::Stoi(value.GetString());
      case DataType::UnicodeString:
        return Converter<int64_t>::Stoi(value.GetUnicodeString());
      case DataType::Bool:
        return value.GetBool() ? 1 : 0;
      default:
        throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(valueType) + " cannot be coerced to Big Int");
    }
  }

  std::string Coercions::ToString(const Value &value, const bool &explicitCast){
    const auto valueType = value.GetType();
    switch (valueType) {
      case DataType::TinyInt:
        return std::to_string(value.GetTinyInt());
      case DataType::SmallInt:
        return std::to_string(value.GetSmallInt());
      case DataType::Int:
        return std::to_string(value.GetInt());
      case DataType::BigInt:
        return std::to_string(value.GetBigInt());
      case DataType::Decimal:
        return value.GetDecimal().ToString();
      case DataType::String:
      case DataType::UnicodeString:
        return{reinterpret_cast<const char*>(value.GetRawData()), value.GetSize()};
      case DataType::Bool:
        return value.GetBool() ? "true" : "false";
      case DataType::DateTime:
        return value.GetDateTime().ToString();
      case DataType::Guid:
        return value.GetGuid().ToString();
      case DataType::RowIdentifier:
      case DataType::Invalid:
      default:
        throw runtime_error("Invalid Column type");
    }
  }

  u16string Coercions::ToUnicodeString(const Value &value, const bool &explicitCast){
      return {reinterpret_cast<const char16_t *>(value.GetRawData()), value.GetSize()};
  }

  Guid Coercions::ToGuid(const Value &value, const bool &explicitCast){
    const auto valueType = value.GetType();
    switch (valueType) {
      case DataType::Guid:
        return {value.GetRawData(), value.GetSize()};
      case DataType::String:
      case DataType::UnicodeString:
        return Guid::FromString(value.GetString());
      default:
        throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(valueType) + " cannot be coerced to Guid");
    }
  }

  DateTime Coercions::ToDateTime(const Value &value, const bool &explicitCast){
    const auto valueType = value.GetType();
    switch (valueType) {
    case DataType::UnicodeString:
    case DataType::String: {
      DateTime date;
      DateTime::FromString(date, value.GetString());
      return date;
    }
    case DataType::DateTime:
      return DateTime(*reinterpret_cast<const time_t *>(value.GetRawData()));
    default:
      throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(valueType) + " cannot be coerced to Guid");
    }
  }

  Decimal Coercions::ToDecimal(const Value &value, const bool &explicitCast){
    const auto valueType = value.GetType();

    switch (valueType) {
      case DataType::TinyInt:
        break;
      case DataType::SmallInt:
        break;
      case DataType::Int:
        break;
      case DataType::BigInt:
        break;
      case DataType::Decimal:
        return Decimal(value.GetRawData(), value.GetSize());
      case DataType::String:
      case DataType::UnicodeString:
        return Decimal(value.GetString());
      case DataType::Bool:
        break;
      case DataType::DateTime:
      case DataType::Guid:
      case DataType::RowIdentifier:
      case DataType::Invalid:
      default:
        throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(valueType) + " cannot be coerced to Decimal");

    }
  }

  bool Coercions::CanBeParsedToType(const Constants::DataType &toType, const Value &value){
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
        case DataType::UnicodeString:
          return Coercions::CanGetUnicodeString(value);
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

  void Coercions::DownCastFromSmallInt(Value &value){
    const auto smallInt = value.GetSmallInt();
    if (!Converter<int8_t>::TryStoi(smallInt))
      return;

    const auto tinyInt = Converter<int8_t>::Stoi(smallInt);
    value.SetData(tinyInt);
  }

  void Coercions::DownCastFromInt(Value &value){
    const auto integer = value.GetInt();

    if (Converter<int8_t>::TryStoi(integer)) {
      const auto tinyInt = Converter<int8_t>::Stoi(integer);
      value.SetData(tinyInt);
      return;
    }

    if (!Converter<int16_t>::TryStoi(integer))
      return;

    const auto smallInt = Converter<int16_t>::Stoi(integer);
    value.SetData(smallInt);
  }

  void Coercions::DownCastFromBigInt(Value &value){
    const auto bigInt = value.GetBigInt();

    if (Converter<int8_t>::TryStoi(bigInt)) {
      const auto tinyInt = Converter<int8_t>::Stoi(bigInt);
      value.SetData(tinyInt);
      return;
    }

    if (Converter<int16_t>::TryStoi(bigInt)) {
      const auto smallInt = Converter<int16_t>::Stoi(bigInt);
      value.SetData(smallInt);
      return;
    }

    if (!Converter<int32_t>::TryStoi(bigInt))
      return;

    const auto integer = Converter<int32_t>::Stoi(bigInt);
    value.SetData(integer);
  }

  void Coercions::DeduceIntegerType(Value &value){
    switch (value.GetType()) {
      case DataType::SmallInt:
        return Coercions::DownCastFromSmallInt(value);
      case DataType::Int:
        return Coercions::DownCastFromInt(value);
      case DataType::BigInt:
        return Coercions::DownCastFromBigInt(value);
      case DataType::TinyInt:
      default:
        return;
    }
  }
}