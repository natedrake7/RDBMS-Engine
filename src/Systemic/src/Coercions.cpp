#include "../include/Coercions.h"

#include "../include/Converter.h"
#include "../include/Functions/StringFunctions.h"
#include "DataTypes/DateTime.h"

namespace DataTypes{
  constexpr CoercionType Coercions::GetCoercionType(
    const DataType fromType,
    const DataType toType
  ){
    return Coercions::TypeCoercionMatrix[static_cast<Int>(fromType)][static_cast<Int>(toType)];
  }

  bool Coercions::ParseAsBoolFromString(const Value &value){
    const auto strData = Functions::String::Lower(value.AsString());

    if (TrueStrings.Contains(strData))
      return true;

    if (FalseStrings.Contains(strData))
      return false;

    return false;
  }

  bool Coercions::ParseAsBoolFromString(const Value &value, bool &outVal){
    const auto strData = Functions::String::Lower(value.AsString());

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

  bool Coercions::CanGetTinyInt(const Value &value){
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
      return Converter<TinyInt>::TryStoi(value.AsString());
    case DataType::UnicodeString:
      return Converter<TinyInt>::TryStoi(value.AsUnicodeString());
    default:
      return false;
    }
  }

  bool Coercions::CanGetBool(const Value &value){
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

  bool Coercions::CanGetSmallInt(const Value &value){
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
      return Converter<SmallInt>::TryStoi(value.AsString());
    case DataType::UnicodeString:
      return Converter<SmallInt>::TryStoi(value.AsUnicodeString());
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
      return Converter<Int>::TryStoi(value.AsBigInt());
    case DataType::Decimal:
      return false;
    case DataType::String:
      return Converter<Int>::TryStoi(value.AsString());
    case DataType::UnicodeString:
      return Converter<Int>::TryStoi(value.AsUnicodeString());
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
      return Converter<int64_t>::TryStoi(value.AsString());
    case DataType::UnicodeString:
      return Converter<int64_t>::TryStoi(value.AsUnicodeString());
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
      return Guid::Validate(value.AsString());
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
      return DateTime::FromString(value.AsString());
    default:
      return false;
    }
  }

  bool Coercions::CanGetDecimal(const Value &value){
    return false;
  }

  void Coercions::DownCastFromSmallInt(Value &value){
    const auto smallInt = value.AsSmallInt();
    if (!Converter<TinyInt>::TryStoi(smallInt))
      return;

    const auto tinyInt = Converter<TinyInt>::Stoi(smallInt);
    value.SetData(tinyInt);
  }

  void Coercions::DownCastFromInt(Value &value){
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

  void Coercions::DownCastFromBigInt(Value &value){
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

  bool Coercions::IsCoercionAllowed(
    const DataType fromType,
    const DataType toType,
    const bool explicitCast
  ){
    const auto coercionType = GetCoercionType(fromType, toType);
    return explicitCast
             ? (coercionType == CoercionType::Implicit || coercionType == CoercionType::Explicit)
             : (coercionType == CoercionType::Implicit);
  }

  bool Coercions::ToBool(const Value &value, const bool explicitCast){
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
    case DataType::UnicodeString:
      return Converter<TinyInt>::Stoi(value.AsUnicodeString());
    case DataType::Bool:
      return *reinterpret_cast<const bool*>(value.Data());
    default: {
      if (valueType == DataType::Unknown)
        throw std::invalid_argument("Invalid Field Type");
      throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(valueType) + " cannot be coerced to Bool");
    }
    }
  }

TinyInt Coercions::ToTinyInt(const Value &value, const bool explicitCast){
  const auto valueType = value.GetType();
  switch (valueType) {
  case DataType::TinyInt:
    return *reinterpret_cast<const TinyInt *>(value.Data());
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
  case DataType::UnicodeString:
    return Converter<TinyInt>::Stoi(value.AsUnicodeString());
  case DataType::Bool:
    return value.AsBool() ? 1 : 0;
  default: {
    if (valueType == DataType::Unknown)
      throw std::invalid_argument("Invalid Field Type");

    throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(valueType) + " cannot be coerced to Tiny Int");
  }
  }
}

  SmallInt Coercions::ToSmallInt(const Value &value, const bool explicitCast){
    const auto valueType = value.GetType();
    switch (valueType) {
    case DataType::TinyInt:
      return *reinterpret_cast<const TinyInt *>(value.Data());
    case DataType::SmallInt:
      return *reinterpret_cast<const SmallInt *>(value.Data());
    case DataType::Int:
      return Converter<SmallInt>::Stoi(value.AsInt());
    case DataType::BigInt:
      return Converter<SmallInt>::Stoi(value.AsBigInt());
    case DataType::Decimal:
      return 0;
    case DataType::String:
      return Converter<SmallInt>::Stoi(value.AsString());
    case DataType::UnicodeString:
      return Converter<SmallInt>::Stoi(value.AsUnicodeString());
    case DataType::Bool:
      return value.AsBool() ? 1 : 0;
    default: {
      if (valueType == DataType::Unknown)
        throw std::invalid_argument("Invalid Field Type");

      throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(valueType) + " cannot be coerced to Small Int");
    }
    }
  }

  Int Coercions::ToInt(const Value &value, const bool explicitCast){
    const auto valueType = value.GetType();
    switch (valueType) {
    case DataType::TinyInt:
      return *reinterpret_cast<const TinyInt *>(value.Data());
    case DataType::SmallInt:
      return *reinterpret_cast<const SmallInt *>(value.Data());
    case DataType::Int:
      return *reinterpret_cast<const Int *>(value.Data());
    case DataType::BigInt:
      return Converter<Int>::Stoi(value.AsBigInt());
    case DataType::Decimal:
      return 0;
    case DataType::String:
      return Converter<Int>::Stoi(value.AsString());
    case DataType::UnicodeString:
      return Converter<Int>::Stoi(value.AsUnicodeString());
    case DataType::Bool:
      return value.AsBool() ? 1 : 0;
    default: {
      if (valueType == DataType::Unknown)
        throw std::invalid_argument("Invalid Field Type");

      throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(valueType) + " cannot be coerced to Int");
    }
    }
  }

  int64_t Coercions::ToBigInt(const Value &value, const bool explicitCast){
    const auto valueType = value.GetType();
    switch (valueType) {
    case DataType::TinyInt:
      return *reinterpret_cast<const TinyInt *>(value.Data());
    case DataType::SmallInt:
      return *reinterpret_cast<const SmallInt *>(value.Data());
    case DataType::Int:
      return *reinterpret_cast<const Int *>(value.Data());
    case DataType::BigInt:
      return *reinterpret_cast<const int64_t *>(value.Data());
    case DataType::Decimal:
      return 0;
    case DataType::String:
      return Converter<int64_t>::Stoi(value.AsString());
    case DataType::UnicodeString:
      return Converter<int64_t>::Stoi(value.AsUnicodeString());
    case DataType::Bool:
      return value.AsBool() ? 1 : 0;
    default: {
      if (valueType == DataType::Unknown)
        throw std::invalid_argument("Invalid Field Type");

      throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(valueType) + " cannot be coerced to Big Int");
    }
    }
  }

  std::string Coercions::ToString(const Value &value, const bool explicitCast){
    const auto valueType = value.GetType();
    switch (valueType) {
    case DataType::TinyInt:
      return std::to_string(value.AsTinyInt());
    case DataType::SmallInt:
      return std::to_string(value.AsSmallInt());
    case DataType::Int:
      return std::to_string(value.AsInt());
    case DataType::BigInt:
      return std::to_string(value.AsBigInt());
    case DataType::Decimal:
      return value.AsDecimal().ToString();
    case DataType::String:
    case DataType::UnicodeString:
      return{reinterpret_cast<const char*>(value.Data()), value.Size()};
    case DataType::Bool:
      return value.AsBool() ? "true" : "false";
    case DataType::DateTime:
      return value.AsDateTime().ToString();
    case DataType::Guid:
      return value.AsGuid().ToString();
    case DataType::RowIdentifier:
    case DataType::Unknown:
    default: {
      if (valueType == DataType::Unknown)
        throw std::invalid_argument("Invalid Field Type");

      throw std::runtime_error("Invalid Column type");
    }
    }
  }

  std::u16string Coercions::ToUnicodeString(const Value &value, const bool explicitCast){
    return {reinterpret_cast<const char16_t *>(value.Data()), value.Size()};
  }

  Guid Coercions::ToGuid(const Value &value, const bool explicitCast){
    const auto valueType = value.GetType();
    switch (valueType) {
    case DataType::Guid:
      return {value.Data(), value.Size()};
    case DataType::String:
    case DataType::UnicodeString:
      return Guid::FromString(value.AsString());
    default: {
      if (valueType == DataType::Unknown)
        throw std::invalid_argument("Invalid Field Type");

      throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(valueType) + " cannot be coerced to Guid");
    }
    }
  }

  DateTime Coercions::ToDateTime(const Value &value, const bool explicitCast){
    const auto valueType = value.GetType();
    switch (valueType) {
    case DataType::UnicodeString:
    case DataType::String: {
      DateTime date;
      DateTime::FromString(date, value.AsString());
      return date;
    }
    case DataType::DateTime:
      return DateTime(*reinterpret_cast<const int64_t*>(value.Data()));
    default: {
      if (valueType == DataType::Unknown)
        throw std::invalid_argument("Invalid Field Type");

      throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(valueType) + " cannot be coerced to DateTime");
    }
    }
  }

  Decimal Coercions::ToDecimal(const Value &value, const bool explicitCast){
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
    case DataType::UnicodeString:
      return Decimal(value.AsString());
    case DataType::Bool:
      return Decimal(value.AsBool());
    case DataType::DateTime:
    case DataType::Guid:
    case DataType::RowIdentifier:
    case DataType::Unknown:
    default: {
      if (valueType == DataType::Unknown)
        throw std::invalid_argument("Invalid Field Type");

      throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(valueType) + " cannot be coerced to Decimal");
    }
    }
  }

  bool Coercions::CanBeParsedToType(const DataType toType, const Value &value){
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
