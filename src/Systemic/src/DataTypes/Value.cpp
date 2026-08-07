#include "../../include/DataTypes/Value.h"

#include "../../include/Coercions/Coercions.h"
#include "../../include/Functions/StringFunctions.h"

#include <stdexcept>

#include "Comparators.h"
#include "DataTypes/DateTime.h"
#include "../../include/Memory/IAllocator.h"
#include "DataTypes/DataTypes.StaticData.h"

long double Value::InterpolateString() const{
    // const auto str = this->AsString();
    //
    // constexpr Int MAX_PREFIX_LEN = 8;  // Use first 8 characters
    // constexpr double BASE = 256.0;        // ASCII character set
    //
    // const auto length = std::min(str.Size(), MAX_PREFIX_LEN);
    //
    // long double result = 0.0;
    // for (int i = 0;i < length; i++){
    //     const auto charValue = static_cast<unsigned char>(str[i]);
    //     const auto weight = std::pow(BASE, MAX_PREFIX_LEN - i - 1);
    //     result += charValue * weight;
    // }
    return 0;

    // return result;
}

Value::Value(
    const object_t* data,
    const Int size,
    const DataType type,
    const ::Memory::IAllocator* allocator,
    const column_index_t index
):  _size(size),
    _columnIndex(index), _type(type),
    _isNull(false){
    switch (type){
    case DataType::String:{
        auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(size));
        std::memcpy(buffer, data, size);
        this->_data._external = buffer;
        break;
    }
    case DataType::Bool:
        this->WriteInline(*reinterpret_cast<const bool*>(data));
        break;
    case DataType::TinyInt:
        this->WriteInline(*reinterpret_cast<const TinyInt*>(data));
        break;
    case DataType::SmallInt:
        this->WriteInline(*reinterpret_cast<const SmallInt*>(data));
        break;
    case DataType::Int:
        this->WriteInline(*reinterpret_cast<const Int*>(data));
        break;
    case DataType::BigInt:
        this->WriteInline(*reinterpret_cast<const BigInt*>(data));
        break;
    case DataType::Decimal:
        this->WriteInline(DataTypes::Decimal(data, size));
        break;
    case DataType::DateTime:
        this->WriteInline(DataTypes::DateTime(*reinterpret_cast<const BigInt*>(data)));
        break;
    case DataType::Guid:
        this->WriteInline(DataTypes::Guid(data));
        break;
    case DataType::Json:{
        auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(size));
        std::memcpy(buffer, data, size);
        this->_data._external = buffer;
        break;
    }
    default:
        throw std::runtime_error("Value::Value() called with unknown type");
    }
}

Value::Value(
    const object_t* data,
    const Int size, const DataType type,
    const column_index_t index
):  _size(size),
    _columnIndex(index), _type(type),
    _isNull(false)
{
    switch (type){
    case DataType::String:{
        this->_data._external = data;
        break;
    }
    case DataType::Bool:
        this->WriteInline(*reinterpret_cast<const bool*>(data));
        break;
    case DataType::TinyInt:
        this->WriteInline(*reinterpret_cast<const TinyInt*>(data));
        break;
    case DataType::SmallInt:
        this->WriteInline(*reinterpret_cast<const SmallInt*>(data));
        break;
    case DataType::Int:
        this->WriteInline(*reinterpret_cast<const Int*>(data));
        break;
    case DataType::BigInt:
        this->WriteInline(*reinterpret_cast<const BigInt*>(data));
        break;
    case DataType::Decimal:
        this->WriteInline(DataTypes::Decimal(data, size));
        break;
    case DataType::DateTime:
        this->WriteInline(DataTypes::DateTime(*reinterpret_cast<const BigInt*>(data)));
        break;
    case DataType::Guid:
        this->WriteInline(DataTypes::Guid(data));
        break;
    case DataType::Json:{
        this->_data._external = data;
        break;
    }
    default:
        throw std::runtime_error("Value::Value() called with unknown type");
    }
}

Value::Value(const column_index_t index)
    : _columnIndex(index) {}

Value::Value(
    const std::string& data,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(data.size()));
    std::memcpy(buffer, data.data(), data.size());
    this->_data._external = buffer;

    this->_size = data.size();

    this->_columnIndex = index;
    this->_type = DataType::String;
    this->_isNull = false;
}

Value::Value(
    const DataTypes::StringView& data,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(data.Size()));
    std::memcpy(buffer, data.Data(), data.Size());
    this->_data._external = buffer;
    this->_size = data.Size();
    this->_columnIndex = index;
    this->_type = DataType::String;
    this->_isNull = false;
}

Value::Value(
    const Serialization::JsonValue& data,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(data.Size()));
    std::memcpy(buffer, data.Data(), data.Size());
    this->_data._external = buffer;
    this->_size = data.Size();
    this->_columnIndex = index;
    this->_type = JSON_TYPES_NAMES[static_cast<Int>(data.Type())];
    this->_isNull = false;
}

// Value Value::FromMove(
//     object_t* data,
//     const Int size,
//     const DataType type,
//     const Memory::IAllocator* allocator,
//     const column_index_t index
// ){
//     return Value(data, size, type, allocator, index);
// }
//
// Value Value::FromExternalStorage(
//     const object_t* data,
//     const Int size,
//     const DataType type,
//     const Memory::IAllocator* allocator,
//     const column_index_t index
// ){
//     return Value(data, size, type, allocator, index);
// }

Value Value::FromExternalStorage(
    const object_t* data,
    const Int size, const DataType type,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    return Value(data, size, type, allocator, index);
}

Value Value::SessionValue(
    const object_t* data,
    const Int size,
    const DataType type,
    const column_index_t index
){
    return Value(data, size, type, index);
}

Value Value::Null(const column_index_t columnIndex){
    return Value(columnIndex);
}

void Value::SetColumnIndex(const column_index_t otherIndex) { this->_columnIndex = otherIndex; }

void Value::SetType(const DataType otherType){ this->_type = otherType; }

void Value::SetNull(){
    this->_isNull = true;
    this->_size = 0;
}

bool Value::IsInline() const{
    return Value::IsInline(this->_type);
}

block_size_t Value::Size() const{ return this->_size; }

const object_t* Value::Data() const{
    return this->IsInline()
        ? reinterpret_cast<const object_t*>(&this->_data)
        : this->_data._external;
}

bool Value::IsNull() const { return this->_isNull; }

column_index_t Value::GetColumnIndex() const { return this->_columnIndex;}

DataType Value::GetType() const{ return this->_type; }

bool Value::AsBool() const {
    return DataTypes::Coercions::ToBool(*this);
}

TinyInt Value::AsTinyInt() const {
    return DataTypes::Coercions::ToTinyInt(*this);
}

SmallInt Value::AsSmallInt() const {
    return DataTypes::Coercions::ToSmallInt(*this);
}

Int Value::AsInt() const {
    return DataTypes::Coercions::ToInt(*this);
}

BigInt Value::AsBigInt() const {
    return DataTypes::Coercions::ToBigInt(*this);
}

DataTypes::String Value::AsString(const ::Memory::IAllocator* allocator) const {
    return DataTypes::Coercions::ToString(allocator, *this);
}

std::string Value::AsStdString() const{
    return std::string(reinterpret_cast<const char*>(this->Data()), this->_size);
}

DataTypes::StringView Value::AsStringView() const{
    return DataTypes::Coercions::ToStringView(*this);
}

DataTypes::Decimal Value::AsDecimal() const {
    return DataTypes::Coercions::ToDecimal(*this);
}

DataTypes::DateTime Value::AsDateTime() const {
    return DataTypes::Coercions::ToDateTime(*this);
}

time_t Value::AsUnixTimeStamp() const{ return this->_data._dateTime.UnixTimeStamp(); }

DataTypes::Guid Value::AsGuid() const {
    return DataTypes::Coercions::ToGuid(*this);
}

DataTypes::JsonBinary Value::AsJson(const ::Memory::IAllocator* allocator) const{
    return DataTypes::Coercions::ToJsonBinary(allocator, *this);
}

page_id_t Value::AsLargeObjectPointer() const{
    return *reinterpret_cast<const page_id_t*>(&this->_data);
}

// std::ostream & operator<<(std::ostream& os, const Value &field){
//     if (field.IsNull()) {
//         os << "NULL";
//         return os;
//     }
//
//     switch (field._type){
//     case DataType::TinyInt:
//         os << field.AsTinyInt();
//         break;
//     case DataType::SmallInt:
//         os << field.AsSmallInt();
//         break;
//     case DataType::Int:
//         os << field.AsInt();
//         break;
//     case DataType::BigInt:
//         os << field.AsBigInt();
//         break;
//     case DataType::Decimal:
//         os << field.AsDecimal();
//         break;
//     case DataType::String:
//         os << field.AsString();
//         break;
//     case DataType::Bool:
//         os << field.AsBool();
//         break;
//     case DataType::DateTime:
//         field.AsDateTime().Print(os);
//         break;
//     case DataType::Guid:
//         os << field.AsGuid();
//         break;
//     default:
//         break;
//     }
//
//     return os;
// }

bool Value::ParseAsBoolFromString() const{
    const auto strView = this->AsStringView();

    for (const auto& str: TrueStrings)
        if (DataTypes::StringView::Compare<StringComparisonType::EqualsIgnoreCase>(str, strView))
            return true;

    for (const auto& str: FalseStrings)
        if (DataTypes::StringView::Compare<StringComparisonType::EqualsIgnoreCase>(str, strView))
            return false;

    return false;
}


long double Value::Interpolate() const{
    switch (this->_type){
    case DataType::TinyInt:
        return this->AsTinyInt();
    case DataType::SmallInt:
        return this->AsSmallInt();
    case DataType::Int:
        return this->AsInt();
    case DataType::BigInt:
        return this->AsBigInt();
    case DataType::Decimal:
        return this->AsDecimal().ToDouble();
    case DataType::String:
        return this->InterpolateString();
    case DataType::Bool:
        return this->AsBool();
    case DataType::DateTime:
        return this->AsDateTime().UnixTimeStamp();
    case DataType::Guid:
        return this->AsGuid().Interpolate();
    case DataType::RowIdentifier:
    case DataType::Null:
    default:
        throw std::runtime_error("Value::Interpolate() called with unknown type");
    }
}

bool operator<(const Value &lhs, const Value &rhs){
    return Comparators::Compare(lhs, rhs) == Comparators::Comparator::Less;
}

bool operator==(const Value &lhs, const Value &rhs){
    return Comparators::Compare(lhs, rhs) == Comparators::Comparator::Equal;
}

bool operator>(const Value &lhs, const Value &rhs){
    return Comparators::Compare(lhs, rhs) == Comparators::Comparator::Greater;
}
bool operator<=(const Value &lhs, const Value &rhs){
    return Comparators::Compare(lhs, rhs) <= Comparators::Comparator::Equal;
}

bool operator>=(const Value &lhs, const Value &rhs){
    return Comparators::Compare(lhs, rhs) >= Comparators::Comparator::Equal;
}

bool operator!=(const Value &lhs, const Value &rhs){
    return Comparators::Compare(lhs, rhs) != Comparators::Comparator::Equal;
}
