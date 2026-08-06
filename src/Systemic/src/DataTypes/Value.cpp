#include "../../include/DataTypes/Value.h"

#include <cmath>

#include "../../include/Coercions/Coercions.h"
#include "../../include/Functions/StringFunctions.h"

#include <stdexcept>

#include "Comparators.h"
#include "DataTypes/DateTime.h"
#include "../../include/Memory/IAllocator.h"
#include "DataTypes/DataTypes.StaticData.h"

long double Value::InterpolateString() const{
    const auto str = this->AsString();

    constexpr Int MAX_PREFIX_LEN = 8;  // Use first 8 characters
    constexpr double BASE = 256.0;        // ASCII character set

    const auto length = std::min(str.Size(), MAX_PREFIX_LEN);

    long double result = 0.0;
    for (int i = 0;i < length; i++){
        const auto charValue = static_cast<unsigned char>(str[i]);
        const auto weight = std::pow(BASE, MAX_PREFIX_LEN - i - 1);
        result += charValue * weight;
    }

    return result;
}

void Value::BinaryOperationException(const DataType lhs, const DataType rhs) {
    const auto& leftStr = SQL_TYPES_NAMES[static_cast<Int>(lhs)];
    const auto& rightStr = SQL_TYPES_NAMES[static_cast<Int>(rhs)];

    throw std::invalid_argument("Left Operand has type: "
        + std::string(leftStr.Data(), leftStr.Size())
        + " and right operand has type: "
        + std::string(rightStr.Data(), rightStr.Size())
    );
}

Value::Value(
    object_t* data,
    const Int size,
    const DataType type,
    const Memory::IAllocator* allocator,
    const column_index_t index
):  _data(data), _allocator(allocator), _size(size),
    _columnIndex(index), _type(type){}

Value::Value(const column_index_t index)
    :   _data(nullptr), _allocator(nullptr),
        _size(0), _columnIndex(index),
        _type(DataType::Null) {}

Value::Value(const Value &other){
    this->_size = other._size;
    this->_type = other._type;
    this->_columnIndex = other._columnIndex;
    this->_allocator = other._allocator;

    if (other._data == nullptr) {
        this->_data = nullptr;
        return;
    }

    auto* buffer = static_cast<object_t*>(this->_allocator->AllocateRaw(this->_size));
    std::memcpy(buffer, other._data, this->_size);
    this->_data = buffer;
}

Value& Value::operator=(const Value& other){
    if (this == &other)
        return *this;

    this->_size = other._size;
    this->_type = other._type;
    this->_columnIndex = other._columnIndex;
    this->_allocator = other._allocator;
    if (other._data == nullptr) {
        this->_data = nullptr;
        return *this;
    }
    auto* buffer = static_cast<object_t*>(this->_allocator->AllocateRaw(this->_size));
    std::memcpy(buffer, other._data, this->_size);
    this->_data = buffer;
    return *this;
}

Value::Value(Value &&other)noexcept {
    if (this == &other)
        return;

    this->_size = other._size;
    this->_type = other._type;
    this->_data = other._data;
    this->_columnIndex = other._columnIndex;
    this->_allocator = other._allocator;

    other._data = nullptr;
    other._allocator = nullptr;
    other._size = 0;
    other._columnIndex = 0;
}
Value& Value::operator=(Value &&other) noexcept{
    if (this == &other)
        return *this;

    this->_size = other._size;
    this->_type = other._type;
    this->_data = other._data;
    this->_columnIndex = other._columnIndex;
    this->_allocator = other._allocator;

    other._data = nullptr;
    other._size = 0;
    other._columnIndex = 0;
    other._allocator = nullptr;

    return *this;
}

Value::Value(
    const ::Memory::IAllocator* allocator,
    const column_index_t index
):  _data(nullptr), _allocator(allocator),
    _size(0), _columnIndex(index),
    _type(DataType::Null) {}

Value::Value(
    const object_t* data,
    const Int size,
    const DataType type,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    this->_size = size;
    this->_type = type;
    this->_columnIndex = index;
    this->_allocator = allocator;

    auto* buffer = static_cast<object_t*>(this->_allocator->AllocateRaw(size));
    std::memcpy(buffer, data, size);
    this->_data = buffer;
}

Value::Value(
    const std::string& data,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(data.size()));
    std::memcpy(buffer, data.data(), data.size());
    this->_data = buffer;

    this->_size = data.size();

    this->_allocator = allocator;
    this->_columnIndex = index;
    this->_type = DataType::String;
}

Value::Value(
    const DataTypes::StringView& data,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(data.Size()));
    std::memcpy(buffer, data.Data(), data.Size());
    this->_data = buffer;

    this->_size = data.Size();

    this->_allocator = allocator;
    this->_columnIndex = index;
    this->_type = DataType::String;
}

Value::Value(
    const Serialization::JsonValue& data,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(data.Size()));
    std::memcpy(buffer, data.Data(), data.Size());
    this->_data = buffer;
    this->_size = data.Size();
    this->_allocator = allocator;
    this->_columnIndex = index;
    this->_type = JSON_TYPES_NAMES[static_cast<Int>(data.Type())];
}

Value Value::FromMove(
    object_t* data,
    const Int size,
    const DataType type,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    return Value(data, size, type, allocator, index);
}

Value Value::FromExternalStorage(
    const object_t* data,
    const Int size,
    const DataType type,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    return Value(data, size, type, allocator, index);
}

Value Value::Null(const column_index_t columnIndex){
    return Value(columnIndex);
}

Value Value::Null(const Memory::IAllocator* allocator, const column_index_t columnIndex){
    return Value(allocator, columnIndex);
}

void Value::SetColumnIndex(const column_index_t otherIndex) { this->_columnIndex = otherIndex; }

void Value::SetType(const DataType otherType){ this->_type = otherType; }
void Value::SetNull(){
    this->_data = nullptr;
    this->_size = 0;
}

block_size_t Value::Size() const{ return this->_size; }

const object_t* Value::Data() const{ return this->_data; }

bool Value::IsNull() const { return this->_data == nullptr; }

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

DataTypes::String Value::AsString() const {
    return DataTypes::Coercions::ToString(*this);
}

std::string Value::AsStdString() const{
    return std::string(reinterpret_cast<const char*>(this->_data), this->_size);
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

time_t Value::AsUnixTimeStamp() const{ return *reinterpret_cast<const time_t*>(this->_data); }

DataTypes::Guid Value::AsGuid() const {
    return DataTypes::Coercions::ToGuid(*this);
}

DataTypes::JsonBinary Value::AsJson() const{
    return DataTypes::Coercions::ToJsonBinary(*this);
}

page_id_t Value::AsLargeObjectPointer() const{
    return *reinterpret_cast<const page_id_t*>(this->_data);
}

std::ostream & operator<<(std::ostream& os, const Value &field){
    if (field.IsNull()) {
        os << "NULL";
        return os;
    }

    switch (field._type){
    case DataType::TinyInt:
        os << field.AsTinyInt();
        break;
    case DataType::SmallInt:
        os << field.AsSmallInt();
        break;
    case DataType::Int:
        os << field.AsInt();
        break;
    case DataType::BigInt:
        os << field.AsBigInt();
        break;
    case DataType::Decimal:
        os << field.AsDecimal();
        break;
    case DataType::String:
        os << field.AsString();
        break;
    case DataType::Bool:
        os << field.AsBool();
        break;
    case DataType::DateTime:
        field.AsDateTime().Print(os);
        break;
    case DataType::Guid:
        os << field.AsGuid();
        break;
    default:
        break;
    }

    return os;
}

const Memory::IAllocator* Value::GetAllocator() const{ return this->_allocator;}

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
