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
){
    this->data = data;
    this->size = size;
    this->type = type;
    this->_allocator = allocator;
    this->columnIndex = index;
}

Value::Value(const column_index_t index){
    this->data = nullptr;
    this->_allocator = nullptr;
    this->size = 0;
    this->type = DataType::Null;
    this->columnIndex = index;
}

Value::Value(const Value &other){
    this->size = other.size;
    this->type = other.type;
    this->columnIndex = other.columnIndex;
    this->_allocator = other._allocator;

    if (other.data == nullptr) {
        this->data = nullptr;
        return;
    }

    auto* buffer = static_cast<object_t*>(this->_allocator->AllocateRaw(this->size));
    std::memcpy(buffer, other.data, this->size);
    this->data = buffer;
}

Value& Value::operator=(const Value& other){
    if (this == &other)
        return *this;

    this->size = other.size;
    this->type = other.type;
    this->columnIndex = other.columnIndex;
    this->_allocator = other._allocator;
    if (other.data == nullptr) {
        this->data = nullptr;
        return *this;
    }
    auto* buffer = static_cast<object_t*>(this->_allocator->AllocateRaw(this->size));
    std::memcpy(buffer, other.data, this->size);
    this->data = buffer;
    return *this;
}

Value::Value(Value &&other)noexcept {
    if (this == &other)
        return;

    this->size = other.size;
    this->type = other.type;
    this->data = other.data;
    this->columnIndex = other.columnIndex;
    this->_allocator = other._allocator;

    other.data = nullptr;
    other._allocator = nullptr;
    other.size = 0;
    other.columnIndex = 0;
}
Value & Value::operator=(Value &&other) noexcept{
    if (this == &other)
        return *this;

    this->size = other.size;
    this->type = other.type;
    this->data = other.data;
    this->columnIndex = other.columnIndex;
    this->_allocator = other._allocator;

    other.data = nullptr;
    other.size = 0;
    other.columnIndex = 0;
    other._allocator = nullptr;

    return *this;
}

Value::Value(
    const ::Memory::IAllocator* allocator,
    const column_index_t index
){
    this->data = nullptr;
    this->columnIndex = index;
    this->size = 0;
    this->type = DataType::Null;
    this->_allocator = allocator;
}

Value::Value(
    const object_t* data,
    const Int size,
    const DataType type,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    this->data = nullptr;
    this->size = size;
    this->type = type;
    this->columnIndex = index;
    this->_allocator = allocator;

    auto* buffer = static_cast<object_t*>(this->_allocator->AllocateRaw(size));
    std::memcpy(buffer, data, size);
    this->data = buffer;
}

Value::Value(
    const std::string& data,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(data.size()));
    std::memcpy(buffer, data.data(), data.size());
    this->data = buffer;

    this->size = data.size();

    this->_allocator = allocator;
    this->columnIndex = index;
    this->type = DataType::String;
}

Value::Value(
    const DataTypes::StringView& data,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(data.Size()));
    std::memcpy(buffer, data.Data(), data.Size());
    this->data = buffer;

    this->size = data.Size();

    this->_allocator = allocator;
    this->columnIndex = index;
    this->type = DataType::String;
}

Value::Value(
    const Serialization::JsonValue& data,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    auto* buffer = static_cast<object_t*>(allocator->AllocateRaw(data.Size()));
    std::memcpy(buffer, data.Data(), data.Size());
    this->data = buffer;
    this->size = data.Size();
    this->_allocator = allocator;
    this->columnIndex = index;
    this->type = JSON_TYPES_NAMES[static_cast<Int>(data.Type())];
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

void Value::SetColumnIndex(const column_index_t otherIndex) { this->columnIndex = otherIndex; }

void Value::SetType(const DataType otherType){ this->type = otherType; }
void Value::SetNull(){
    this->data = nullptr;
    this->size = 0;
}

block_size_t Value::Size() const{ return this->size; }

const object_t* Value::Data() const{ return this->data; }

bool Value::IsNull() const { return this->data == nullptr; }

column_index_t Value::GetColumnIndex() const { return this->columnIndex;}

DataType Value::GetType() const{ return this->type; }

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
    return std::string(reinterpret_cast<const char*>(this->data), this->size);
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

time_t Value::AsUnixTimeStamp() const{ return *reinterpret_cast<const time_t*>(this->data); }

DataTypes::Guid Value::AsGuid() const {
    return DataTypes::Coercions::ToGuid(*this);
}

DataTypes::JsonBinary Value::AsJson() const{
    return DataTypes::Coercions::ToJsonBinary(*this);
}

page_id_t Value::AsLargeObjectPointer() const{
    return *reinterpret_cast<const page_id_t*>(this->data);
}

std::ostream & operator<<(std::ostream& os, const Value &field){
    if (field.IsNull()) {
        os << "NULL";
        return os;
    }

    switch (field.type){
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
        if (str.Contains(strView, StringComparisonType::EqualsIgnoreCase))
            return true;

    for (const auto& str: FalseStrings)
        if (str.Contains(strView, StringComparisonType::EqualsIgnoreCase))
            return false;

    return false;
}


long double Value::Interpolate() const{
    switch (this->type){
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
