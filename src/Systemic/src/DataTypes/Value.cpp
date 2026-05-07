#include "../../include/DataTypes/Value.h"

#include <cmath>

#include "../../include/Coercions.h"
#include "../../include/Converter.h"
#include "../../include/Functions/StringFunctions.h"

#include <stdexcept>

#include "Comparators.h"
#include "DataTypes/DateTime.h"
#include "../../include/Memory/IAllocator.h"
#include "DataTypes/DataTypes.StaticData.h"

bool Value::TryParseAsBool()const{
    if (this->type == DataType::String)
        return this->TryParseAsBoolFromString();

    if (this->type == DataType::BigInt
        || this->type == DataType::TinyInt
        || this->type == DataType::SmallInt
        || this->type == DataType::Int
    ) return this->TryParseAsBoolFromInt();

    return false;
}

bool Value::TryParseAsBoolFromString()const{
    const auto stringView = this->AsStringView();
    if (stringView == "true" || stringView == "1")
        return true;

    if (stringView == "false" || stringView == "0")
        return true;

    return false;
}

bool Value::TryParseAsBoolFromInt()const{
    const auto intData = this->AsBigInt();

    if (intData == 1)
        return true;

    if (intData == 0)
        return true;

    return false;
}

bool Value::TryParseDate(){
    DataTypes::DateTime parsedDate;

    const auto result = DataTypes::DateTime::FromString(parsedDate, this->AsStringView());

    if (!result)
        return false;

    this->SetData(parsedDate);
    return true;
}

Value Value::PerformBigIntAddition(const Value& lhs, const Value& rhs){
    return Value(
        lhs.AsBigInt() +  rhs.AsBigInt(),
        lhs.GetAllocator(),
        0
    );
}

Value Value::PerformStringAddition(const Value& lhs, const Value& rhs){
    return Value(
        DataTypes::String::Concat(lhs.AsStringView(), rhs.AsStringView(), lhs.GetAllocator()),
        lhs.GetAllocator(),
        0
    );
}

Value Value::PerformDecimalAddition(const Value& lhs, const Value& rhs){
    return Value(lhs.AsDecimal() + rhs.AsDecimal(), lhs.GetAllocator(), 0);
}

Value Value::PerformBigIntSubtraction(const Value& lhs, const Value& rhs){
    return Value(
        lhs.AsBigInt() -  rhs.AsBigInt(),
        lhs.GetAllocator(),
        0
    );
}

Value Value::PerformDecimalSubtraction(const Value& lhs, const Value& rhs){
    return Value(lhs.AsDecimal() - rhs.AsDecimal(), lhs.GetAllocator(), 0);
}

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
    const auto& leftStr = SqlTypesString[static_cast<Int>(lhs)];
    const auto& rightStr = SqlTypesString[static_cast<Int>(rhs)];

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

Value::Value(const Value &copyVal){
    this->size = copyVal.size;
    this->type = copyVal.type;
    this->columnIndex = copyVal.columnIndex;
    this->_allocator = copyVal._allocator;

    if (copyVal.data == nullptr) {
        this->data = nullptr;
        return;
    }

    this->data = static_cast<object_t*>(this->_allocator->AllocateRaw(this->size));
    std::memcpy(this->data, copyVal.data, this->size);
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

Value::~Value() = default;

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

    this->data = static_cast<object_t*>(this->_allocator->AllocateRaw(size));
    std::memcpy(this->data, data, size);
}

Value::Value(const bool data, const Memory::IAllocator* allocator, const column_index_t index){
    this->data = static_cast<object_t*>(allocator->AllocateRaw(sizeof(bool)));
    std::memcpy(this->data, &data, sizeof(bool));

    this->_allocator = allocator;
    this->size = sizeof(bool);
    this->columnIndex = index;
    this->type = DataType::Bool;
}

Value::Value(
    const TinyInt data,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    this->data = static_cast<object_t*>(allocator->AllocateRaw(sizeof(TinyInt)));
    std::memcpy(this->data, &data, sizeof(TinyInt));

    this->_allocator = allocator;
    this->size = sizeof(TinyInt);
    this->columnIndex = index;
    this->type = DataType::TinyInt;
}

Value::Value(
    const SmallInt data,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    this->data = static_cast<object_t*>(allocator->AllocateRaw(sizeof(SmallInt)));
    std::memcpy(this->data, &data, sizeof(SmallInt));

    this->_allocator = allocator;
    this->size = sizeof(SmallInt);
    this->columnIndex = index;
    this->type = DataType::SmallInt;
}

Value::Value(
    const Int data,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    this->data = static_cast<object_t*>(allocator->AllocateRaw(sizeof(Int)));
    std::memcpy(this->data, &data, sizeof(Int));

    this->_allocator = allocator;
    this->size = sizeof(Int);
    this->columnIndex = index;
    this->type = DataType::Int;
}

Value::Value(
    const BigInt data,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    this->data = static_cast<object_t*>(allocator->AllocateRaw(sizeof(BigInt)));
    std::memcpy(this->data, &data, sizeof(BigInt));

    this->_allocator = allocator;
    this->size = sizeof(BigInt);
    this->columnIndex = index;
    this->type = DataType::BigInt;
}

Value::Value(
    const std::string& data,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    this->data = static_cast<object_t*>(allocator->AllocateRaw(data.size()));
    std::memcpy(this->data, data.data(), data.size());
    this->size = data.size();

    this->_allocator = allocator;
    this->columnIndex = index;
    this->type = DataType::String;
}

Value::Value(
    const DataTypes::String &data,
    const Memory::IAllocator *allocator,
    const column_index_t index
){
    this->data = static_cast<object_t*>(allocator->AllocateRaw(data.Size()));
    std::memcpy(this->data, data.Data(), data.Size());
    this->size = data.Size();

    this->_allocator = allocator;
    this->columnIndex = index;
    this->type = DataType::String;
}

Value::Value(
    const DataTypes::StringView& data,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    this->data = static_cast<object_t*>(allocator->AllocateRaw(data.Size()));
    std::memcpy(this->data, data.Data(), data.Size());
    this->size = data.Size();
    this->_allocator = allocator;
    this->columnIndex = index;
    this->type = DataType::String;
}

Value::Value(
    const DataTypes::DateTime& data,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    this->data = static_cast<object_t*>(allocator->AllocateRaw(DataTypes::DateTime::Size()));
    const auto dt = data.UnixTimeStamp();
    std::memcpy(this->data, &dt, DataTypes::DateTime::Size());

    this->_allocator = allocator;
    this->size = DataTypes::DateTime::Size();
    this->columnIndex = index;
    this->type = DataType::DateTime;
}

Value::Value(
    const DataTypes::Decimal& data,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    this->data = static_cast<object_t*>(allocator->AllocateRaw(data.GetRawDataSize()));
    std::memcpy(this->data, data.GetRawData(), data.GetRawDataSize());

    this->_allocator = allocator;
    this->size = data.GetRawDataSize();
    this->columnIndex = index;
    this->type = DataType::Decimal;
}

Value::Value(
    const DataTypes::Guid& data,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    this->data = static_cast<object_t*>(allocator->AllocateRaw(DataTypes::Guid::Size()));
    std::memcpy(this->data, data.GetData().data(), DataTypes::Guid::Size());

    this->_allocator = allocator;
    this->size = DataTypes::Guid::Size();
    this->columnIndex = index;
    this->type = DataType::Guid;
}

Value::Value(
    const Serialization::JsonValue& data,
    const Memory::IAllocator* allocator,
    const column_index_t index
){
    this->data = static_cast<object_t*>(allocator->AllocateRaw(data.Size()));
    std::memcpy(this->data, data.Data(), data.Size());
    this->size = data.Size();
    this->_allocator = allocator;
    this->columnIndex = index;
    this->type = JsonToSqlTypes[static_cast<Int>(data.Type())];
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

bool Value::IsNull() const { return this->data == nullptr; }

column_index_t Value::GetColumnIndex() const { return this->columnIndex;}
DataType Value::GetType() const{ return this->type; }

void Value::SetNull(){
    this->data = nullptr;
    this->size = 0;
}

void Value::SetData(const bool otherData){
    this->data = static_cast<object_t*>(this->_allocator->AllocateRaw(sizeof(bool)));
    std::memcpy(this->data, &otherData, sizeof(bool));

    this->size = sizeof(bool);
    this->type = DataType::Bool;
}

void Value::SetData(const TinyInt otherData) {
    this->data = static_cast<object_t*>(this->_allocator->AllocateRaw(sizeof(TinyInt)));
    std::memcpy(this->data, &otherData, sizeof(TinyInt));

    this->size = sizeof(TinyInt);
    this->type = DataType::TinyInt;
}

void Value::SetData(const SmallInt otherData) {
    this->data = static_cast<object_t*>(this->_allocator->AllocateRaw(sizeof(SmallInt)));
    std::memcpy(this->data, &otherData, sizeof(SmallInt));

    this->size = sizeof(SmallInt);
    this->type = DataType::SmallInt;
}

void Value::SetData(const Int otherData) {
    this->data = static_cast<object_t*>(this->_allocator->AllocateRaw(sizeof(Int)));
    std::memcpy(this->data, &otherData, sizeof(Int));

    this->size = sizeof(Int);
    this->type = DataType::Int;
}

void Value::SetData(const BigInt otherData) {
    this->data = static_cast<object_t*>(this->_allocator->AllocateRaw(sizeof(BigInt)));
    std::memcpy(this->data, &otherData, sizeof(BigInt));
    this->size = sizeof(BigInt);

    this->type = DataType::BigInt;
}

void Value::SetData(const std::string &otherData) {
    this->data = static_cast<object_t*>(this->_allocator->AllocateRaw(otherData.size()));
    std::memcpy(this->data, otherData.data(), this->size);

    this->size = otherData.size();
    this->type = DataType::String;
}

void Value::SetData(const DataTypes::Decimal &otherData) {
    this->data = static_cast<object_t*>(this->_allocator->AllocateRaw(otherData.GetRawDataSize()));
    std::memcpy(this->data, otherData.GetRawData(), this->size);

    this->size = otherData.GetRawDataSize();
    this->type = DataType::Decimal;
}

void Value::SetData(const DataTypes::DateTime &otherData) {
    this->data = static_cast<object_t*>(this->_allocator->AllocateRaw(DataTypes::DateTime::Size()));
    const auto dt = otherData.UnixTimeStamp();
    std::memcpy(this->data, &dt, DataTypes::DateTime::Size());

    this->size = DataTypes::DateTime::Size();
    this->type = DataType::DateTime;
}

void Value::SetData(const DataTypes::Guid &otherData){
    this->data = static_cast<object_t*>(this->_allocator->AllocateRaw(DataTypes::Guid::Size()));
    std::memcpy(this->data, otherData.GetData().data(), this->size);

    this->size = DataTypes::Guid::Size();
    this->type = DataType::Guid;
}

void Value::SetData(const page_id_t pageId){
    this->data = static_cast<object_t*>(this->_allocator->AllocateRaw(sizeof(page_id_t)));
    std::memcpy(this->data, &pageId, this->size);

    this->size = sizeof(page_id_t);
    this->type = DataType::Int;
}

block_size_t Value::Size() const{ return this->size; }

const object_t * Value::Data() const{ return this->data; }

object_t* Value::DataUnsafe() const{
    return this->data;
}

bool Value::AsBool() const {
    return DataTypes::Coercions::ToBool(*this);
}

int8_t Value::AsTinyInt() const {
    return DataTypes::Coercions::ToTinyInt(*this);
}

SmallInt Value::AsSmallInt() const {
    return DataTypes::Coercions::ToSmallInt(*this);
}

Int Value::AsInt() const {
    return DataTypes::Coercions::ToInt(*this);
}

int64_t Value::AsBigInt() const {
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

time_t Value::AsUnixTimeStamp() const{ return *reinterpret_cast<time_t *>(this->data); }

DataTypes::Guid Value::AsGuid() const {
    return DataTypes::Coercions::ToGuid(*this);
}

DataTypes::JsonBinary Value::AsJson() const{
    return DataTypes::Coercions::ToJsonBinary(*this);
}

page_id_t Value::AsLargeObjectPointer() const{
    return *reinterpret_cast<page_id_t *>(this->data);
}

void Value::SetColumnIndex(const column_index_t otherIndex) { this->columnIndex = otherIndex; }

void Value::SetType(const DataType otherType){ this->type = otherType; }

void Value::Deserialize(const std::vector<char> &buffer, UnsignedInt &offset){
    std::memcpy(&this->size, buffer.data() + offset, sizeof(block_size_t));
    offset += sizeof(block_size_t);

    std::memcpy(&this->type, buffer.data() + offset, sizeof(DataType));
    offset += sizeof(DataType);

    this->data = new object_t[this->size];
    std::memcpy(this->data, buffer.data() + offset, sizeof(object_t) * this->size);
    offset += this->size;
}

DataType Value::PromoteType(const DataType lhs, const DataType rhs){
    return lhs > rhs ? lhs : rhs;
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

Value& Value::operator=(const Value &rhs){
    if (this == &rhs)
        return *this;

    this->size = rhs.size;
    this->type = rhs.type;
    this->columnIndex = rhs.columnIndex;
    this->_allocator = rhs.GetAllocator();

    if (rhs.data == nullptr) {
        this->data = nullptr;
        return *this;
    }

    this->data = static_cast<object_t*>(this->_allocator->AllocateRaw(this->size));
    std::memcpy(this->data, rhs.data, this->size);

    return *this;
}

//TODO implement operations by dataType
Value operator+(const Value &lhs, const Value &rhs){
    switch (Value::PromoteType(lhs.type, rhs.type)) {
    case DataType::TinyInt:
    case DataType::SmallInt:
    case DataType::Int:
    case DataType::BigInt: {
        auto value = Value::PerformBigIntAddition(lhs, rhs);
        DataTypes::Coercions::DeduceIntegerType(value);
        return value;
    }
    case DataType::Decimal:
        return Value::PerformDecimalAddition(lhs, rhs);
    case DataType::String:
        return Value::PerformStringAddition(lhs, rhs);
    case DataType::Bool:
        return Value(lhs.AsBool() + rhs.AsBool(), lhs.GetAllocator(), 0);
    case DataType::DateTime:
    case DataType::Guid:
    case DataType::RowIdentifier:
    case DataType::Null:
    default:
        Value::BinaryOperationException(lhs.type, rhs.type);
    }

    return Value::Null(nullptr);
}

Value& Value::operator+=(const Value &rhs){
    *this = *this + rhs;
    return *this;
}

Value operator-(const Value &lhs, const Value &rhs){
    switch (Value::PromoteType(lhs.type, rhs.type)) {
        case DataType::TinyInt:
        case DataType::SmallInt:
        case DataType::Int:
        case DataType::BigInt: {
            auto value = Value::PerformBigIntSubtraction(lhs, rhs);
            DataTypes::Coercions::DeduceIntegerType(value);
            return value;
        }
        case DataType::Decimal:
            return Value::PerformDecimalSubtraction(lhs, rhs);
        case DataType::String:
        case DataType::Bool:
        case DataType::DateTime:
        case DataType::Guid:
        case DataType::RowIdentifier:
        case DataType::Null:
        default:
            Value::BinaryOperationException(lhs.type, rhs.type);
    }

    return Value::Null(nullptr);
}

Value operator/(const Value &lhs, const Value &rhs){
    return Value::Null(nullptr);
}

Value operator%(const Value &lhs, const Value &rhs){
    switch (Value::PromoteType(lhs.type, rhs.type)) {
    case DataType::TinyInt:
    case DataType::SmallInt:
    case DataType::Int:
    case DataType::BigInt: {
        auto value = Value(lhs.AsBigInt() % rhs.AsBigInt(), lhs.GetAllocator(), 0);
        DataTypes::Coercions::DeduceIntegerType(value);
        return value;
    }
    case DataType::Bool:
        return Value(lhs.AsBool() % rhs.AsBool(), lhs.GetAllocator(), 0);
    case DataType::Decimal:
    // return Field(lhs.GetDecimal() % rhs.GetDecimal(), 0);WWW
    case DataType::String:
    case DataType::DateTime:
    case DataType::Guid:
    case DataType::RowIdentifier:
    case DataType::Null:
    default:
        Value::BinaryOperationException(lhs.type, rhs.type);
    }

    return Value::Null(nullptr);
}

Value operator*(const Value &lhs, const Value &rhs){
    switch (Value::PromoteType(lhs.type, rhs.type)) {
    case DataType::TinyInt:
    case DataType::SmallInt:
    case DataType::Int:
    case DataType::BigInt: {
        auto value = Value(lhs.AsBigInt() * rhs.AsBigInt(), lhs.GetAllocator(), 0);
        DataTypes::Coercions::DeduceIntegerType(value);
        return value;
    }
    case DataType::Bool:
        return Value(lhs.AsBool() * rhs.AsBool(), lhs.GetAllocator(), 0);
    case DataType::Decimal:
        return Value(lhs.AsDecimal() * rhs.AsDecimal(), lhs.GetAllocator(), 0);
    case DataType::String:
    case DataType::DateTime:
    case DataType::Guid:
    case DataType::RowIdentifier:
    case DataType::Null:
    default:
        Value::BinaryOperationException(lhs.type, rhs.type);
    }

    return Value::Null(nullptr);
}

bool operator<(const Value &lhs, const Value &rhs){
    return Comparators::Compare(lhs, rhs) == Comparators::Comparator::Less;
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

bool operator==(const Value &lhs, const Value &rhs){
    return Comparators::Compare(lhs, rhs) == Comparators::Comparator::Equal;
}

bool operator!=(const Value &lhs, const Value &rhs){
    return Comparators::Compare(lhs, rhs) != Comparators::Comparator::Equal;
}

const Memory::IAllocator* Value::GetAllocator() const{ return this->_allocator;}

bool Value::ParseAsBoolFromString() const{
    const auto strView = this->AsStringView();

    for (const auto& str: TrueStrings)
        if (str.Contains(strView, StringComparisonType::EqualsIgnoreOrdinalCase))
            return true;

    for (const auto& str: FalseStrings)
        if (str.Contains(strView, StringComparisonType::EqualsIgnoreOrdinalCase))
            return false;

    return false;
}

Value Value::EqualsIgnoreOrdinalCase(const Value &lhs, const Value &rhs){
    return Value(
        lhs.AsStringView().Contains(rhs.AsStringView(), StringComparisonType::EqualsIgnoreOrdinalCase),
        lhs.GetAllocator(),
        0
    );
}

int64_t Value::Hash() const {
    return static_cast<int64_t>(0);
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