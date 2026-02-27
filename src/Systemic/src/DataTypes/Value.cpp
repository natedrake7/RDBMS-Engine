#include "../../include/DataTypes/Value.h"

#include <cmath>

#include "../../include/Coercions.h"
#include "../../include/Converter.h"
#include "../../include/Functions/StringFunctions.h"

#include <cstring>
#include <stdexcept>

#include "DataTypes/DateTime.h"
#include "../../include/Memory/IAllocator.h"

bool Value::TryParseAsBool()const{
    if (this->type == DataType::String || this->type == DataType::UnicodeString)
        return this->TryParseAsBoolFromString();

    if (this->type == DataType::BigInt
        || this->type == DataType::TinyInt
        || this->type == DataType::SmallInt
        || this->type == DataType::Int
    ) return this->TryParseAsBoolFromInt();

    return false;
}

bool Value::TryParseAsBoolFromString()const{
    const auto strData = Functions::String::Lower(this->AsString());

    const auto stringView = this->AsStringView();
    if (strData == "true" || strData == "1")
        return true;

    if (strData == "false" || strData == "0")
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
    const auto strData = Functions::String::Lower(this->AsString());

    DataTypes::DateTime parsedDate;

    const auto result = DataTypes::DateTime::FromString(parsedDate, strData);

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
        lhs.AsString() + rhs.AsString(),
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

std::tuple<bool, Value> Value::PerformNullEqualityComparison(
    const Value &lhs,
    const Value &rhs
){
    if (lhs.IsNull())
        return std::make_tuple(true, Value(rhs.IsNull(), lhs.GetAllocator(), 0));

    if (rhs.IsNull())
        return std::make_tuple(true, Value(false, lhs.GetAllocator(), 0));

    return std::make_tuple(false, Value::Null());
}

std::tuple<bool, Value> Value::PerformNullGreaterComparison(const Value& lhs, const Value& rhs){
    const auto& isLeftNull = lhs.IsNull();
    const auto& isRightNull = rhs.IsNull();

    if (isLeftNull && isRightNull)
        return std::make_tuple(true, Value(false, lhs.GetAllocator(), 0));

    if (isLeftNull && !isRightNull)
        return std::make_tuple(true, Value(false, lhs.GetAllocator(), 0));

    if (!isLeftNull && isRightNull)
        return std::make_tuple(true, Value(true, lhs.GetAllocator(), 0));

    return std::make_tuple(false, Value::Null());
}

std::tuple<bool, Value> Value::PerformNullGreaterEqualComparison(const Value& lhs, const Value& rhs){
    const auto& isLeftNull = lhs.IsNull();
    const auto& isRightNull = rhs.IsNull();

    if (isLeftNull && isRightNull)
        return std::make_tuple(true, Value(true, lhs.GetAllocator(), 0));

    if (isLeftNull && !isRightNull)
        return std::make_tuple(true, Value(false, lhs.GetAllocator(), 0));

    if (!isLeftNull && isRightNull)
        return std::make_tuple(true, Value(true, lhs.GetAllocator(), 0));

    return std::make_tuple(false, Value::Null());
}

std::tuple<bool, Value> Value::PerformNullLessComparison(const Value& lhs, const Value& rhs){
    const auto& isLeftNull = lhs.IsNull();
    const auto& isRightNull = rhs.IsNull();

    if (isLeftNull && isRightNull)
        return std::make_tuple(true, Value(false, lhs.GetAllocator(), 0));

    if (isLeftNull && !isRightNull)
        return std::make_tuple(true, Value(true, lhs.GetAllocator(), 0));

    if (!isLeftNull && isRightNull)
        return std::make_tuple(true, Value(false, lhs.GetAllocator(), 0));

    return std::make_tuple(false, Value::Null());
}

std::tuple<bool, Value> Value::PerformNullLessEqualComparison(const Value& lhs, const Value& rhs){
    const auto& isLeftNull = lhs.IsNull();
    const auto& isRightNull = rhs.IsNull();

    if (isLeftNull && isRightNull)
        return std::make_tuple(true, Value(true, lhs.GetAllocator(), 0));

    if (isLeftNull && !isRightNull)
        return std::make_tuple(true, Value(true, lhs.GetAllocator(), 0));

    if (!isLeftNull && isRightNull)
        return std::make_tuple(true, Value(false, lhs.GetAllocator(), 0));

    return std::make_tuple(false, Value::Null());
}

std::tuple<bool, Value> Value::PerformNullInEqualityComparison(const Value &lhs, const Value &rhs){
    if (lhs.IsNull())
        return std::make_tuple(true, Value(!rhs.IsNull(), lhs.GetAllocator(), 0));

    if (rhs.IsNull())
        return std::make_tuple(true, Value(true, lhs.GetAllocator(), 0));

    return std::make_tuple(false, Value::Null());
}

long double Value::InterpolateString() const{
    const auto str = this->AsString();

    constexpr auto MAX_PREFIX_LEN = 8;  // Use first 8 characters
    constexpr double BASE = 256.0;        // ASCII character set

    const auto length = std::min(str.length(), static_cast<size_t>(MAX_PREFIX_LEN));

    long double result = 0.0;
    for (int i = 0;i < length; i++){
        const auto charValue = static_cast<unsigned char>(str[i]);
        const auto weight = std::pow(BASE, MAX_PREFIX_LEN - i - 1);
        result += charValue * weight;
    }

    return result;
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

Value::Value(const column_index_t index){
    this->data = nullptr;
    this->columnIndex = index;
    this->size = 0;
    this->type = DataType::Unknown;
    this->_allocator = nullptr;
}

Value::Value(
    const void *data,
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

// Value::Value(
//     const object_t* data,
//     const Int size,
//     const DataType type,
//     const Memory::Allocator* allocator,
//     const column_index_t index
// ){
//     this->data = static_cast<object_t*>(allocator->Allocate(size));
//     std::memcpy(this->data, data, size);
//
//     this->_allocator = allocator;
//     this->size = size;
//     this->type = type;
//     this->columnIndex = index;
// }

// Value::Value(const bool data, const column_index_t index){
//     this->data = new object_t[sizeof(bool)];
//     std::memcpy(this->data, &data, sizeof(bool));
//
//     this->size = sizeof(bool);
//     this->columnIndex = index;
//     this->type = DataType::Bool;
//     this->usesExternalStorage = false;
// }
//
// Value::Value(const TinyInt data, const column_index_t index){
//     this->data = new object_t[sizeof(TinyInt)];
//     std::memcpy(this->data, &data, sizeof(TinyInt));
//
//     this->size = sizeof(int8_t);
//     this->columnIndex = index;
//     this->type = DataType::TinyInt;
//     this->usesExternalStorage = false;
// }
//
// Value::Value(const SmallInt data, const column_index_t index){
//     this->data = new object_t[sizeof(SmallInt)];
//     std::memcpy(this->data, &data, sizeof(SmallInt));
//
//     this->size = sizeof(SmallInt);
//     this->columnIndex = index;
//     this->type = DataType::SmallInt;
//     this->usesExternalStorage = false;
// }
//
// Value::Value(const Int data, const column_index_t index){
//     this->data = new object_t[sizeof(Int)];
//     std::memcpy(this->data, &data, sizeof(Int));
//
//     this->size = sizeof(Int);
//     this->columnIndex = index;
//     this->type = DataType::Int;
//     this->usesExternalStorage = false;
// }
//
// Value::Value(const int64_t data, const column_index_t index){
//     this->data = new object_t[sizeof(int64_t)];
//     std::memcpy(this->data, &data, sizeof(int64_t));
//
//     this->size = sizeof(int64_t);
//     this->columnIndex = index;
//     this->type = DataType::BigInt;
//     this->usesExternalStorage = false;
// }
//
// Value::Value(const std::string &data, const column_index_t index){
//     this->size = data.size();
//     this->data = new object_t[this->size];
//     std::memcpy(this->data, data.data(), this->size);
//
//     this->columnIndex = index;
//     this->type = DataType::String;
//     this->usesExternalStorage = false;
// }
//
// Value::Value(const DataTypes::DateTime &data, const column_index_t index){
//     this->data = new object_t[DataTypes::DateTime::Size()];
//     const auto dt = data.GetUnixTimeStamp();
//     std::memcpy(this->data, &dt, DataTypes::DateTime::Size());
//
//     this->size = DataTypes::DateTime::Size();
//     this->columnIndex = index;
//     this->type = DataType::DateTime;
//     this->usesExternalStorage = false;
// }
//
// Value::Value(const DataTypes::Decimal &data, const column_index_t index){
//     this->size = data.GetRawDataSize();
//     this->data = new object_t[this->size];
//
//     std::memcpy(this->data, data.GetRawData(), this->size);
//     this->columnIndex = index;
//     this->type = DataType::Decimal;
//     this->usesExternalStorage = false;
// }
//
// Value::Value(const DataTypes::Guid &data, const column_index_t index){
//     this->size = data.Size();
//     this->data = new object_t[this->size];
//     std::memcpy(this->data, data.GetData().data(), this->size);
//
//     this->columnIndex = index;
//     this->type = DataType::Guid;
//     this->usesExternalStorage = false;
// }

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
    this->data = static_cast<object_t*>(allocator->AllocateRaw(data.Size()));
    std::memcpy(this->data, data.GetData().data(), data.Size());

    this->_allocator = allocator;
    this->size = data.Size();
    this->columnIndex = index;
    this->type = DataType::Guid;
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

Value Value::Null(const column_index_t columnIndex) { return Value(columnIndex); }

bool Value::IsNull() const { return this->data == nullptr; }

column_index_t Value::GetColumnIndex() const { return this->columnIndex;}
DataType Value::GetType() const{ return this->type; }

void Value::SetNull(){
    std::free(this->data);
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

DataTypes::StringView Value::AsStringView() const{
    return DataTypes::Coercions::ToString(*this);
}

std::u16string Value::AsUnicodeString() const {
    return DataTypes::Coercions::ToUnicodeString(*this);
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
    return  ColumnTypeRank.Get(lhs) > ColumnTypeRank.Get(rhs) ? lhs : rhs;
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
    case DataType::UnicodeString:
        os << field.AsString();
        break;
    case DataType::Bool:
        os << field.AsBool();
        break;
    case DataType::DateTime:
        os << field.AsDateTime();
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
    case DataType::UnicodeString:
        return Value::PerformStringAddition(lhs, rhs);
    case DataType::Bool:
        return Value(lhs.AsBool() + rhs.AsBool(), lhs.GetAllocator(), 0);
    case DataType::DateTime:
    case DataType::Guid:
    case DataType::RowIdentifier:
    case DataType::Unknown:
    default:
        throw std::invalid_argument("Left Operand has type: "
            + ColumnTypesToStringDictionary.Get(lhs.type)
            + " and right operand has type: "
            + ColumnTypesToStringDictionary.Get(rhs.type));
    }
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
        case DataType::UnicodeString:
        case DataType::Bool:
        case DataType::DateTime:
        case DataType::Guid:
        case DataType::RowIdentifier:
        case DataType::Unknown:
        default:
            throw std::invalid_argument("Left Operand has type: "
                + ColumnTypesToStringDictionary.Get(lhs.type)
                + " and right operand has type: "
                + ColumnTypesToStringDictionary.Get(rhs.type));
    }
}

Value operator/(const Value &lhs, const Value &rhs){
    return Value::Null();
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
    case DataType::UnicodeString:
    case DataType::DateTime:
    case DataType::Guid:
    case DataType::RowIdentifier:
    case DataType::Unknown:
    default:
        throw std::invalid_argument("Left Operand has type: "
            + ColumnTypesToStringDictionary.Get(lhs.type)
            + " and right operand has type: "
            + ColumnTypesToStringDictionary.Get(rhs.type)
        );
    }
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
    case DataType::UnicodeString:
    case DataType::DateTime:
    case DataType::Guid:
    case DataType::RowIdentifier:
    case DataType::Unknown:
    default:
        throw std::invalid_argument("Left Operand has type: "
            + ColumnTypesToStringDictionary.Get(lhs.type)
            + " and right operand has type: "
            + ColumnTypesToStringDictionary.Get(rhs.type)
        );
    }
}

Value operator<(const Value &lhs, const Value &rhs){
    const auto& [returnOutput, output] = Value::PerformNullLessComparison(lhs, rhs);
    if (returnOutput)
        return output;

    switch (Value::PromoteType(lhs.type, rhs.type)) {
    case DataType::TinyInt:
    case DataType::SmallInt:
    case DataType::Int:
    case DataType::BigInt:
        return Value(lhs.AsBigInt() < rhs.AsBigInt(), lhs.GetAllocator(), 0);
    case DataType::Decimal:
        return Value(lhs.AsDecimal() < rhs.AsDecimal(), lhs.GetAllocator(), 0);
    case DataType::String:
        return Value(lhs.AsString() < rhs.AsString(), lhs.GetAllocator(), 0);
    case DataType::UnicodeString:
        return Value(lhs.AsUnicodeString() < rhs.AsUnicodeString(), lhs.GetAllocator(), 0);
    case DataType::Bool:
        return Value(lhs.AsBool() < rhs.AsBool(), lhs.GetAllocator(), 0);
    case DataType::DateTime:
        return Value(lhs.AsDateTime() < rhs.AsDateTime(), lhs.GetAllocator(), 0);
    case DataType::Guid:
        return Value(lhs.AsGuid() < rhs.AsGuid(), lhs.GetAllocator(), 0);
    case DataType::RowIdentifier:
    case DataType::Unknown:
    default:
        throw std::invalid_argument("Left Operand has type: "
            + ColumnTypesToStringDictionary.Get(lhs.type)
            + " and right operand has type: "
            + ColumnTypesToStringDictionary.Get(rhs.type)
        );
    }
}

Value operator>(const Value &lhs, const Value &rhs){
    return rhs < lhs;
}

Value operator<=(const Value &lhs, const Value &rhs){
    const auto& [returnOutput, value] = Value::PerformNullLessEqualComparison(lhs, rhs);
    if (returnOutput)
        return value;

    switch (Value::PromoteType(lhs.type, rhs.type)) {
    case DataType::TinyInt:
    case DataType::SmallInt:
    case DataType::Int:
    case DataType::BigInt:
        return Value(lhs.AsBigInt() <= rhs.AsBigInt(), lhs.GetAllocator(), 0);
    case DataType::Decimal:
        return Value(lhs.AsDecimal() <= rhs.AsDecimal(), lhs.GetAllocator(), 0);
    case DataType::String:
        return Value(lhs.AsString() <= rhs.AsString(), lhs.GetAllocator(), 0);
    case DataType::UnicodeString:
        return Value(lhs.AsUnicodeString() <= rhs.AsUnicodeString(), lhs.GetAllocator(), 0);
    case DataType::Bool:
        return Value(lhs.AsBool() <= rhs.AsBool(), lhs.GetAllocator(), 0);
    case DataType::DateTime:
        return Value(lhs.AsDateTime() <= rhs.AsDateTime(), lhs.GetAllocator(), 0);
    case DataType::Guid:
        return Value(lhs.AsGuid() <= rhs.AsGuid(), lhs.GetAllocator(), 0);
    case DataType::RowIdentifier:
    case DataType::Unknown:
    default:
        throw std::invalid_argument("Left Operand has type: "
            + ColumnTypesToStringDictionary.Get(lhs.type)
            + " and right operand has type: "
            + ColumnTypesToStringDictionary.Get(rhs.type)
        );
    }
}


Value operator>=(const Value &lhs, const Value &rhs){
    const auto& [returnOutput, value] = Value::PerformNullGreaterEqualComparison(lhs, rhs);
    if (returnOutput)
        return value;

    switch (Value::PromoteType(lhs.type, rhs.type)) {
    case DataType::TinyInt:
    case DataType::SmallInt:
    case DataType::Int:
    case DataType::BigInt: {
        const auto left = lhs.AsBigInt();
        const auto right = rhs.AsBigInt();

        return Value(left >= right, lhs.GetAllocator(), 0);
    }
    case DataType::Decimal:
        return Value(lhs.AsDecimal() >= rhs.AsDecimal(), lhs.GetAllocator(), 0);
    case DataType::String:
        return Value(lhs.AsString() >= rhs.AsString(), lhs.GetAllocator(), 0);
    case DataType::UnicodeString:
        return Value(lhs.AsUnicodeString() >= rhs.AsUnicodeString(), lhs.GetAllocator(), 0);
    case DataType::Bool:
        return Value(lhs.AsBool() >= rhs.AsBool(), lhs.GetAllocator(), 0);
    case DataType::DateTime:
        return Value(lhs.AsDateTime() >= rhs.AsDateTime(), lhs.GetAllocator(), 0);
    case DataType::Guid:
        return Value(lhs.AsGuid() >= rhs.AsGuid(), lhs.GetAllocator(), 0);
    case DataType::RowIdentifier:
    case DataType::Unknown:
    default:
        throw std::invalid_argument("Left Operand has type: "
            + ColumnTypesToStringDictionary.Get(lhs.type)
            + " and right operand has type: "
            + ColumnTypesToStringDictionary.Get(rhs.type)
        );
    }
}

Value operator==(const Value &lhs, const Value &rhs){
    const auto& [returnOutput, output] = Value::PerformNullEqualityComparison(lhs, rhs);
    if (returnOutput)
        return output;

    switch (Value::PromoteType(lhs.type, rhs.type)) {
    case DataType::TinyInt:
    case DataType::SmallInt:
    case DataType::Int:
    case DataType::BigInt: {
        auto value = Value(lhs.AsBigInt() == rhs.AsBigInt(), lhs.GetAllocator(), 0);
        DataTypes::Coercions::DeduceIntegerType(value);
        return value;
    }
    case DataType::Decimal:
        return Value(lhs.AsDecimal() == rhs.AsDecimal(), lhs.GetAllocator(), 0);
    case DataType::String:
        return Value(lhs.AsString() == rhs.AsString(), lhs.GetAllocator(), 0);
    case DataType::UnicodeString:
        return Value(lhs.AsUnicodeString() == rhs.AsUnicodeString(), lhs.GetAllocator(), 0);
    case DataType::Bool:
        return Value(lhs.AsBool() == rhs.AsBool(), lhs.GetAllocator(), 0);
    case DataType::DateTime:
        return Value(lhs.AsDateTime() == rhs.AsDateTime(), lhs.GetAllocator(), 0);
    case DataType::Guid:
        return Value(lhs.AsGuid() == rhs.AsGuid(), lhs.GetAllocator(), 0);
    case DataType::RowIdentifier:
    case DataType::Unknown:
    default:
        throw std::invalid_argument("Left Operand has type: "
            + ColumnTypesToStringDictionary.Get(lhs.type)
            + " and right operand has type: "
            + ColumnTypesToStringDictionary.Get(rhs.type)
        );
    }
}

Value operator!=(const Value &lhs, const Value &rhs){
    const auto& [returnOutput, output] = Value::PerformNullInEqualityComparison(lhs, rhs);
    if (returnOutput)
        return output;

    switch (Value::PromoteType(lhs.type, rhs.type)) {
    case DataType::TinyInt:
    case DataType::SmallInt:
    case DataType::Int:
    case DataType::BigInt: {
        auto value = Value(lhs.AsBigInt() != rhs.AsBigInt(), lhs.GetAllocator(), 0);
        DataTypes::Coercions::DeduceIntegerType(value);
        return value;
    }
    case DataType::Decimal:
        return Value(lhs.AsDecimal() != rhs.AsDecimal(), lhs.GetAllocator(), 0);
    case DataType::String:
        return Value(lhs.AsString() != rhs.AsString(), lhs.GetAllocator(), 0);
    case DataType::UnicodeString:
        return Value(lhs.AsUnicodeString() != rhs.AsUnicodeString(), lhs.GetAllocator(), 0);
    case DataType::Bool:
        return Value(lhs.AsBool() != rhs.AsBool(), lhs.GetAllocator(), 0);
    case DataType::DateTime:
        return Value(lhs.AsDateTime() != rhs.AsDateTime(), lhs.GetAllocator(), 0);
    case DataType::Guid:
        return Value(lhs.AsGuid() != rhs.AsGuid(), lhs.GetAllocator(), 0);
    case DataType::RowIdentifier:
    case DataType::Unknown:
    default:
        throw std::invalid_argument("Left Operand has type: "
            + ColumnTypesToStringDictionary.Get(lhs.type)
            + " and right operand has type: "
            + ColumnTypesToStringDictionary.Get(rhs.type)
        );
    }
}

const Memory::IAllocator* Value::GetAllocator() const{ return this->_allocator;}

bool Value::ParseAsBoolFromString() const{
    const auto strData = Functions::String::Lower(this->AsString());

    if (strData == "true" || strData == "1")
        return true;

    if (strData == "false" || strData == "0")
        return false;

    return false;
}

Value Value::EqualsIgnoreOrdinalCase(const Value &lhs, const Value &rhs){
    return Value(
        Functions::String::EqualsIgnoreCase(lhs.AsString(), rhs.AsString()),
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
    case DataType::UnicodeString:
        return this->InterpolateString();
    case DataType::Bool:
        return this->AsBool();
    case DataType::DateTime:
        return this->AsDateTime().UnixTimeStamp();
    case DataType::Guid:
        return this->AsGuid().Interpolate();
    case DataType::RowIdentifier:
    case DataType::Unknown:
    default:
        throw std::runtime_error("Value::Interpolate() called with unknown type");
    }
}

void Value::Resize(const Int newSize){
    this->data = new object_t[newSize];
    this->size = newSize;
}
