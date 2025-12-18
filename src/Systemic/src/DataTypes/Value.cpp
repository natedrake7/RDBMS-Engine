#include "../../include/DataTypes/Value.h"

#include <cmath>

#include "../../include/Coercions.h"
#include "../../include/Converter.h"
#include "../../include/Functions/StringFunctions.h"

#include <cstring>
#include <stdexcept>

Value::Value()
{
    this->data = nullptr;
    this->size = 0;
    this->type = DataType::Unknown;
    this->columnIndex = 0;
}

Value::Value(const Value &copyVal){
    this->size = copyVal.size;
    this->type = copyVal.type;
    this->columnIndex = copyVal.columnIndex;

    if (copyVal.data == nullptr) {
        this->data = nullptr;
        return;
    }

    this->data = new object_t[this->size];
    memcpy(this->data, copyVal.data, this->size);
}

Value::Value(Value &&other)noexcept {
    if (this == &other)
        return;

    this->size = other.size;
    this->type = other.type;
    this->data = other.data;
    this->columnIndex = other.columnIndex;

    other.data = nullptr;
    other.size = 0;
    other.columnIndex = 0;
}

Value & Value::operator=(Value &&other) noexcept{
    if (this == &other)
        return *this;

    delete[] this->data;

    this->size = other.size;
    this->type = other.type;
    this->data = other.data;
    this->columnIndex = other.columnIndex;

    other.data = nullptr;
    other.size = 0;
    other.columnIndex = 0;

    return *this;
}

Value::Value(const void *data, const column_index_t &columnIndex){
    this->data = nullptr;
    this->columnIndex = columnIndex;
    this->size = 0;
    this->type = DataType::Unknown;
}

Value::Value(const void *data, const int &size, const DataType &type){
    this->data = nullptr;
    this->size = size;
    this->type = type;
    this->columnIndex = 0;

    this->data = new object_t[size];
    std::memcpy(this->data, data, size);
}

Value::Value(const unsigned char *data, const int &size, const DataType &type){
    this->data = new object_t[size];
    memcpy(this->data, data, size);

    this->size = size;
    this->type = type;
    this->columnIndex = 0;
}

Value::Value(const bool &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(bool)];
    memcpy(this->data, &data, sizeof(bool));
    
    this->size = sizeof(bool);
    this->columnIndex = columnIndex;
    this->type = DataType::Bool;
}

Value::Value(const int8_t &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(int8_t)];
    memcpy(this->data, &data, sizeof(int8_t));
    
    this->size = sizeof(int8_t);
    this->columnIndex = columnIndex;
    this->type = DataType::TinyInt;
}

Value::Value(const int16_t &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(int16_t)];
    memcpy(this->data, &data, sizeof(int16_t));
    
    this->size = sizeof(int16_t);
    this->columnIndex = columnIndex;
    this->type = DataType::SmallInt;
}

Value::Value(const int32_t &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(int32_t)];
    memcpy(this->data, &data, sizeof(int32_t));
    
    this->size = sizeof(int32_t);
    this->columnIndex = columnIndex;
    this->type = DataType::Int;
}

Value::Value(const int64_t &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(int64_t)];
    memcpy(this->data, &data, sizeof(int64_t));
    
    this->size = sizeof(int64_t);
    this->columnIndex = columnIndex;
    this->type = DataType::BigInt;
}

Value::Value(const DataTypes::DateTime &data, const column_index_t &columnIndex){
    this->data = new object_t[DataTypes::DateTime::DateTimeSize()];
    memcpy(this->data, &data.GetUnixTimeStamp(), DataTypes::DateTime::DateTimeSize());
    
    this->size = DataTypes::DateTime::DateTimeSize();
    this->columnIndex = columnIndex;
    this->type = DataType::DateTime;
}

Value::Value(const DataTypes::Decimal &data, const column_index_t &columnIndex){
    this->size = data.GetRawDataSize();
    this->data = new object_t[this->size];

    memcpy(this->data, data.GetRawData(), this->size);
    this->columnIndex = columnIndex;
    this->type = DataType::Decimal;
}

Value::Value(const DataTypes::Guid &data, const column_index_t &columnIndex){
    this->size = data.Size();
    this->data = new object_t[this->size];
    memcpy(this->data, data.GetData().data(), this->size);

    this->columnIndex = columnIndex;
    this->type = DataType::Guid;
}

Value::Value(const string &data, const column_index_t& columnIndex, const bool& isIdentifier)
{
    this->size = data.size();
    this->data = new object_t[this->size];
    memcpy(this->data, data.data(), this->size);
    
    this->columnIndex = columnIndex;
    this->type = DataType::String;
}

Value::Value(const u16string &data, const column_index_t &columnIndex)
{
    this->size = data.size();
    this->data = new object_t[this->size];
    memcpy(this->data, data.data(), this->size);
    
    this->columnIndex = columnIndex;
    this->type = DataType::UnicodeString;
}

Value Value::Null(const column_index_t &columnIndex) { return Value(nullptr, columnIndex); }

Value::~Value() = default;

bool Value::IsNull() const { return this->data == nullptr; }

const column_index_t & Value::GetColumnIndex() const { return this->columnIndex;}

void Value::SetData(const bool &otherData){
    delete this->data;
    
    this->data = new object_t[sizeof(bool)];
    memcpy(this->data, &otherData, sizeof(bool));
    this->size = sizeof(bool);

    this->type = DataType::Bool;
}

void Value::SetData(const string &otherData) {
    delete this->data;

    this->size = otherData.size();
    this->data = new object_t[this->size];
    memcpy(this->data, otherData.data(), this->size);

    this->type = DataType::String;
}
void Value::SetData(const u16string &otherData) {
    delete this->data;
    
    this->size = otherData.size();
    this->data = new object_t[this->size];
    memcpy(this->data, otherData.data(), this->size);

    this->type = DataType::UnicodeString;
}
void Value::SetData(const int8_t &otherData) {
    delete this->data;
    
    this->data = new object_t[sizeof(int8_t)];
    memcpy(this->data, &otherData, sizeof(int8_t));
    this->size = sizeof(int8_t);

    this->type = DataType::TinyInt;
}

void Value::SetData(const int16_t &otherData) {
    delete this->data;
    
    this->data = new object_t[sizeof(int16_t)];
    memcpy(this->data, &otherData, sizeof(int16_t));
    this->size = sizeof(int16_t);

    this->type = DataType::SmallInt;
}
void Value::SetData(const int32_t &otherData) {
    delete this->data;
    
    this->data = new object_t[sizeof(int32_t)];
    memcpy(this->data, &otherData, sizeof(int32_t));
    this->size = sizeof(int32_t);

    this->type = DataType::Int;
}
void Value::SetData(const int64_t &otherData) {
    delete this->data;
    
    this->data = new object_t[sizeof(int64_t)];
    memcpy(this->data, &otherData, sizeof(int64_t));
    this->size = sizeof(int64_t);

    this->type = DataType::BigInt;
}
void Value::SetData(const DataTypes::DateTime &otherData) {
    delete this->data;
    
    this->data = new object_t[DataTypes::DateTime::DateTimeSize()];
    memcpy(this->data, &otherData.GetUnixTimeStamp(), DataTypes::DateTime::DateTimeSize());
    
    this->size = DataTypes::DateTime::DateTimeSize();

    this->type = DataType::DateTime;
}

void Value::SetData(const DataTypes::Guid &otherData){
    delete this->data;

    this->size = otherData.Size();
    this->data = new object_t[this->size];
    memcpy(this->data, otherData.GetData().data(), this->size);

    this->type = DataType::Guid;
}

bool Value::TryParseAsBool(bool& result)const{
    if (this->type == DataType::String || this->type == DataType::UnicodeString) {
        return this->TryParseAsBoolFromString(result);
    }

    if (this->type == DataType::BigInt
        || this->type == DataType::TinyInt
        || this->type == DataType::SmallInt
        || this->type == DataType::Int) {
        return this->TryParseAsBoolFromInt(result);
    }

    return false;

}

bool Value::TryParseAsBoolFromString(bool& result)const{
    const auto strData = Functions::String::Lower(this->GetString());

    if (strData == "true" || strData == "1")
        return true;

    if (strData == "false" || strData == "0")
        return true;

    return false;
}

bool Value::ParseAsBoolFromString() const{
    const auto strData = Functions::String::Lower(this->GetString());

    if (strData == "true" || strData == "1")
        return true;

    if (strData == "false" || strData == "0")
        return false;

    return false;
}

Value Value::EqualsIgnoreOrdinalCase(const Value &lhs, const Value &rhs){
    return Value(
        Functions::String::EqualsIgnoreCase(lhs.GetString(), rhs.GetString()),
        0
    );
}

int64_t Value::Hash() const {
    return static_cast<int64_t>(0);
}

long double Value::Interpolate() const{
    switch (this->type){
        case DataType::TinyInt:
            return this->GetTinyInt();
        case DataType::SmallInt:
            return this->GetSmallInt();
        case DataType::Int:
            return this->GetInt();
        case DataType::BigInt:
            return this->GetBigInt();
        case DataType::Decimal:
            return this->GetDecimal().ToDouble();
        case DataType::String:
        case DataType::UnicodeString:
            return this->InterpolateString();
        case DataType::Bool:
            return this->GetBool();
        case DataType::DateTime:
            return this->GetDateTime().GetUnixTimeStamp();
        case DataType::Guid:
            return this->GetGuid().Interpolate();
        case DataType::RowIdentifier:
        case DataType::Unknown:
        default:
            throw std::runtime_error("Value::Interpolate() called with unknown type");
    }
}

bool Value::TryParseAsBoolFromInt(bool& result)const{
    const auto intData = this->GetBigInt();

    if (intData == 1)
        return true;

    if (intData == 0)
        return true;

    return false;
}

bool Value::TryParseDate(){
    const auto strData = Functions::String::Lower(this->GetString());

    DataTypes::DateTime parsedDate;

    auto result = DataTypes::DateTime::FromString(parsedDate, strData);

    if (!result)
        return false;

    this->SetData(parsedDate);
    return true;
}

void Value::SetData(const DataTypes::Decimal &otherData) {
    delete this->data;
    
    this->size = otherData.GetRawDataSize();
    this->data = new object_t[this->size];
    memcpy(this->data, otherData.GetRawData(), this->size);

    this->type = DataType::Decimal;
}

const object_t * Value::GetRawData() const{ return this->data; }

bool Value::GetBool() const {
    return DataTypes::Coercions::ToBool(*this);
}

int8_t Value::GetTinyInt() const {
    return DataTypes::Coercions::ToTinyInt(*this);
}

int16_t Value::GetSmallInt() const {
    return DataTypes::Coercions::ToSmallInt(*this);
}

int32_t Value::GetInt() const {
    return DataTypes::Coercions::ToInt(*this);
}

int64_t Value::GetBigInt() const {
    return DataTypes::Coercions::ToBigInt(*this);
}

string Value::GetString() const {
    return DataTypes::Coercions::ToString(*this);
}

u16string Value::GetUnicodeString() const {
    return DataTypes::Coercions::ToUnicodeString(*this);
}

DataTypes::Decimal Value::GetDecimal() const {
    return DataTypes::Coercions::ToDecimal(*this);
}

DataTypes::DateTime Value::GetDateTime() const {
    return DataTypes::Coercions::ToDateTime(*this);
}

DataTypes::Guid Value::GetGuid() const {
    return DataTypes::Coercions::ToGuid(*this);
}

time_t Value::GetUnixTimeStamp() const{ return *reinterpret_cast<time_t *>(this->data); }

void Value::SetColumnIndex(const column_index_t &otherIndex) { this->columnIndex = otherIndex; }

void Value::SetType(const DataType &otherType){ this->type = otherType; }

void Value::Deserialize(const std::vector<char> &buffer, uint32_t &offset){
    memcpy(&this->size, buffer.data() + offset, sizeof(block_size_t));
    offset += sizeof(block_size_t);

    memcpy(&this->type, buffer.data() + offset, sizeof(DataType));
    offset += sizeof(DataType);

    this->data = new object_t[this->size];
    memcpy(this->data, buffer.data() + offset, sizeof(object_t) * this->size);
    offset += this->size;
}

const DataType & Value::GetType() const{ return this->type; }

const block_size_t& Value::GetSize() const{ return this->size; }

DataType Value::PromoteType(const DataType &lhs, const DataType &rhs){
    return  ColumnTypeRank.Get(lhs) > ColumnTypeRank.Get(rhs) ? lhs : rhs;
}

ostream & operator<<(ostream& os, const Value &field){
    if (field.IsNull()) {
        os << "NULL";
        return os;
    }

    switch (field.type){
        case DataType::TinyInt:
            os << field.GetTinyInt();
            break;
        case DataType::SmallInt:
            os << field.GetSmallInt();
            break;
        case DataType::Int:
            os << field.GetInt();
            break;
        case DataType::BigInt:
            os << field.GetBigInt();
            break;
        case DataType::Decimal:
            os << field.GetDecimal();
            break;
        case DataType::String:
        case DataType::UnicodeString:
            os << field.GetString();
            break;
        case DataType::Bool:
            os << field.GetBool();
            break;
        case DataType::DateTime:
            os << field.GetDateTime();
            break;
        case DataType::Guid:
            os << field.GetGuid();
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

    if (rhs.data == nullptr) {
        this->data = nullptr;
        return *this;
    }

    this->data = new object_t[this->size];
    memcpy(this->data, rhs.data, this->size);

    return *this;
}

Value Value::PerformTinyIntAddition(const int8_t &lhs, const int8_t &rhs){
        return
        (Converter<int8_t>::AssertOverflow(lhs, rhs))
        ?
            Value(
                static_cast<int16_t>(lhs +  rhs),
                0
            )
        :
            Value(
                static_cast<int8_t>(lhs + rhs),
                0
            ) ;
}

Value Value::PerformSmallIntAddition(const int16_t &lhs, const int16_t &rhs){
    return
    (Converter<int16_t>::AssertOverflow(lhs, rhs))
    ?
        Value(
            static_cast<int32_t>(lhs +  rhs),
            0
        )
    :
        Value(
            static_cast<int16_t>(lhs + rhs),
            0
        ) ;
}

Value Value::PerformIntAddition(const int32_t &lhs, const int32_t &rhs){
    return
    (Converter<int32_t>::AssertOverflow(lhs, rhs))
    ?
        Value(
            static_cast<int64_t>(lhs +  rhs),
            0
        )
    :
        Value(
            static_cast<int32_t>(lhs + rhs),
            0
        ) ;
}

Value Value::PerformBigIntAddition(const int64_t &lhs, const int64_t &rhs){
    return Value(
        lhs +  rhs,
        0
    );
}

Value Value::PerformStringAddition(const string &lhs, const string &rhs){
    return Value(
        lhs + rhs,
        0
    );

}

Value Value::PerformDecimalAddition(const DataTypes::Decimal &lhs, const DataTypes::Decimal &rhs){
    return Value(lhs + rhs, 0);
}

Value Value::PerformTinyIntSubtraction(const int8_t &lhs, const int8_t &rhs){
    return
        (Converter<int8_t>::AssertOverflow(lhs, rhs))
        ?
            Value(
                static_cast<int16_t>(lhs -  rhs),
                0
            )
        :
            Value(
                static_cast<int8_t>(lhs - rhs),
                0
            ) ;
}

Value Value::PerformSmallIntSubtraction(const int16_t &lhs, const int16_t &rhs){
    return
        (Converter<int16_t>::AssertOverflow(lhs, rhs))
        ?
            Value(
                static_cast<int32_t>(lhs -  rhs),
                0
            )
        :
            Value(
                static_cast<int16_t>(lhs - rhs),
                0
            ) ;
}

Value Value::PerformIntSubtraction(const int32_t &lhs, const int32_t &rhs){
    return
        (Converter<int32_t>::AssertOverflow(lhs, rhs))
        ?
            Value(
                static_cast<int64_t>(lhs -  rhs),
                0
            )
        :
            Value(
                static_cast<int32_t>(lhs - rhs),
                0
            ) ;
}

Value Value::PerformBigIntSubtraction(const int64_t &lhs, const int64_t &rhs){
        return Value(
            lhs -  rhs,
            0
        );
}

Value Value::PerformDecimalSubtraction(const DataTypes::Decimal &lhs, const DataTypes::Decimal &rhs){
    return Value(lhs - rhs, 0);
}

std::tuple<bool, Value> Value::PerformNullEqualityComparison(const Value &lhs, const Value &rhs){
    if (lhs.IsNull())
        return std::make_tuple(true, Value(rhs.IsNull(), 0));

    if (rhs.IsNull())
        return std::make_tuple(true, Value(false, 0));

    return std::make_tuple(false, Value(nullptr, 0));
}

std::tuple<bool, Value> Value::PerformNullInEqualityComparison(const Value &lhs, const Value &rhs){
    if (lhs.IsNull())
        return std::make_tuple(true, Value(!rhs.IsNull(), 0));

    if (rhs.IsNull())
        return std::make_tuple(true, Value(true, 0));

    return std::make_tuple(false, Value(nullptr, 0));
}

long double Value::InterpolateString() const{
    const auto str = this->GetString();

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

//TODO implement operations by dataType
Value operator+(const Value &lhs, const Value &rhs){
    switch (Value::PromoteType(lhs.type, rhs.type)) {
        case DataType::TinyInt:
        case DataType::SmallInt:
        case DataType::Int:
        case DataType::BigInt: {
            auto value = Value::PerformBigIntAddition(lhs.GetBigInt(), rhs.GetBigInt());
            DataTypes::Coercions::DeduceIntegerType(value);
            return value;
        }
        case DataType::Decimal:
            return Value::PerformDecimalAddition(lhs.GetDecimal(), rhs.GetDecimal());
        case DataType::String:
        case DataType::UnicodeString:
                return Value::PerformStringAddition(lhs.GetString(), rhs.GetString());
        case DataType::Bool:
                return Value(lhs.GetBool() + rhs.GetBool(), 0);
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
            auto value = Value::PerformBigIntSubtraction(lhs.GetBigInt(), rhs.GetBigInt());
            DataTypes::Coercions::DeduceIntegerType(value);
            return value;
        }
        case DataType::Decimal:
            return Value::PerformDecimalSubtraction(lhs.GetDecimal(), rhs.GetDecimal());
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
    return Value(nullptr, 0);
}

Value operator*(const Value &lhs, const Value &rhs){
    switch (Value::PromoteType(lhs.type, rhs.type)) {
        case DataType::TinyInt:
        case DataType::SmallInt:
        case DataType::Int:
        case DataType::BigInt: {
            auto value = Value(lhs.GetBigInt() * rhs.GetBigInt(), 0);
            DataTypes::Coercions::DeduceIntegerType(value);
            return value;
        }
        case DataType::Bool:
            return Value(lhs.GetBool() * rhs.GetBool(), 0);
        case DataType::Decimal:
            return Value(lhs.GetDecimal() * rhs.GetDecimal(), 0);
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
    const auto& [returnOutput, output] = Value::PerformNullInEqualityComparison(lhs, rhs);
    if (returnOutput)
        return output;

    switch (Value::PromoteType(lhs.type, rhs.type)) {
        case DataType::TinyInt:
        case DataType::SmallInt:
        case DataType::Int:
        case DataType::BigInt:
            return Value(lhs.GetBigInt() < rhs.GetBigInt(), 0);
        case DataType::Decimal:
            return Value(lhs.GetDecimal() < rhs.GetDecimal(), 0);
        case DataType::String:
            return Value(lhs.GetString() < rhs.GetString(), 0);
        case DataType::UnicodeString:
            return Value(lhs.GetUnicodeString() < rhs.GetUnicodeString(), 0);
        case DataType::Bool:
            return Value(lhs.GetBool() < rhs.GetBool(), 0);
        case DataType::DateTime:
            return Value(lhs.GetDateTime() < rhs.GetDateTime(), 0);
        case DataType::Guid:
            return Value(lhs.GetGuid() < rhs.GetGuid(), 0);
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
    const auto& [returnOutput, _] = Value::PerformNullEqualityComparison(lhs, rhs);
    if (returnOutput)
        return Value(true, 0);

    switch (Value::PromoteType(lhs.type, rhs.type)) {
        case DataType::TinyInt:
        case DataType::SmallInt:
        case DataType::Int:
        case DataType::BigInt:
            return Value(lhs.GetBigInt() <= rhs.GetBigInt(), 0);
        case DataType::Decimal:
            return Value(lhs.GetDecimal() <= rhs.GetDecimal(), 0);
        case DataType::String:
            return Value(lhs.GetString() <= rhs.GetString(), 0);
        case DataType::UnicodeString:
            return Value(lhs.GetUnicodeString() <= rhs.GetUnicodeString(), 0);
        case DataType::Bool:
            return Value(lhs.GetBool() <= rhs.GetBool(), 0);
        case DataType::DateTime:
            return Value(lhs.GetDateTime() <= rhs.GetDateTime(), 0);
        case DataType::Guid:
            return Value(lhs.GetGuid() <= rhs.GetGuid(), 0);
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
    const auto& [returnOutput, _] = Value::PerformNullEqualityComparison(lhs, rhs);
    if (returnOutput)
        return Value(true, 0);

    switch (Value::PromoteType(lhs.type, rhs.type)) {
        case DataType::TinyInt:
        case DataType::SmallInt:
        case DataType::Int:
        case DataType::BigInt: {
            const auto left = lhs.GetBigInt();
            const auto right = rhs.GetBigInt();

            return Value(left >= right, 0);
        }
        case DataType::Decimal:
            return Value(lhs.GetDecimal() >= rhs.GetDecimal(), 0);
        case DataType::String:
            return Value(lhs.GetString() >= rhs.GetString(), 0);
        case DataType::UnicodeString:
            return Value(lhs.GetUnicodeString() >= rhs.GetUnicodeString(), 0);
        case DataType::Bool:
            return Value(lhs.GetBool() >= rhs.GetBool(), 0);
        case DataType::DateTime:
            return Value(lhs.GetDateTime() >= rhs.GetDateTime(), 0);
        case DataType::Guid:
            return Value(lhs.GetGuid() >= rhs.GetGuid(), 0);
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
            auto value = Value(lhs.GetBigInt() == rhs.GetBigInt(), 0);
            DataTypes::Coercions::DeduceIntegerType(value);
            return value;
        }
        case DataType::Decimal:
            return Value(lhs.GetDecimal() == rhs.GetDecimal(), 0);
        case DataType::String:
            return Value(lhs.GetString() == rhs.GetString(), 0);
        case DataType::UnicodeString:
            return Value(lhs.GetUnicodeString() == rhs.GetUnicodeString(), 0);
        case DataType::Bool:
            return Value(lhs.GetBool() == rhs.GetBool(), 0);
        case DataType::DateTime:
            return Value(lhs.GetDateTime() == rhs.GetDateTime(), 0);
        case DataType::Guid:
            return Value(lhs.GetGuid() == rhs.GetGuid(), 0);
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
            auto value = Value(lhs.GetBigInt() != rhs.GetBigInt(), 0);
            DataTypes::Coercions::DeduceIntegerType(value);
            return value;
        }
        case DataType::Decimal:
            return Value(lhs.GetDecimal() != rhs.GetDecimal(), 0);
        case DataType::String:
            return Value(lhs.GetString() != rhs.GetString(), 0);
        case DataType::UnicodeString:
            return Value(lhs.GetUnicodeString() != rhs.GetUnicodeString(), 0);
        case DataType::Bool:
            return Value(lhs.GetBool() != rhs.GetBool(), 0);
        case DataType::DateTime:
            return Value(lhs.GetDateTime() != rhs.GetDateTime(), 0);
        case DataType::Guid:
            return Value(lhs.GetGuid() != rhs.GetGuid(), 0);
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

Value operator%(const Value &lhs, const Value &rhs){
    switch (Value::PromoteType(lhs.type, rhs.type)) {
        case DataType::TinyInt:
        case DataType::SmallInt:
        case DataType::Int:
        case DataType::BigInt: {
            auto value = Value(lhs.GetBigInt() % rhs.GetBigInt(), 0);
            DataTypes::Coercions::DeduceIntegerType(value);
            return value;
        }
        case DataType::Bool:
            return Value(lhs.GetBool() % rhs.GetBool(), 0);
        case DataType::Decimal:
            // return Field(lhs.GetDecimal() % rhs.GetDecimal(), 0);
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