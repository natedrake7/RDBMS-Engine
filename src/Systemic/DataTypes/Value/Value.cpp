#include "Value.h"

#include "../../Coercions/Coercions.h"
#include "../../Converter/Converter.h"
#include "../../Functions/StringFunctions.h"

#include <cstring>
#include <stdexcept>

Value::Value()
{
    this->data = nullptr;
    this->size = 0;
    this->type = Constants::DataType::Unknown;
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

    this->data = new Constants::object_t[this->size];
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

Value::Value(const void *data, const Constants::column_index_t &columnIndex){
    this->data = nullptr;
    this->columnIndex = columnIndex;
    this->size = 0;
    this->type = Constants::DataType::Unknown;
}

Value::Value(const void *data, const int &size, const Constants::DataType &type){
    this->data = nullptr;
    this->size = size;
    this->type = type;
    this->columnIndex = 0;

    this->data = new Constants::object_t[size];
    std::memcpy(this->data, data, size);
}

Value::Value(const unsigned char *data, const int &size, const Constants::DataType &type){
    this->data = new Constants::object_t[size];
    memcpy(this->data, data, size);

    this->size = size;
    this->type = type;
    this->columnIndex = 0;
}

Value::Value(const bool &data, const column_index_t &columnIndex){
    this->data = new Constants::object_t[sizeof(bool)];
    memcpy(this->data, &data, sizeof(bool));
    
    this->size = sizeof(bool);
    this->columnIndex = columnIndex;
    this->type = Constants::DataType::Bool;
}

Value::Value(const int8_t &data, const column_index_t &columnIndex){
    this->data = new Constants::object_t[sizeof(int8_t)];
    memcpy(this->data, &data, sizeof(int8_t));
    
    this->size = sizeof(int8_t);
    this->columnIndex = columnIndex;
    this->type = Constants::DataType::TinyInt;
}

Value::Value(const int16_t &data, const column_index_t &columnIndex){
    this->data = new Constants::object_t[sizeof(int16_t)];
    memcpy(this->data, &data, sizeof(int16_t));
    
    this->size = sizeof(int16_t);
    this->columnIndex = columnIndex;
    this->type = Constants::DataType::SmallInt;
}

Value::Value(const int32_t &data, const column_index_t &columnIndex){
    this->data = new Constants::object_t[sizeof(int32_t)];
    memcpy(this->data, &data, sizeof(int32_t));
    
    this->size = sizeof(int32_t);
    this->columnIndex = columnIndex;
    this->type = Constants::DataType::Int;
}

Value::Value(const int64_t &data, const column_index_t &columnIndex){
    this->data = new Constants::object_t[sizeof(int64_t)];
    memcpy(this->data, &data, sizeof(int64_t));
    
    this->size = sizeof(int64_t);
    this->columnIndex = columnIndex;
    this->type = Constants::DataType::BigInt;
}

Value::Value(const DataTypes::DateTime &data, const column_index_t &columnIndex){
    this->data = new Constants::object_t[DataTypes::DateTime::DateTimeSize()];
    memcpy(this->data, &data.GetUnixTimeStamp(), DataTypes::DateTime::DateTimeSize());
    
    this->size = DataTypes::DateTime::DateTimeSize();
    this->columnIndex = columnIndex;
    this->type = Constants::DataType::DateTime;
}

Value::Value(const DataTypes::Decimal &data, const column_index_t &columnIndex){
    this->size = data.GetRawDataSize();
    this->data = new Constants::object_t[this->size];

    memcpy(this->data, data.GetRawData(), this->size);
    this->columnIndex = columnIndex;
    this->type = Constants::DataType::Decimal;
}

Value::Value(const DataTypes::Guid &data, const column_index_t &columnIndex){
    this->size = data.Size();
    this->data = new Constants::object_t[this->size];
    memcpy(this->data, data.GetData().data(), this->size);

    this->columnIndex = columnIndex;
    this->type = Constants::DataType::Guid;
}

Value::Value(const string &data, const Constants::column_index_t& columnIndex, const bool& isIdentifier)
{
    this->size = data.size();
    this->data = new Constants::object_t[this->size];
    memcpy(this->data, data.data(), this->size);
    
    this->columnIndex = columnIndex;
    this->type = Constants::DataType::String;
}

Value::Value(const u16string &data, const Constants::column_index_t &columnIndex)
{
    this->size = data.size();
    this->data = new Constants::object_t[this->size];
    memcpy(this->data, data.data(), this->size);
    
    this->columnIndex = columnIndex;
    this->type = Constants::DataType::UnicodeString;
}

Value::~Value() = default;

bool Value::IsNull() const { return this->data == nullptr; }

const Constants::column_index_t & Value::GetColumnIndex() const { return this->columnIndex;}

void Value::SetData(const bool &otherData){
    delete this->data;
    
    this->data = new Constants::object_t[sizeof(bool)];
    memcpy(this->data, &otherData, sizeof(bool));
    this->size = sizeof(bool);

    this->type = Constants::DataType::Bool;
}

void Value::SetData(const string &otherData) {
    delete this->data;

    this->size = otherData.size();
    this->data = new Constants::object_t[this->size];
    memcpy(this->data, otherData.data(), this->size);

    this->type = Constants::DataType::String;
}
void Value::SetData(const u16string &otherData) {
    delete this->data;
    
    this->size = otherData.size();
    this->data = new Constants::object_t[this->size];
    memcpy(this->data, otherData.data(), this->size);

    this->type = Constants::DataType::UnicodeString;
}
void Value::SetData(const int8_t &otherData) {
    delete this->data;
    
    this->data = new Constants::object_t[sizeof(int8_t)];
    memcpy(this->data, &otherData, sizeof(int8_t));
    this->size = sizeof(int8_t);

    this->type = Constants::DataType::TinyInt;
}

void Value::SetData(const int16_t &otherData) {
    delete this->data;
    
    this->data = new Constants::object_t[sizeof(int16_t)];
    memcpy(this->data, &otherData, sizeof(int16_t));
    this->size = sizeof(int16_t);

    this->type = Constants::DataType::SmallInt;
}
void Value::SetData(const int32_t &otherData) {
    delete this->data;
    
    this->data = new Constants::object_t[sizeof(int32_t)];
    memcpy(this->data, &otherData, sizeof(int32_t));
    this->size = sizeof(int32_t);

    this->type = Constants::DataType::Int;
}
void Value::SetData(const int64_t &otherData) {
    delete this->data;
    
    this->data = new Constants::object_t[sizeof(int64_t)];
    memcpy(this->data, &otherData, sizeof(int64_t));
    this->size = sizeof(int64_t);

    this->type = Constants::DataType::BigInt;
}
void Value::SetData(const DataTypes::DateTime &otherData) {
    delete this->data;
    
    this->data = new Constants::object_t[DataTypes::DateTime::DateTimeSize()];
    memcpy(this->data, &otherData.GetUnixTimeStamp(), DataTypes::DateTime::DateTimeSize());
    
    this->size = DataTypes::DateTime::DateTimeSize();

    this->type = Constants::DataType::DateTime;
}

void Value::SetData(const DataTypes::Guid &otherData){
    delete this->data;

    this->size = otherData.Size();
    this->data = new Constants::object_t[this->size];
    memcpy(this->data, otherData.GetData().data(), this->size);

    this->type = Constants::DataType::Guid;
}

bool Value::TryParseAsBool(bool& result)const{
    if (this->type == Constants::DataType::String || this->type == Constants::DataType::UnicodeString) {
        return this->TryParseAsBoolFromString(result);
    }

    if (this->type == Constants::DataType::BigInt
        || this->type == Constants::DataType::TinyInt
        || this->type == Constants::DataType::SmallInt
        || this->type == Constants::DataType::Int) {
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
    this->data = new Constants::object_t[this->size];
    memcpy(this->data, otherData.GetRawData(), this->size);

    this->type = Constants::DataType::Decimal;
}

const Constants::object_t * Value::GetRawData() const{ return this->data; }

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

void Value::SetColumnIndex(const Constants::column_index_t &otherIndex) { this->columnIndex = otherIndex; }

void Value::SetType(const Constants::DataType &otherType){ this->type = otherType; }

void Value::Deserialize(const std::vector<char> &buffer, uint32_t &offset){
    memcpy(&this->size, buffer.data() + offset, sizeof(Constants::block_size_t));
    offset += sizeof(Constants::block_size_t);

    memcpy(&this->type, buffer.data() + offset, sizeof(Constants::DataType));
    offset += sizeof(Constants::DataType);

    this->data = new object_t[this->size];
    memcpy(this->data, buffer.data() + offset, sizeof(Constants::object_t) * this->size);
    offset += this->size;
}

const Constants::DataType & Value::GetType() const{ return this->type; }

const block_size_t& Value::GetSize() const{ return this->size; }

Constants::DataType Value::PromoteType(const Constants::DataType &lhs, const Constants::DataType &rhs){
    return  ColumnTypeRank.Get(lhs) > ColumnTypeRank.Get(rhs) ? lhs : rhs;
}

ostream & operator<<(ostream& os, const Value &field){
    if (field.IsNull()) {
        os << "NULL";
        return os;
    }

    switch (field.type){
        case Constants::DataType::TinyInt:
            os << field.GetTinyInt();
            break;
        case Constants::DataType::SmallInt:
            os << field.GetSmallInt();
            break;
        case Constants::DataType::Int:
            os << field.GetInt();
            break;
        case Constants::DataType::BigInt:
            os << field.GetBigInt();
            break;
        case Constants::DataType::Decimal:
            os << field.GetDecimal();
            break;
        case Constants::DataType::String:
        case Constants::DataType::UnicodeString:
            os << field.GetString();
            break;
        case Constants::DataType::Bool:
            os << field.GetBool();
            break;
        case Constants::DataType::DateTime:
            os << field.GetDateTime();
            break;
        case Constants::DataType::Guid:
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

    this->data = new Constants::object_t[this->size];
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

//TODO implement operations by dataType
Value operator+(const Value &lhs, const Value &rhs){
    switch (Value::PromoteType(lhs.type, rhs.type)) {
        case Constants::DataType::TinyInt:
        case Constants::DataType::SmallInt:
        case Constants::DataType::Int:
        case Constants::DataType::BigInt: {
            auto value = Value::PerformBigIntAddition(lhs.GetBigInt(), rhs.GetBigInt());
            DataTypes::Coercions::DeduceIntegerType(value);
            return value;
        }
        case Constants::DataType::Decimal:
            return Value::PerformDecimalAddition(lhs.GetDecimal(), rhs.GetDecimal());
        case Constants::DataType::String:
        case Constants::DataType::UnicodeString:
                return Value::PerformStringAddition(lhs.GetString(), rhs.GetString());
        case Constants::DataType::Bool:
                return Value(lhs.GetBool() + rhs.GetBool(), 0);
        case Constants::DataType::DateTime:
        case Constants::DataType::Guid:
        case Constants::DataType::RowIdentifier:
        case Constants::DataType::Unknown:
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
        case Constants::DataType::TinyInt:
        case Constants::DataType::SmallInt:
        case Constants::DataType::Int:
        case Constants::DataType::BigInt: {
            auto value = Value::PerformBigIntSubtraction(lhs.GetBigInt(), rhs.GetBigInt());
            DataTypes::Coercions::DeduceIntegerType(value);
            return value;
        }
        case Constants::DataType::Decimal:
            return Value::PerformDecimalSubtraction(lhs.GetDecimal(), rhs.GetDecimal());
        case Constants::DataType::String:
        case Constants::DataType::UnicodeString:
        case Constants::DataType::Bool:
        case Constants::DataType::DateTime:
        case Constants::DataType::Guid:
        case Constants::DataType::RowIdentifier:
        case Constants::DataType::Unknown:
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
        case Constants::DataType::TinyInt:
        case Constants::DataType::SmallInt:
        case Constants::DataType::Int:
        case Constants::DataType::BigInt: {
            auto value = Value(lhs.GetBigInt() * rhs.GetBigInt(), 0);
            DataTypes::Coercions::DeduceIntegerType(value);
            return value;
        }
        case Constants::DataType::Bool:
            return Value(lhs.GetBool() * rhs.GetBool(), 0);
        case Constants::DataType::Decimal:
            return Value(lhs.GetDecimal() * rhs.GetDecimal(), 0);
        case Constants::DataType::String:
        case Constants::DataType::UnicodeString:
        case Constants::DataType::DateTime:
        case Constants::DataType::Guid:
        case Constants::DataType::RowIdentifier:
        case Constants::DataType::Unknown:
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
        case Constants::DataType::TinyInt:
        case Constants::DataType::SmallInt:
        case Constants::DataType::Int:
        case Constants::DataType::BigInt:
            return Value(lhs.GetBigInt() < rhs.GetBigInt(), 0);
        case Constants::DataType::Decimal:
            return Value(lhs.GetDecimal() < rhs.GetDecimal(), 0);
        case Constants::DataType::String:
            return Value(lhs.GetString() < rhs.GetString(), 0);
        case Constants::DataType::UnicodeString:
            return Value(lhs.GetUnicodeString() < rhs.GetUnicodeString(), 0);
        case Constants::DataType::Bool:
            return Value(lhs.GetBool() < rhs.GetBool(), 0);
        case Constants::DataType::DateTime:
            return Value(lhs.GetDateTime() < rhs.GetDateTime(), 0);
        case Constants::DataType::Guid:
            return Value(lhs.GetGuid() < rhs.GetGuid(), 0);
        case Constants::DataType::RowIdentifier:
        case Constants::DataType::Unknown:
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
        case Constants::DataType::TinyInt:
        case Constants::DataType::SmallInt:
        case Constants::DataType::Int:
        case Constants::DataType::BigInt:
            return Value(lhs.GetBigInt() <= rhs.GetBigInt(), 0);
        case Constants::DataType::Decimal:
            return Value(lhs.GetDecimal() <= rhs.GetDecimal(), 0);
        case Constants::DataType::String:
            return Value(lhs.GetString() <= rhs.GetString(), 0);
        case Constants::DataType::UnicodeString:
            return Value(lhs.GetUnicodeString() <= rhs.GetUnicodeString(), 0);
        case Constants::DataType::Bool:
            return Value(lhs.GetBool() <= rhs.GetBool(), 0);
        case Constants::DataType::DateTime:
            return Value(lhs.GetDateTime() <= rhs.GetDateTime(), 0);
        case Constants::DataType::Guid:
            return Value(lhs.GetGuid() <= rhs.GetGuid(), 0);
        case Constants::DataType::RowIdentifier:
        case Constants::DataType::Unknown:
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
        case Constants::DataType::TinyInt:
        case Constants::DataType::SmallInt:
        case Constants::DataType::Int:
        case Constants::DataType::BigInt: {
            const auto left = lhs.GetBigInt();
            const auto right = rhs.GetBigInt();

            return Value(left >= right, 0);
        }
        case Constants::DataType::Decimal:
            return Value(lhs.GetDecimal() >= rhs.GetDecimal(), 0);
        case Constants::DataType::String:
            return Value(lhs.GetString() >= rhs.GetString(), 0);
        case Constants::DataType::UnicodeString:
            return Value(lhs.GetUnicodeString() >= rhs.GetUnicodeString(), 0);
        case Constants::DataType::Bool:
            return Value(lhs.GetBool() >= rhs.GetBool(), 0);
        case Constants::DataType::DateTime:
            return Value(lhs.GetDateTime() >= rhs.GetDateTime(), 0);
        case Constants::DataType::Guid:
            return Value(lhs.GetGuid() >= rhs.GetGuid(), 0);
        case Constants::DataType::RowIdentifier:
        case Constants::DataType::Unknown:
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
        case Constants::DataType::TinyInt:
        case Constants::DataType::SmallInt:
        case Constants::DataType::Int:
        case Constants::DataType::BigInt: {
            auto value = Value(lhs.GetBigInt() == rhs.GetBigInt(), 0);
            DataTypes::Coercions::DeduceIntegerType(value);
            return value;
        }
        case Constants::DataType::Decimal:
            return Value(lhs.GetDecimal() == rhs.GetDecimal(), 0);
        case Constants::DataType::String:
            return Value(lhs.GetString() == rhs.GetString(), 0);
        case Constants::DataType::UnicodeString:
            return Value(lhs.GetUnicodeString() == rhs.GetUnicodeString(), 0);
        case Constants::DataType::Bool:
            return Value(lhs.GetBool() == rhs.GetBool(), 0);
        case Constants::DataType::DateTime:
            return Value(lhs.GetDateTime() == rhs.GetDateTime(), 0);
        case Constants::DataType::Guid:
            return Value(lhs.GetGuid() == rhs.GetGuid(), 0);
        case Constants::DataType::RowIdentifier:
        case Constants::DataType::Unknown:
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
        case Constants::DataType::TinyInt:
        case Constants::DataType::SmallInt:
        case Constants::DataType::Int:
        case Constants::DataType::BigInt: {
            auto value = Value(lhs.GetBigInt() != rhs.GetBigInt(), 0);
            DataTypes::Coercions::DeduceIntegerType(value);
            return value;
        }
        case Constants::DataType::Decimal:
            return Value(lhs.GetDecimal() != rhs.GetDecimal(), 0);
        case Constants::DataType::String:
            return Value(lhs.GetString() != rhs.GetString(), 0);
        case Constants::DataType::UnicodeString:
            return Value(lhs.GetUnicodeString() != rhs.GetUnicodeString(), 0);
        case Constants::DataType::Bool:
            return Value(lhs.GetBool() != rhs.GetBool(), 0);
        case Constants::DataType::DateTime:
            return Value(lhs.GetDateTime() != rhs.GetDateTime(), 0);
        case Constants::DataType::Guid:
            return Value(lhs.GetGuid() != rhs.GetGuid(), 0);
        case Constants::DataType::RowIdentifier:
        case Constants::DataType::Unknown:
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
        case Constants::DataType::TinyInt:
        case Constants::DataType::SmallInt:
        case Constants::DataType::Int:
        case Constants::DataType::BigInt: {
            auto value = Value(lhs.GetBigInt() % rhs.GetBigInt(), 0);
            DataTypes::Coercions::DeduceIntegerType(value);
            return value;
        }
        case Constants::DataType::Bool:
            return Value(lhs.GetBool() % rhs.GetBool(), 0);
        case Constants::DataType::Decimal:
            // return Field(lhs.GetDecimal() % rhs.GetDecimal(), 0);
        case Constants::DataType::String:
        case Constants::DataType::UnicodeString:
        case Constants::DataType::DateTime:
        case Constants::DataType::Guid:
        case Constants::DataType::RowIdentifier:
        case Constants::DataType::Unknown:
        default:
            throw std::invalid_argument("Left Operand has type: "
                + ColumnTypesToStringDictionary.Get(lhs.type)
                + " and right operand has type: "
                + ColumnTypesToStringDictionary.Get(rhs.type)
                );
    }
}