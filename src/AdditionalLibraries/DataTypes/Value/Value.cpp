#include "Value.h"

#include "../../Coercions/Coercions.h"
#include "../../Converter/Converter.h"
#include "../../Functions/StringFunctions.h"

#include <cstring>
#include <stdexcept>

using namespace Constants;

Value::Value()
{
    this->data = nullptr;
    this->size = 0;
    this->type = DataType::Invalid;
    this->columnIndex = 0;
    this->isIdentifier = false;
}

Value::Value(const Value &copyVal){
    this->size = copyVal.size;
    this->type = copyVal.type;
    this->columnIndex = copyVal.columnIndex;
    this->isIdentifier = copyVal.isIdentifier;

    if (copyVal.data == nullptr) {
        this->data = nullptr;
        return;
    }

    this->data = new object_t[this->size];
    memcpy(this->data, copyVal.data, this->size);
}

Value::Value(const void *data, const Constants::column_index_t &columnIndex){
    this->data = nullptr;
    this->columnIndex = columnIndex;
    this->size = 0;
    this->type = DataType::Invalid;
    this->isIdentifier = false;
}

Value::Value(const unsigned char *data, const int &size, const DataType &type){
    this->data = new object_t[size];
    memcpy(this->data, data, size);

    this->size = size;
    this->type = type;
    this->columnIndex = 0;
    this->isIdentifier = false;
}

Value::Value(const bool &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(bool)];
    memcpy(this->data, &data, sizeof(bool));
    
    this->size = sizeof(bool);
    this->columnIndex = columnIndex;
    this->type = DataType::Bool;
    this->isIdentifier = false;
}

Value::Value(const int8_t &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(int8_t)];
    memcpy(this->data, &data, sizeof(int8_t));
    
    this->size = sizeof(int8_t);
    this->columnIndex = columnIndex;
    this->type = DataType::TinyInt;
    this->isIdentifier = false;
}

Value::Value(const int16_t &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(int16_t)];
    memcpy(this->data, &data, sizeof(int16_t));
    
    this->size = sizeof(int16_t);
    this->columnIndex = columnIndex;
    this->type = DataType::SmallInt;
    this->isIdentifier = false;
}

Value::Value(const int32_t &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(int32_t)];
    memcpy(this->data, &data, sizeof(int32_t));
    
    this->size = sizeof(int32_t);
    this->columnIndex = columnIndex;
    this->type = DataType::Int;
    this->isIdentifier = false;
}

Value::Value(const int64_t &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(int64_t)];
    memcpy(this->data, &data, sizeof(int64_t));
    
    this->size = sizeof(int64_t);
    this->columnIndex = columnIndex;
    this->type = DataType::BigInt;
    this->isIdentifier = false;
}

Value::Value(const DataTypes::DateTime &data, const column_index_t &columnIndex){
    this->data = new object_t[DataTypes::DateTime::DateTimeSize()];
    memcpy(this->data, &data.GetUnixTimeStamp(), DataTypes::DateTime::DateTimeSize());
    
    this->size = DataTypes::DateTime::DateTimeSize();
    this->columnIndex = columnIndex;
    this->type = DataType::DateTime;
    this->isIdentifier = false;
}

Value::Value(const DataTypes::Decimal &data, const column_index_t &columnIndex){
    this->size = data.GetRawDataSize();
    this->data = new object_t[this->size];

    memcpy(this->data, data.GetRawData(), this->size);
    this->columnIndex = columnIndex;
    this->type = DataType::Decimal;
    this->isIdentifier = false;
}

Value::Value(const DataTypes::Guid &data, const column_index_t &columnIndex){
    this->size = data.Size();
    this->data = new object_t[this->size];
    memcpy(this->data, data.GetData().data(), this->size);

    this->columnIndex = columnIndex;
    this->type = DataType::Guid;
    this->isIdentifier = false;
}

Value::Value(const string &data, const Constants::column_index_t& columnIndex, const bool& isIdentifier)
{
    this->size = data.size();
    this->data = new object_t[this->size];
    memcpy(this->data, data.data(), this->size);
    
    this->columnIndex = columnIndex;
    this->type = DataType::String;
    this->isIdentifier = isIdentifier;
}

Value::Value(const u16string &data, const Constants::column_index_t &columnIndex)
{
    this->size = data.size();
    this->data = new object_t[this->size];
    memcpy(this->data, data.data(), this->size);
    
    this->columnIndex = columnIndex;
    this->type = DataType::UnicodeString;
    this->isIdentifier = false;
}

Value::~Value() = default;

bool Value::GetIsNull() const { return this->data == nullptr; }

const Constants::column_index_t & Value::GetColumnIndex() const { return this->columnIndex;}

void Value::SetData(const bool &data){
    delete this->data;
    
    this->data = new object_t[sizeof(bool)];
    memcpy(this->data, &data, sizeof(bool));
    this->size = sizeof(bool);

    this->type = DataType::Bool;
}

void Value::SetData(const string &data) {
    delete this->data;

    this->size = data.size();
    this->data = new object_t[this->size];
    memcpy(this->data, data.data(), this->size);

    this->type = DataType::String;
}
void Value::SetData(const u16string &data) {
    delete this->data;
    
    this->size = data.size();
    this->data = new object_t[this->size];
    memcpy(this->data, data.data(), this->size);

    this->type = DataType::UnicodeString;
}
void Value::SetData(const int8_t &data) { 
    delete this->data;
    
    this->data = new object_t[sizeof(int8_t)];
    memcpy(this->data, &data, sizeof(int8_t));
    this->size = sizeof(int8_t);

    this->type = DataType::TinyInt;
}

void Value::SetData(const int16_t &data) { 
    delete this->data;
    
    this->data = new object_t[sizeof(int16_t)];
    memcpy(this->data, &data, sizeof(int16_t));
    this->size = sizeof(int16_t);

    this->type = DataType::SmallInt;
}
void Value::SetData(const int32_t &data) { 
    delete this->data;
    
    this->data = new object_t[sizeof(int32_t)];
    memcpy(this->data, &data, sizeof(int32_t));
    this->size = sizeof(int32_t);

    this->type = DataType::Int;
}
void Value::SetData(const int64_t &data) { 
    delete this->data;
    
    this->data = new object_t[sizeof(int64_t)];
    memcpy(this->data, &data, sizeof(int64_t));
    this->size = sizeof(int64_t);

    this->type = DataType::BigInt;
}
void Value::SetData(const DataTypes::DateTime &data) { 
    delete this->data;
    
    this->data = new object_t[DataTypes::DateTime::DateTimeSize()];
    memcpy(this->data, &data.GetUnixTimeStamp(), DataTypes::DateTime::DateTimeSize());
    
    this->size = DataTypes::DateTime::DateTimeSize();

    this->type = DataType::DateTime;
}

void Value::SetData(const DataTypes::Guid &data){
    delete this->data;

    this->size = data.Size();
    this->data = new object_t[this->size];
    memcpy(this->data, data.GetData().data(), this->size);

    this->type = DataType::Guid;
}

void Value::SetName(std::string &data){
    this->name = std::move(data);
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
    const auto strData = AdditionalLibraries::StringFunctions::Lower(this->GetString());

    if (strData == "true" || strData == "1")
        return true;

    if (strData == "false" || strData == "0")
        return true;

    return false;
}

bool Value::ParseAsBoolFromString() const{
    const auto strData = AdditionalLibraries::StringFunctions::Lower(this->GetString());

    if (strData == "true" || strData == "1")
        return true;

    if (strData == "false" || strData == "0")
        return false;

    return false;
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
    const auto strData = AdditionalLibraries::StringFunctions::Lower(this->GetString());

    DataTypes::DateTime parsedDate;

    auto result = DataTypes::DateTime::FromString(parsedDate, strData);

    if (!result)
        return false;

    this->SetData(parsedDate);
    return true;
}

void Value::InferType(){

    switch (this->type){
        case DataType::BigInt:

            break;
        case DataType::Decimal:
            break;
        case DataType::String:
            if (this->TryParseDate())
                return;

            break;
        case DataType::UnicodeString:
            break;
        case DataType::Bool:
            break;
        default:
            break;
    }
}

void Value::SetData(const DataTypes::Decimal &data) { 
    delete this->data;
    
    this->size = data.GetRawDataSize();
    this->data = new object_t[this->size];
    memcpy(this->data, data.GetRawData(), this->size);

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

void Value::SetColumnIndex(const Constants::column_index_t &columnIndex) { this->columnIndex = columnIndex; }

void Value::SetType(const DataType &type){ this->type = type; }

const DataType & Value::GetType() const{ return this->type; }

const block_size_t& Value::GetSize() const{ return this->size; }

DataType Value::PromoteType(const DataType &lhs, const DataType &rhs){
    return  ColumnTypeRank.Get(lhs) > ColumnTypeRank.Get(rhs) ? lhs : rhs;
}

AdditionalDataTypes::ResultStatus Value::ValidateSize(const DataType& columnType, const block_size_t &columnMaxSize) const{
    AdditionalDataTypes::ResultStatus status;
    status.code = AdditionalDataTypes::ResultCode::Ok;

    bool isValid = true;
    switch (columnType) {
        case DataType::TinyInt:
            isValid = Converter<int8_t>::TryStoi(this->GetBigInt());
            break;
        case DataType::SmallInt:
            isValid = Converter<int16_t>::TryStoi(this->GetBigInt());
            break;
        case DataType::Int:
            isValid = Converter<int32_t>::TryStoi(this->GetBigInt());
            break;
        case DataType::BigInt:
            isValid = Converter<int64_t>::TryStoi(this->GetBigInt());
            break;
        case DataType::Decimal:
            isValid = Converter<DataTypes::Decimal>::TryStoi(this->GetDecimal(), columnMaxSize);
            break;
        case DataType::String:
        case DataType::UnicodeString:
            isValid = this->size <= columnMaxSize;
            break;
        case DataType::Bool:
            isValid = Converter<bool>::TryStoi(this->GetBigInt());
            break;
        case DataType::DateTime:
            isValid = this->size == DataTypes::DateTime::DateTimeSize();
            break;
        case DataType::Guid:
            isValid = this->size == DataTypes::Guid::GuidSize();
            break;
        case DataType::RowIdentifier:
        case DataType::Invalid:
        default:
            isValid = false;
            break;
    }

    if (isValid)
        return status;

    std::ostringstream os;
    os << "Value: " << *this << " exceeds column max size " << columnMaxSize;

    status.code = AdditionalDataTypes::ResultCode::ColumnSizeExceeded;
    status.message = os.str();

    return status;
}

bool Value::IsVariable()const{ return !this->name.empty(); }

ostream & operator<<(ostream& os, const Value &field){

    if (!field.name.empty())
        os << field.name << ": ";

    if (field.GetIsNull()) {
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
            os << field.GetString();
            break;
        case DataType::UnicodeString:
            //TODO
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

//TODO implement operations by dataType
Value operator+(const Value &lhs, const Value &rhs){
    switch (Value::PromoteType(lhs.type, rhs.type)) {
        case DataType::TinyInt:
            return Value::PerformTinyIntAddition(lhs.GetTinyInt(), rhs.GetTinyInt());
        case DataType::SmallInt:
            return Value::PerformSmallIntAddition(lhs.GetSmallInt(), rhs.GetSmallInt());
        case DataType::Int:
            return Value::PerformIntAddition(lhs.GetInt(), rhs.GetInt());
        case DataType::BigInt:
            return Value::PerformBigIntAddition(lhs.GetBigInt(), rhs.GetBigInt());
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
        case DataType::Invalid:
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
            return Value::PerformTinyIntSubtraction(lhs.GetTinyInt(), rhs.GetTinyInt());
        case DataType::SmallInt:
            return Value::PerformSmallIntSubtraction(lhs.GetSmallInt(), rhs.GetSmallInt());
        case DataType::Int:
            return Value::PerformIntSubtraction(lhs.GetInt(), rhs.GetInt());
        case DataType::BigInt:
            return Value::PerformBigIntSubtraction(lhs.GetBigInt(), rhs.GetBigInt());
        case DataType::Decimal:
            return Value::PerformDecimalSubtraction(lhs.GetDecimal(), rhs.GetDecimal());
        case DataType::String:
        case DataType::UnicodeString:
        case DataType::Bool:
        case DataType::DateTime:
        case DataType::Guid:
        case DataType::RowIdentifier:
        case DataType::Invalid:
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
            return Value(lhs.GetTinyInt() * rhs.GetTinyInt(), 0);
        case DataType::SmallInt:
            return Value(lhs.GetSmallInt() * rhs.GetSmallInt(), 0);
        case DataType::Int:
            return Value(lhs.GetInt() * rhs.GetInt(), 0);
        case DataType::BigInt:
            return Value(lhs.GetBigInt() * rhs.GetBigInt(), 0);
        case DataType::Bool:
            return Value(lhs.GetBool() * rhs.GetBool(), 0);
        case DataType::Decimal:
            return Value(lhs.GetDecimal() * rhs.GetDecimal(), 0);
        case DataType::String:
        case DataType::UnicodeString:
        case DataType::DateTime:
        case DataType::Guid:
        case DataType::RowIdentifier:
        case DataType::Invalid:
        default:
            throw std::invalid_argument("Left Operand has type: "
                + ColumnTypesToStringDictionary.Get(lhs.type)
                + " and right operand has type: "
                + ColumnTypesToStringDictionary.Get(rhs.type)
                );
    }
}

Value operator<(const Value &lhs, const Value &rhs){
    switch (Value::PromoteType(lhs.type, rhs.type)) {
        case DataType::TinyInt:
            return Value(lhs.GetTinyInt() < rhs.GetTinyInt(), 0);
        case DataType::SmallInt:
            return Value(lhs.GetSmallInt() < rhs.GetSmallInt(), 0);
        case DataType::Int:
            return Value(lhs.GetInt() < rhs.GetInt(), 0);
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
        case DataType::Invalid:
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
    switch (Value::PromoteType(lhs.type, rhs.type)) {
        case DataType::TinyInt:
            return Value(lhs.GetTinyInt() <= rhs.GetTinyInt(), 0);
        case DataType::SmallInt:
            return Value(lhs.GetSmallInt() <= rhs.GetSmallInt(), 0);
        case DataType::Int:
            return Value(lhs.GetInt() <= rhs.GetInt(), 0);
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
        case DataType::Invalid:
        default:
            throw std::invalid_argument("Left Operand has type: "
                + ColumnTypesToStringDictionary.Get(lhs.type)
                + " and right operand has type: "
                + ColumnTypesToStringDictionary.Get(rhs.type)
                );
    }
}

Value operator>=(const Value &lhs, const Value &rhs){
    switch (Value::PromoteType(lhs.type, rhs.type)) {
        case DataType::TinyInt:
            return Value(lhs.GetTinyInt() >= rhs.GetTinyInt(), 0);
        case DataType::SmallInt:
            return Value(lhs.GetSmallInt() >= rhs.GetSmallInt(), 0);
        case DataType::Int:
            return Value(lhs.GetInt() >= rhs.GetInt(), 0);
        case DataType::BigInt:
            return Value(lhs.GetBigInt() >= rhs.GetBigInt(), 0);
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
        case DataType::Invalid:
        default:
            throw std::invalid_argument("Left Operand has type: "
                + ColumnTypesToStringDictionary.Get(lhs.type)
                + " and right operand has type: "
                + ColumnTypesToStringDictionary.Get(rhs.type)
                );
    }
}

Value operator==(const Value &lhs, const Value &rhs){
    switch (Value::PromoteType(lhs.type, rhs.type)) {
        case DataType::TinyInt:
            return Value(lhs.GetTinyInt() == rhs.GetTinyInt(), 0);
        case DataType::SmallInt:
            return Value(lhs.GetSmallInt() == rhs.GetSmallInt(), 0);
        case DataType::Int:
            return Value(lhs.GetInt() == rhs.GetInt(), 0);
        case DataType::BigInt:
            return Value(lhs.GetBigInt() == rhs.GetBigInt(), 0);
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
        case DataType::Invalid:
        default:
            throw std::invalid_argument("Left Operand has type: "
                + ColumnTypesToStringDictionary.Get(lhs.type)
                + " and right operand has type: "
                + ColumnTypesToStringDictionary.Get(rhs.type)
                );
    }
}

Value operator!=(const Value &lhs, const Value &rhs){
    switch (Value::PromoteType(lhs.type, rhs.type)) {
        case DataType::TinyInt:
            return Value(lhs.GetTinyInt() != rhs.GetTinyInt(), 0);
        case DataType::SmallInt:
            return Value(lhs.GetSmallInt() != rhs.GetSmallInt(), 0);
        case DataType::Int:
            return Value(lhs.GetInt() != rhs.GetInt(), 0);
        case DataType::BigInt:
            return Value(lhs.GetBigInt() != rhs.GetBigInt(), 0);
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
        case DataType::Invalid:
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
            return Value(lhs.GetTinyInt() % rhs.GetTinyInt(), 0);
        case DataType::SmallInt:
            return Value(lhs.GetSmallInt() % rhs.GetSmallInt(), 0);
        case DataType::Int:
            return Value(lhs.GetInt() % rhs.GetInt(), 0);
        case DataType::BigInt:
            return Value(lhs.GetBigInt() % rhs.GetBigInt(), 0);
        case DataType::Bool:
            return Value(lhs.GetBool() % rhs.GetBool(), 0);
        case DataType::Decimal:
            // return Field(lhs.GetDecimal() % rhs.GetDecimal(), 0);
        case DataType::String:
        case DataType::UnicodeString:
        case DataType::DateTime:
        case DataType::Guid:
        case DataType::RowIdentifier:
        case DataType::Invalid:
        default:
            throw std::invalid_argument("Left Operand has type: "
                + ColumnTypesToStringDictionary.Get(lhs.type)
                + " and right operand has type: "
                + ColumnTypesToStringDictionary.Get(rhs.type)
                );
    }
}