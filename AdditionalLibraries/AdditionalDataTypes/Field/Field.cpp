#include "Field.h"

#include "../../SafeConverter/SafeConverter.h"
#include "../../StringFunctions/StringFunctions.h"

#include <cstring>
#include <stdexcept>

using namespace Constants;

Field::Field()
{
    this->data = nullptr;
    this->columnIndex = 0;
    this->isIdentifier = false;
}

Field::Field(const void *data, const Constants::column_index_t &columnIndex){
    this->data = nullptr;
    this->columnIndex = columnIndex;
    this->type = ColumnType::ColumnTypeCount;
    this->isIdentifier = false;
}

Field::Field(const bool &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(bool)];
    memcpy(this->data, &data, sizeof(bool));
    
    this->size = sizeof(bool);
    this->columnIndex = columnIndex;
    this->type = ColumnType::Bool;
    this->isIdentifier = false;
}

Field::Field(const int8_t &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(int8_t)];
    memcpy(this->data, &data, sizeof(int8_t));
    
    this->size = sizeof(int8_t);
    this->columnIndex = columnIndex;
    this->type = ColumnType::TinyInt;
    this->isIdentifier = false;
}

Field::Field(const int16_t &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(int16_t)];
    memcpy(this->data, &data, sizeof(int16_t));
    
    this->size = sizeof(int16_t);
    this->columnIndex = columnIndex;
    this->type = ColumnType::SmallInt;
    this->isIdentifier = false;
}

Field::Field(const int32_t &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(int32_t)];
    memcpy(this->data, &data, sizeof(int32_t));
    
    this->size = sizeof(int32_t);
    this->columnIndex = columnIndex;
    this->type = ColumnType::Int;
    this->isIdentifier = false;
}

Field::Field(const int64_t &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(int64_t)];
    memcpy(this->data, &data, sizeof(int64_t));
    
    this->size = sizeof(int64_t);
    this->columnIndex = columnIndex;
    this->type = ColumnType::BigInt;
    this->isIdentifier = false;
}

Field::Field(const DataTypes::DateTime &data, const column_index_t &columnIndex){
    this->data = new object_t[DataTypes::DateTime::DateTimeSize()];
    memcpy(this->data, &data.GetUnixTimeStamp(), DataTypes::DateTime::DateTimeSize());
    
    this->size = DataTypes::DateTime::DateTimeSize();
    this->columnIndex = columnIndex;
    this->type = ColumnType::DateTime;
    this->isIdentifier = false;
}

Field::Field(const DataTypes::Decimal &data, const column_index_t &columnIndex){
    this->size = data.GetRawDataSize();
    this->data = new object_t[this->size];

    memcpy(this->data, data.GetRawData(), this->size);
    this->columnIndex = columnIndex;
    this->type = ColumnType::Decimal;
    this->isIdentifier = false;
}

Field::Field(const DataTypes::Guid &data, const column_index_t &columnIndex){
    this->size = data.Size();
    this->data = new object_t[this->size];
    memcpy(this->data, data.GetData().data(), this->size);

    this->columnIndex = columnIndex;
    this->type = ColumnType::Guid;
    this->isIdentifier = false;
}

Field::Field(const string &data, const Constants::column_index_t& columnIndex, const bool& isIdentifier)
{
    this->size = data.size();
    this->data = new object_t[this->size];
    memcpy(this->data, data.data(), this->size);
    
    this->columnIndex = columnIndex;
    this->type = ColumnType::String;
    this->isIdentifier = isIdentifier;
}

Field::Field(const u16string &data, const Constants::column_index_t &columnIndex)
{
    this->size = data.size();
    this->data = new object_t[this->size];
    memcpy(this->data, data.data(), this->size);
    
    this->columnIndex = columnIndex;
    this->type = ColumnType::UnicodeString;
}

Field::~Field() = default;

bool Field::GetIsNull() const { return this->data == nullptr; }

const Constants::column_index_t & Field::GetColumnIndex() const { return this->columnIndex;}

void Field::SetData(const bool &data){
    delete this->data;
    
    this->data = new object_t[sizeof(bool)];
    memcpy(this->data, &data, sizeof(bool));
    this->size = sizeof(bool);

    this->type = ColumnType::Bool;
}

void Field::SetData(const string &data) {
    delete this->data;

    this->size = data.size();
    this->data = new object_t[this->size];
    memcpy(this->data, data.data(), this->size);

    this->type = ColumnType::String;
}
void Field::SetData(const u16string &data) {
    delete this->data;
    
    this->size = data.size();
    this->data = new object_t[this->size];
    memcpy(this->data, data.data(), this->size);

    this->type = ColumnType::UnicodeString;
}
void Field::SetData(const int8_t &data) { 
    delete this->data;
    
    this->data = new object_t[sizeof(int8_t)];
    memcpy(this->data, &data, sizeof(int8_t));
    this->size = sizeof(int8_t);

    this->type = ColumnType::TinyInt;
}

void Field::SetData(const int16_t &data) { 
    delete this->data;
    
    this->data = new object_t[sizeof(int16_t)];
    memcpy(this->data, &data, sizeof(int16_t));
    this->size = sizeof(int16_t);

    this->type = ColumnType::SmallInt;
}
void Field::SetData(const int32_t &data) { 
    delete this->data;
    
    this->data = new object_t[sizeof(int32_t)];
    memcpy(this->data, &data, sizeof(int32_t));
    this->size = sizeof(int32_t);

    this->type = ColumnType::Int;
}
void Field::SetData(const int64_t &data) { 
    delete this->data;
    
    this->data = new object_t[sizeof(int64_t)];
    memcpy(this->data, &data, sizeof(int64_t));
    this->size = sizeof(int64_t);

    this->type = ColumnType::BigInt;
}
void Field::SetData(const DataTypes::DateTime &data) { 
    delete this->data;
    
    this->data = new object_t[DataTypes::DateTime::DateTimeSize()];
    memcpy(this->data, &data.GetUnixTimeStamp(), DataTypes::DateTime::DateTimeSize());
    
    this->size = DataTypes::DateTime::DateTimeSize();

    this->type = ColumnType::DateTime;
}

void Field::SetData(const DataTypes::Guid &data){
    delete this->data;

    this->size = data.Size();
    this->data = new object_t[this->size];
    memcpy(this->data, data.GetData().data(), this->size);

    this->type = ColumnType::Guid;
}

void Field::SetName(std::string &data){
    this->name = std::move(data);
}

bool Field::TryParseAsBool(bool& result)const{
    if (this->type == ColumnType::String || this->type == ColumnType::UnicodeString) {
        return this->TryParseAsBoolFromString(result);
    }

    if (this->type == ColumnType::BigInt
        || this->type == ColumnType::TinyInt
        || this->type == ColumnType::SmallInt
        || this->type == ColumnType::Int) {
        return this->TryParseAsBoolFromInt(result);
    }

    return false;

}

bool Field::TryParseAsBoolFromString(bool& result)const{
    const auto strData = AdditionalLibraries::Lower(this->GetString());

    if (strData == "true" || strData == "1")
        return true;

    if (strData == "false" || strData == "0")
        return true;

    return false;
}

bool Field::ParseAsBoolFromString() const{
    const auto strData = AdditionalLibraries::Lower(this->GetString());

    if (strData == "true" || strData == "1")
        return true;

    if (strData == "false" || strData == "0")
        return false;

    return false;
}

bool Field::TryParseAsBoolFromInt(bool& result)const{
    const auto intData = this->GetBigInt();

    if (intData == 1)
        return true;

    if (intData == 0)
        return true;

    return false;
}

bool Field::TryParseDate(){
    const auto strData = AdditionalLibraries::Lower(this->GetString());

    DataTypes::DateTime parsedDate;

    auto result = DataTypes::DateTime::FromString(parsedDate, strData);

    if (!result)
        return false;

    this->SetData(parsedDate);
    return true;
}

void Field::InferType(){

    switch (this->type){
        case ColumnType::BigInt:

            break;
        case ColumnType::Decimal:
            break;
        case ColumnType::String:
            if (this->TryParseDate())
                return;

            break;
        case ColumnType::UnicodeString:
            break;
        case ColumnType::Bool:
            break;
        default:
            break;
    }
}

void Field::SetData(const DataTypes::Decimal &data) { 
    delete this->data;
    
    this->size = data.GetRawDataSize();
    this->data = new object_t[this->size];
    memcpy(this->data, data.GetRawData(), this->size);

    this->type = ColumnType::Decimal;
}

const object_t * Field::GetRawData() const{ return this->data; }

bool Field::GetBool() const {
    switch (this->type) {
        case ColumnType::TinyInt:
            return SafeConverter<bool>::SafeStoi(this->GetTinyInt());
        case ColumnType::SmallInt:
            return SafeConverter<bool>::SafeStoi(this->GetSmallInt());
        case ColumnType::Int:
            return SafeConverter<bool>::SafeStoi(this->GetInt());
        case ColumnType::BigInt:
            return SafeConverter<bool>::SafeStoi(this->GetBigInt());
        case ColumnType::Decimal:
            return false;
        case ColumnType::String:
            return this->ParseAsBoolFromString();
        case ColumnType::UnicodeString:
            return SafeConverter<int8_t>::SafeStoi(this->GetUnicodeString());
        case ColumnType::Bool:
            return *reinterpret_cast<bool*>(this->data);
        default:
            throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(this->type) + " cannot be coerced to Bool");
    }
}

int8_t Field::GetTinyInt() const {
    switch (this->type) {
        case ColumnType::TinyInt:
            return *reinterpret_cast<int8_t *>(this->data);
        case ColumnType::SmallInt:
            return SafeConverter<int8_t>::SafeStoi(this->GetSmallInt());
        case ColumnType::Int:
            return SafeConverter<int8_t>::SafeStoi(this->GetInt());
        case ColumnType::BigInt:
            return SafeConverter<int8_t>::SafeStoi(this->GetBigInt());
        case ColumnType::Decimal:
            return 0;
        case ColumnType::String:
            return SafeConverter<int8_t>::SafeStoi(this->GetString());
        case ColumnType::UnicodeString:
            return SafeConverter<int8_t>::SafeStoi(this->GetUnicodeString());
        case ColumnType::Bool:
            return this->GetBool() ? 1 : 0;
        default:
            throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(this->type) + " cannot be coerced to Tiny Int");
    }
}

int16_t Field::GetSmallInt() const {
    switch (this->type) {
        case ColumnType::TinyInt:
        case ColumnType::SmallInt:
            return *reinterpret_cast<int16_t *>(this->data);
        case ColumnType::Int:
            return SafeConverter<int16_t>::SafeStoi(this->GetInt());
        case ColumnType::BigInt:
            return SafeConverter<int16_t>::SafeStoi(this->GetBigInt());
        case ColumnType::Decimal:
            return 0;
        case ColumnType::String:
            return SafeConverter<int16_t>::SafeStoi(this->GetString());
        case ColumnType::UnicodeString:
            return SafeConverter<int16_t>::SafeStoi(this->GetUnicodeString());
        case ColumnType::Bool:
            return this->GetBool() ? 1 : 0;
        default:
            throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(this->type) + " cannot be coerced to Small Int");
    }
}

int32_t Field::GetInt() const {
    switch (this->type) {
        case ColumnType::TinyInt:
        case ColumnType::SmallInt:
        case ColumnType::Int:
            return *reinterpret_cast<int32_t *>(this->data);
        case ColumnType::BigInt:
            return SafeConverter<int32_t>::SafeStoi(this->GetBigInt());
        case ColumnType::Decimal:
            return 0;
        case ColumnType::String:
            return SafeConverter<int32_t>::SafeStoi(this->GetString());
        case ColumnType::UnicodeString:
            return SafeConverter<int32_t>::SafeStoi(this->GetUnicodeString());
        case ColumnType::Bool:
            return this->GetBool() ? 1 : 0;
        default:
            throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(this->type) + " cannot be coerced to Int");
    }
}

int64_t Field::GetBigInt() const {
    switch (this->type) {
        case ColumnType::TinyInt:
        case ColumnType::SmallInt:
        case ColumnType::Int:
        case ColumnType::BigInt:
           return *reinterpret_cast<int64_t *>(this->data);
        case ColumnType::Decimal:
            return 0;
        case ColumnType::String:
            return SafeConverter<int64_t>::SafeStoi(this->GetString());
        case ColumnType::UnicodeString:
            return SafeConverter<int64_t>::SafeStoi(this->GetUnicodeString());
        case ColumnType::Bool:
            return this->GetBool() ? 1 : 0;
        default:
            throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(this->type) + " cannot be coerced to Big Int");
    }
}

string Field::GetString() const { return {reinterpret_cast<char*>(this->data), this->size}; }

u16string Field::GetUnicodeString() const { return {reinterpret_cast<char16_t *>(this->data), this->size}; }

DataTypes::Decimal Field::GetDecimal() const{ return DataTypes::Decimal(this->data, this->size); }

DataTypes::DateTime Field::GetDateTime() const {
    switch (this->type) {
        case ColumnType::UnicodeString:
        case ColumnType::String: {
            DataTypes::DateTime date;
            DataTypes::DateTime::FromString(date, this->GetString());
            return date;
        }
        case ColumnType::DateTime:
            return DataTypes::DateTime(*reinterpret_cast<time_t *>(this->data));
        default:
            throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(this->type) + " cannot be coerced to Guid");
    }
}

time_t Field::GetUnixTimeStamp() const{ return *reinterpret_cast<time_t *>(this->data); }

DataTypes::Guid Field::GetGuid() const {
    switch (this->type) {
        case ColumnType::Guid:
            return {this->data, this->size};
        case ColumnType::String:
        case ColumnType::UnicodeString:
            return DataTypes::Guid::FromString(this->GetString());
        default:
            throw std::invalid_argument("Field type " +  ColumnTypesToStringDictionary.Get(this->type) + " cannot be coerced to Guid");
    }
}

void Field::SetColumnIndex(const Constants::column_index_t &columnIndex) { this->columnIndex = columnIndex; }

void Field::SetType(const ColumnType &type){ this->type = type; }

const ColumnType & Field::GetType() const{ return this->type; }

const block_size_t& Field::GetSize() const{ return this->size; }

void Field::Validate(const Headers::ColumnHeader &header){
    const auto columnType = static_cast<ColumnType>(header.dataType);

    if (this->GetIsNull()) {

        this->SetType(columnType);
        this->SetColumnIndex(header.ordinalPosition);

        return;
    }

    switch (columnType) {
      case ColumnType::TinyInt: {
          const auto value = SafeConverter<int8_t>::SafeStoi(this->GetBigInt());
          this->SetData(value);
          break;
      }
      case ColumnType::SmallInt: {
          const auto value = SafeConverter<int16_t>::SafeStoi(this->GetBigInt());
          this->SetData(value);
          break;
      }
      case ColumnType::Int:{
          const auto value = SafeConverter<int32_t>::SafeStoi(this->GetBigInt());
          this->SetData(value);
          break;
      }
      case ColumnType::BigInt:
          SafeConverter<int64_t>::SafeStoi(this->GetBigInt());
          break;
      case ColumnType::String:
      case ColumnType::UnicodeString:
          if (columnType != this->GetType())
              throw runtime_error("Column " + header.name + " has different data type than specified");
          break;
      case ColumnType::Bool: {
          bool value;
          if (this->TryParseAsBool(value) && this->IsVariable())
              break;

          this->SetData(value);
          this->SetType(columnType);
          break;
      }
      case ColumnType::DateTime: {
          const auto datetime = this->GetDateTime();
          if (!DataTypes::DateTime::ValidateDate(datetime))
              throw invalid_argument("failed to validate date");
          break;
      }
      case ColumnType::Decimal:

          break;
    case ColumnType::Guid:
        break;
    default:
    case ColumnType::ColumnTypeCount:
        throw runtime_error(
                "Type mismatch: expected " + Constants::ColumnTypesToStringDictionary.Get(columnType) +
                  " but got " + Constants::ColumnTypesToStringDictionary.Get(this->type));
    }

    this->SetColumnIndex(header.ordinalPosition);
}

void Field::Validate(const ColumnType &columnType, const int &ordinalPosition){
    if (this->GetIsNull()) {

        this->SetType(columnType);
        this->SetColumnIndex(ordinalPosition);

        return;
    }

    switch (columnType) {
    case ColumnType::TinyInt: {
        const auto value = SafeConverter<int8_t>::SafeStoi(this->GetBigInt());
        this->SetData(value);
        break;
    }
    case ColumnType::SmallInt: {
        const auto value = SafeConverter<int16_t>::SafeStoi(this->GetBigInt());
        this->SetData(value);
        break;
    }
    case ColumnType::Int:{
        const auto value = SafeConverter<int32_t>::SafeStoi(this->GetBigInt());
        this->SetData(value);
        break;
    }
    case ColumnType::BigInt:
        SafeConverter<int64_t>::SafeStoi(this->GetBigInt());
        break;
    case ColumnType::String:
    case ColumnType::UnicodeString:
        // if (columnType != this->GetType())
        //     throw runtime_error("Column " + header.name + " has different data type than specified");
        break;
    case ColumnType::Bool: {
        bool value;
        if (this->TryParseAsBool(value) && this->IsVariable())
            break;

        this->SetData(value);
        this->SetType(columnType);
        break;
    }
    case ColumnType::DateTime: {
        const auto datetime = this->GetDateTime();
        if (!DataTypes::DateTime::ValidateDate(datetime))
            throw invalid_argument("failed to validate date");
        break;
    }
    case ColumnType::Decimal:

        break;
    case ColumnType::Guid:
        break;
    default:
    case ColumnType::ColumnTypeCount:
        throw invalid_argument("Invalid column type");
    }

    this->SetColumnIndex(ordinalPosition);
}

bool Field::IsVariable()const{ return !this->name.empty(); }

ostream & operator<<(ostream& os, const Field &field){

    if (!field.name.empty())
        os << field.name << ": ";

    if (field.GetIsNull()) {
        os << "NULL";
        return os;
    }

    switch (field.type){
        case ColumnType::TinyInt:
            os << field.GetTinyInt();
            break;
        case ColumnType::SmallInt:
            os << field.GetSmallInt();
            break;
        case ColumnType::Int:
            os << field.GetInt();
            break;
        case ColumnType::BigInt:
            os << field.GetBigInt();
            break;
        case ColumnType::Decimal:
            os << field.GetDecimal();
            break;
        case ColumnType::String:
            os << field.GetString();
            break;
        case ColumnType::UnicodeString:
            //TODO
            os << field.GetString();
            break;
        case ColumnType::Bool:
            os << field.GetBool();
            break;
        case ColumnType::DateTime:
            os << field.GetDateTime();
            break;
        case ColumnType::Guid:
            os << field.GetGuid();
            break;
        default:
            break;
    }

    return os;
}