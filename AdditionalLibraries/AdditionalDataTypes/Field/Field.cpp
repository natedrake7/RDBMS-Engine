#include "Field.h"

#include "../../SafeConverter/SafeConverter.h"

#include <cstring>
#include <stdexcept>

using namespace Constants;

Field::Field()
{
    this->data = nullptr;
    this->columnIndex = 0;
}

Field::Field(const void *data, const Constants::column_index_t &columnIndex){
    this->data = nullptr;
    this->columnIndex = columnIndex;
    this->type = ColumnType::ColumnTypeCount;
}

Field::Field(const bool &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(bool)];
    memcpy(this->data, &data, sizeof(bool));
    
    this->size = sizeof(bool);
    this->columnIndex = columnIndex;
    this->type = ColumnType::Bool;
}

Field::Field(const int8_t &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(int8_t)];
    memcpy(this->data, &data, sizeof(int8_t));
    
    this->size = sizeof(int8_t);
    this->columnIndex = columnIndex;
    this->type = ColumnType::TinyInt;
}

Field::Field(const int16_t &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(int16_t)];
    memcpy(this->data, &data, sizeof(int16_t));
    
    this->size = sizeof(int16_t);
    this->columnIndex = columnIndex;
    this->type = ColumnType::SmallInt;
}

Field::Field(const int32_t &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(int32_t)];
    memcpy(this->data, &data, sizeof(int32_t));
    
    this->size = sizeof(int32_t);
    this->columnIndex = columnIndex;
    this->type = ColumnType::Int;
}

Field::Field(const int64_t &data, const column_index_t &columnIndex){
    this->data = new object_t[sizeof(int64_t)];
    memcpy(this->data, &data, sizeof(int64_t));
    
    this->size = sizeof(int64_t);
    this->columnIndex = columnIndex;
    this->type = ColumnType::BigInt;
}

Field::Field(const DataTypes::DateTime &data, const column_index_t &columnIndex){
    this->data = new object_t[DataTypes::DateTime::DateTimeSize()];
    memcpy(this->data, &data.GetUnixTimeStamp(), DataTypes::DateTime::DateTimeSize());
    
    this->size = DataTypes::DateTime::DateTimeSize();
    this->columnIndex = columnIndex;
    this->type = ColumnType::DateTime;
}

Field::Field(const DataTypes::Decimal &data, const column_index_t &columnIndex){
    this->size = data.GetRawDataSize();
    this->data = new object_t[this->size];

    memcpy(this->data, data.GetRawData(), this->size);
    this->columnIndex = columnIndex;
    this->type = ColumnType::Decimal;
}

Field::Field(const DataTypes::Guid &data, const column_index_t &columnIndex){
    this->size = data.Size();
    this->data = new object_t[this->size];
    memcpy(this->data, data.GetData().data(), this->size);

    this->columnIndex = columnIndex;
    this->type = ColumnType::Guid;
}

Field::Field(const string &data, const Constants::column_index_t& columnIndex)
{
    this->size = data.size();
    this->data = new object_t[this->size];
    memcpy(this->data, data.data(), this->size);
    
    this->columnIndex = columnIndex;
    this->type = ColumnType::String;
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

void Field::SetData(const DataTypes::Decimal &data) { 
    delete this->data;
    
    this->size = data.GetRawDataSize();
    this->data = new object_t[this->size];
    memcpy(this->data, data.GetRawData(), this->size);

    this->type = ColumnType::Decimal;
}

const object_t * Field::GetRawData() const{ return this->data; }

bool Field::GetBool() const { return *reinterpret_cast<bool*>(this->data); }

int8_t Field::GetTinyInt() const { return *reinterpret_cast<int8_t *>(this->data); }

int16_t Field::GetSmallInt() const { return *reinterpret_cast<int16_t *>(this->data); }

int32_t Field::GetInt() const { return *reinterpret_cast<int32_t *>(this->data); }

int64_t Field::GetBigInt() const { return *reinterpret_cast<int64_t *>(this->data); }

string Field::GetString() const { return { reinterpret_cast<char *>(this->data)}; }

u16string Field::GetUnicodeString() const { return {reinterpret_cast<char16_t *>(this->data)}; }

DataTypes::Decimal Field::GetDecimal() const{ return DataTypes::Decimal(this->data, this->size); }

DataTypes::DateTime Field::GetDateTime() const{ return DataTypes::DateTime(*reinterpret_cast<time_t *>(this->data));}

time_t Field::GetUnixTimeStamp() const{ return *reinterpret_cast<time_t *>(this->data); }

DataTypes::Guid Field::GetGuid() const{ return {this->data, this->size}; }

void Field::SetColumnIndex(const Constants::column_index_t &columnIndex) { this->columnIndex = columnIndex; }

const ColumnType & Field::GetType() const{ return this->type; }

const block_size_t& Field::GetSize() const{ return this->size; }

void Field::Validate(const Headers::ColumnHeader &header){
    const auto columnType = static_cast<ColumnType>(header.dataType);

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
          const auto value = SafeConverter<bool>::SafeStoi(this->GetBigInt());
          this->SetData(value);
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

    this->SetColumnIndex(header.ordinalPosition);
}