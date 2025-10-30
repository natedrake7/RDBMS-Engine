#include "QueryResult.h"

#include "../Network/Protocols/ConnectionProtocol/QueryProtocol/QueryResponseProtocol.h"

#include <iostream>

void QueryResult::AddColumn(Value &field){
  this->data.push_back(std::move(field));
}

void QueryResult::Print() const{
  for (int i = 0; i < this->data.size(); ++i) {
    const auto& column = this->data[i];

    if(column.GetRawData() == nullptr
      || column.GetSize() == 0)
    {
      std::cout   << "NULL"
                  << ((i == this->data.size() - 1) ? "\n" : " || ");

      continue;
    }

      switch (column.GetType()){
        case DataType::TinyInt:
          std::cout << column.GetTinyInt();
          break;
        case DataType::SmallInt:
          std::cout << column.GetSmallInt();
          break;
        case DataType::Int:
          std::cout << column.GetInt();
          break;
        case DataType::BigInt:
          std::cout << column.GetBigInt();
          break;
        case DataType::Decimal:
          std::cout << column.GetDecimal();
          break;
        case DataType::String:
          std::cout << column.GetString();
          break;
        case DataType::UnicodeString:
          //TODO
          std::cout << column.GetString();
          break;
        case DataType::Bool:
          std::cout << (column.GetBool() ? "TRUE" : "FALSE");
          break;
        case DataType::DateTime:
          std::cout << column.GetDateTime();
          break;
        case DataType::Guid:
          std::cout << column.GetGuid();
          break;
        default:
          break;
      }

      std::cout << ((i == this->data.size() - 1) ? "\n" : " || ");
  }
}

const std::vector<Value> & QueryResult::GetData()const{ return this->data; }

int QueryResult::GetSize() const{ return this->data.size(); }

void QueryResult::SetColumnIndex(const int &columnPos, const int32_t &columnIndex){
  if (columnPos >= this->data.size())
    return;

  this->data.at(columnPos).SetColumnIndex(columnIndex);
}

int64_t QueryResult::ComputeHash() const{
  std::hash<std::string> strHash;
  size_t seed = 0;

  for (const auto& value : this->data) {
    auto str = value.GetString(); // or serialize to bytes

    seed ^= strHash(str) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  }

  return static_cast<int64_t>(seed);
}

void QueryResult::Serialize(std::vector<char>& buffer) const{
  for (const auto& value : this->data) {
    const auto size = value.GetSize();
    const auto type = value.GetType();

    Vector::AppendToBuffer(buffer, &size, sizeof(Constants::block_size_t));
    Vector::AppendToBuffer(buffer, &type, sizeof(Constants::DataType));
    Vector::AppendToBuffer(buffer, value.GetRawData(), size);
  }
}

void QueryResult::Deserialize(const std::vector<char> &buffer, uint32_t &offset, const int& dataSize){
  this->data.reserve(dataSize);

  for (int i = 0;i < dataSize; i++) {
      auto value = Value();

      value.Deserialize(buffer, offset);

      this->data.push_back(std::move(value));
  }
}

bool operator==(const QueryResult& lhs, const QueryResult& rhs) {
  if (lhs.GetSize() != rhs.GetSize())
    return false;

  for (int i = 0;i < lhs.data.size(); i++) {
    if ((lhs.data[i] == rhs.data[i]).GetBool() == false)
      return false;
  }

  return true;
}
