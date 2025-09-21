#include "QueryResult.h"
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

void QueryResult::SetColumnIndex(const int &columnPos, const int32_t &columnIndex){
  if (columnPos >= this->data.size())
    return;

  this->data.at(columnPos).SetColumnIndex(columnIndex);
}
