#include "QueryResult.h"
#include <iostream>

void QueryResult::AddColumn(Field &field){
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
        case ColumnType::TinyInt:
          std::cout << column.GetTinyInt();
          break;
        case ColumnType::SmallInt:
          std::cout << column.GetSmallInt();
          break;
        case ColumnType::Int:
          std::cout << column.GetInt();
          break;
        case ColumnType::BigInt:
          std::cout << column.GetBigInt();
          break;
        case ColumnType::Decimal:
          std::cout << column.GetDecimal();
          break;
        case ColumnType::String:
          std::cout << column.GetString();
          break;
        case ColumnType::UnicodeString:
          //TODO
          std::cout << column.GetString();
          break;
        case ColumnType::Bool:
          std::cout << (column.GetBool() ? "TRUE" : "FALSE");
          break;
        case ColumnType::DateTime:
          std::cout << column.GetDateTime();
          break;
        case ColumnType::Guid:
          std::cout << column.GetGuid();
          break;
        default:
          break;
      }

      std::cout << ((i == this->data.size() - 1) ? "\n" : " || ");
  }
}
