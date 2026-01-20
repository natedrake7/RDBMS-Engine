#pragma once
#include "DataTypes/Value.h"


#include <vector>

class QueryResult {
  std::vector<Value> data;

public:
  QueryResult();
  QueryResult(const QueryResult& other);
  QueryResult(QueryResult&& other) noexcept;

  void AddColumn(Value& field);
  void Print()const;
  [[nodiscard]] const std::vector<Value>& GetData()const;
  [[nodiscard]] Value GetColumnAt(const int& columnPos)const;
  [[nodiscard]] Int GetSize()const;
  [[nodiscard]] Int GetByteSize()const;
  void SetColumnIndex(Int columnPos, column_index_t columnIndex);
  [[nodiscard]] BigInt ComputeHash()const;

  void Serialize(std::vector<char>& buffer)const;
  void Deserialize(const std::vector<char>& buffer, UnsignedInt& offset, Int dataSize);

  QueryResult& operator=(const QueryResult& other);
  QueryResult& operator=(QueryResult&& other) noexcept;

  friend bool operator==(const QueryResult& lhs, const QueryResult& rhs);
  friend std::ostream& operator<<(std::ostream& os, const QueryResult& result);
};
