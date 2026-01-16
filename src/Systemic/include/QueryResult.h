#pragma once
#include "DataTypes/Value.h"


#include <vector>

class QueryResult {
  std::vector<Value> data;

public:
  QueryResult() = default;
  ~QueryResult() = default;

  void AddColumn(Value& field);
  void Print()const;
  [[nodiscard]] const std::vector<Value>& GetData()const;
  [[nodiscard]] Value GetColumnAt(const int& columnPos)const;
  [[nodiscard]] int GetSize()const;
  [[nodiscard]] Int GetByteSize()const;
  void SetColumnIndex(const int& columnPos, const int32_t & columnIndex);
  [[nodiscard]] int64_t ComputeHash()const;

  void Serialize(std::vector<char>& buffer)const;
  void Deserialize(const std::vector<char>& buffer, uint32_t& offset, const int& dataSize);

  QueryResult(const QueryResult& other);
  QueryResult(QueryResult&& other) noexcept;
  QueryResult& operator=(const QueryResult& other);
  QueryResult& operator=(QueryResult&& other) noexcept;

  friend bool operator==(const QueryResult& lhs, const QueryResult& rhs);
  friend ostream& operator<<(std::ostream& os, const QueryResult& result);
};
