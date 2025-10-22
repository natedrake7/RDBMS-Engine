#pragma once
#include "../../Systemic/DataTypes/Value/Value.h"


#include <vector>

class QueryResult {
  std::vector<Value> data;

public:
  QueryResult() = default;
  ~QueryResult() = default;

  void AddColumn(Value& field);
  void Print()const;
  [[nodiscard]] const std::vector<Value>& GetData()const;
  [[nodiscard]] int GetSize()const;
  void SetColumnIndex(const int& columnPos, const int32_t & columnIndex);
  int64_t ComputeHash()const;

  friend bool operator==(const QueryResult& lhs, const QueryResult& rhs);
};
