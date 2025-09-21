#pragma once
#include "../../AdditionalLibraries/DataTypes/Value/Value.h"


#include <vector>

class QueryResult {
  std::vector<Value> data;

public:
  QueryResult() = default;
  ~QueryResult() = default;

  void AddColumn(Value& field);
  void Print()const;
  [[nodiscard]] const std::vector<Value>& GetData()const;
  void SetColumnIndex(const int& columnPos, const int32_t & columnIndex);
};
