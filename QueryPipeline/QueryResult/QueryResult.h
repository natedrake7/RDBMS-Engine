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
};
