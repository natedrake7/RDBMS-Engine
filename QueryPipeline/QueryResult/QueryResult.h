#pragma once
#include "../../AdditionalLibraries/AdditionalDataTypes/Field/Field.h"


#include <vector>

class QueryResult {
  std::vector<Field> data;

public:
  QueryResult() = default;
  ~QueryResult() = default;

  void AddColumn(Field& field);
  void Print()const;
};
