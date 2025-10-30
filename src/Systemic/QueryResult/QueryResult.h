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
  [[nodiscard]] int64_t ComputeHash()const;

  void Serialize(std::vector<char>& buffer)const;
  void Deserialize(const std::vector<char>& buffer, uint32_t& offset, const int& dataSize);

  friend bool operator==(const QueryResult& lhs, const QueryResult& rhs);
};
