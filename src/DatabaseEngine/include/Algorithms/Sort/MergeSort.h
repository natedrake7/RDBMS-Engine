#pragma once
#include <vector>
#include "../../../../Systemic/include/DataTypes/SortCondition.h"

namespace QueryPipeline::Statements {
  struct OrderColumn;
}

namespace Expressions {
  class Expression;
}

class QueryResult;

namespace DatabaseEngine::StorageTypes {
    class Row;
}

class MergeSort {
        static void Merge(std::vector<QueryResult>& rows, const int& left, const int& mid, const int& right, const std::vector<QueryPipeline::Statements::OrderColumn*>& sortConditions);
    public:
        static void Sort(std::vector<QueryResult>& rows, const int& left, const int& right, const std::vector<QueryPipeline::Statements::OrderColumn*>& sortConditions);
};
