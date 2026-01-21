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
        static void Merge(std::vector<QueryResult>& rows, Int left, Int mid, Int right, const std::vector<QueryPipeline::Statements::OrderColumn*>& sortConditions);
    public:
        static void Sort(std::vector<QueryResult>& rows, Int left, Int right, const std::vector<QueryPipeline::Statements::OrderColumn*>& sortConditions);
};
