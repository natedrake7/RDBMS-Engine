#pragma once
#include <vector>
#include "../../../../Systemic/include/DataTypes/SortCondition.h"
#include "DataStructures/PolymorphicArray.h"

namespace DatabaseEngine
{
    struct ExecutionProperties;
}

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

struct MergeSortParameters{
    const DatabaseEngine::ExecutionProperties* properties;
    DataStructures::PolymorphicArray<QueryResult>* rows;
    const std::vector<QueryPipeline::Statements::OrderColumn*>* sortConditions;
    Int left;
    Int right;
    Int mid;
};

class MergeSort {
        static void Merge(const MergeSortParameters& parameters);
    public:
        static void Sort(MergeSortParameters& parameters);
};
