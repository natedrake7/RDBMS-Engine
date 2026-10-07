#pragma once
#include <vector>
#include <Systemic/DataTypes/SortCondition.h>
#include <Systemic/DataStructures/PolymorphicArray.h>

namespace CoreEngine{
    class ExecutionContext;
}

namespace QueryPipeline::Statements {
  struct OrderColumn;
}

namespace Expressions {
  class Expression;
}

class MaterializedRow;

namespace CoreEngine::StorageTypes {
    class Row;
}

struct MergeSortParameters{
    const CoreEngine::ExecutionContext* properties;
    DataStructures::PolymorphicArray<MaterializedRow>* rows;
    const DataStructures::PolymorphicArray<QueryPipeline::Statements::OrderColumn*>* sortConditions;
    Int left;
    Int right;
    Int mid;
};

class MergeSort {
        static void Merge(const MergeSortParameters& parameters);
    public:
        static void Sort(MergeSortParameters& parameters);
};
