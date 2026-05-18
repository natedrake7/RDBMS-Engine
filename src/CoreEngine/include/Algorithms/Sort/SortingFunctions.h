#pragma once
#include <unordered_map>
#include <vector>

#include "../../../../Systemic/include/RowIdentifier.h"
#include "../../../../Systemic/include/QueryResult.h"
#include "../../../include/DataStorage/Column.h"
#include "../../../include/DataStorage/Row.h"
#include "../../../../Systemic/include/DataStructures/PolymorphicArray.h"

namespace CoreEngine{
    class ExecutionContext;
    struct ScanState;
}

class GroupCondition;

namespace QueryPipeline::Statements {
  struct OrderColumn;
}


typedef struct AggregateResults {
    uint64_t count;
    long double average;
    long double sum;
    long double min;
    long double max;
    AggregateResults();
} AggregateResults;

struct MergeElement{
    QueryResult value;
    DataTypes::RowIdentifier rowId;
    Int batchId;

    MergeElement(const QueryResult& value, Int batchId);
    MergeElement(QueryResult& value, Int batchId, const DataTypes::RowIdentifier& rowId);
    MergeElement(const MergeElement& other);
    MergeElement(MergeElement&& other) noexcept;
    MergeElement& operator=(const MergeElement& other);
    MergeElement& operator=(MergeElement&& other) noexcept;
};

class SortingFunctions{
         [[nodiscard]] static std::string CreateGroupByKey(
           const CoreEngine::StorageTypes::RID& row,
           const std::vector<GroupCondition> &sortConditions
          );
         static long double ApplyAggregateFunctionToGroup(
           const std::vector<CoreEngine::StorageTypes::RID>& rowGroup,
           const GroupCondition& condition
          );

    public:
         [[nodiscard]] static bool CompareRows(
            const CoreEngine::ExecutionContext& context,
            const QueryResult& firstRow,
            const QueryResult& secondRow,
            const DataStructures::PolymorphicArray<QueryPipeline::Statements::OrderColumn*>& sortConditions
          );
         [[nodiscard]] static bool CompareRowsAscending(
            const CoreEngine::ExecutionContext& context,
            const CoreEngine::StorageTypes::RID& firstRow,
            const CoreEngine::StorageTypes::RID& secondRow,
            const column_index_t& columnIndex
          );
         [[nodiscard]] static bool CompareRowsDescending(
            const CoreEngine::ExecutionContext& context,
            const CoreEngine::StorageTypes::RID& firstRow,
            const CoreEngine::StorageTypes::RID& secondRow,
            const column_index_t& columnIndex
          );
         static void OrderBy(
            const CoreEngine::ExecutionContext& context,
            DataStructures::PolymorphicArray<QueryResult>& rows,
            const DataStructures::PolymorphicArray<QueryPipeline::Statements::OrderColumn*>& conditions
          );
         [[nodiscard]] static std::unordered_map<std::string, AggregateResults> GroupBy(
           const std::vector<CoreEngine::StorageTypes::RID>& rows,
           const std::vector<GroupCondition>& sortConditions
          );
};

class MergeComparator final{
        const DataStructures::PolymorphicArray<QueryPipeline::Statements::OrderColumn*>* sortConditions;
        const CoreEngine::ExecutionContext* context;

    public:
        explicit MergeComparator(
          const DataStructures::PolymorphicArray<QueryPipeline::Statements::OrderColumn*>* sortConditions,
          const CoreEngine::ExecutionContext* context
        );
        bool operator()(
          const MergeElement& first,
          const MergeElement& second
        ) const;
        void SetExecutionContext(const CoreEngine::ExecutionContext* otherContext);
        [[nodiscard]] bool HasProperties() const;
};