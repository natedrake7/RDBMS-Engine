#pragma once
#include <unordered_map>
#include <vector>

#include "../../../../Systemic/include/RowIdentifier.h"
#include "../../../../Systemic/include/QueryResult.h"
#include "../../../include/DataStorage/Column.h"
#include "../../../../Systemic/include/DataStructures/PolymorphicArray.h"

namespace DatabaseEngine{
    struct ExecutionProperties;
}

namespace Pages{
    struct RowReference;
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
           const Pages::RowReference& row,
           const std::vector<GroupCondition> &sortConditions
          );
         static long double ApplyAggregateFunctionToGroup(
           const std::vector<Pages::RowReference>& rowGroup,
           const GroupCondition& condition
          );

    public:
         [[nodiscard]] static bool CompareRows(
            const DatabaseEngine::ExecutionProperties& properties,
            const QueryResult& firstRow,
            const QueryResult& secondRow,
            const std::vector<QueryPipeline::Statements::OrderColumn*>& sortConditions
          );
         [[nodiscard]] static bool CompareRowsAscending(
            const DatabaseEngine::ExecutionProperties& properties,
            const Pages::RowReference& firstRow,
            const Pages::RowReference& secondRow,
            const column_index_t& columnIndex
          );
         [[nodiscard]] static bool CompareRowsDescending(
            const DatabaseEngine::ExecutionProperties& properties,
            const Pages::RowReference& firstRow,
            const Pages::RowReference& secondRow,
            const column_index_t& columnIndex
          );
         static void OrderBy(
            const DatabaseEngine::ExecutionProperties& properties,
            DataStructures::PolymorphicArray<QueryResult>& rows,
            const std::vector<QueryPipeline::Statements::OrderColumn*>& conditions
          );
         [[nodiscard]] static std::unordered_map<std::string, AggregateResults> GroupBy(
           const std::vector<Pages::RowReference>& rows,
           const std::vector<GroupCondition>& sortConditions
          );
};

class MergeComparator final{
        const std::vector<QueryPipeline::Statements::OrderColumn*>* sortConditions;
        const DatabaseEngine::ExecutionProperties* properties;

    public:
        explicit MergeComparator(
          const std::vector<QueryPipeline::Statements::OrderColumn*>* sortConditions,
          const DatabaseEngine::ExecutionProperties* properties
        );
        bool operator()(
          const MergeElement& first,
          const MergeElement& second
        ) const;
        void SetProperties(const DatabaseEngine::ExecutionProperties* properties);
        bool HasProperties() const;
};