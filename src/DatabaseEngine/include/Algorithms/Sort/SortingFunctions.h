#pragma once
#include <unordered_map>
#include <vector>
#include "../../../../Systemic/include/QueryResult.h"
#include "../../../include/DataStorage/Column.h"

namespace QueryPipeline::Statements {
  struct OrderColumn;
}

class GroupCondition;

namespace DatabaseEngine::StorageTypes {
    class Block;
    class Row;
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
    Headers::RowIdentifier rowId;
    Int batchId;

    MergeElement(const QueryResult& value, const Int& batchId);
    MergeElement(const MergeElement& other);
    MergeElement(MergeElement&& other);
    MergeElement& operator=(const MergeElement& other);
    MergeElement& operator=(MergeElement&& other);
};

class SortingFunctions{
         [[nodiscard]] static int CompareBlockByDataType(
           const DatabaseEngine::StorageTypes::Block*& firstBlock,
           const DatabaseEngine::StorageTypes::Block*& secondBlock
          );
         [[nodiscard]] static string CreateGroupByKey(
           const DatabaseEngine::StorageTypes::Row* row,
           const std::vector<GroupCondition> &sortConditions
          );
         static long double ApplyAggregateFunctionToGroup(
           const std::vector<DatabaseEngine::StorageTypes::Row*>& rowGroup,
           const GroupCondition& condition
          );

    public:
         [[nodiscard]] static bool CompareRows(
           const QueryResult& firstRow,
           const QueryResult& secondRow,
           const vector<QueryPipeline::Statements::OrderColumn*>& sortConditions
          );
         [[nodiscard]] static bool CompareRowsAscending(
           const DatabaseEngine::StorageTypes::Row* firstRow,
           const DatabaseEngine::StorageTypes::Row* secondRow,
           const column_index_t& columnIndex
          );
         [[nodiscard]] static bool CompareRowsDescending(
           const DatabaseEngine::StorageTypes::Row* firstRow,
           const DatabaseEngine::StorageTypes::Row* secondRow,
           const column_index_t& columnIndex
          );
         static void OrderBy(
           std::vector<QueryResult>& rows,
           const std::vector<QueryPipeline::Statements::OrderColumn*>& conditions
          );
         [[nodiscard]] static std::unordered_map<std::string, AggregateResults> GroupBy(
           const std::vector<DatabaseEngine::StorageTypes::Row*>& rows,
           const std::vector<GroupCondition>& sortConditions
          );
};

class MergeComparator final{
    const std::vector<QueryPipeline::Statements::OrderColumn*>* sortConditions;

    public:
        explicit MergeComparator(
          const std::vector<QueryPipeline::Statements::OrderColumn*>* sortConditions
        );
        bool operator()(
          const MergeElement& first,
          const MergeElement& second
         ) const;
};