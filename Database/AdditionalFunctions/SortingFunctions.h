#pragma once
#include <unordered_map>
#include <vector>
#include "../../AdditionalLibraries/AdditionalDataTypes/SortCondition/SortCondition.h"
#include "../Column/Column.h"

class GroupCondition;

namespace DatabaseEngine::StorageTypes {
    class Block;
    class Row;
}

using namespace std;

typedef struct AggregateResults {
    uint64_t count;
    long double average;
    long double sum;
    long double min;
    long double max;
    AggregateResults();
} AggregateResults;

class SortingFunctions{
         [[nodiscard]] static int CompareBlockByDataType(const DatabaseEngine::StorageTypes::Block*& firstBlock, const DatabaseEngine::StorageTypes::Block*& secondBlock);
         [[nodiscard]] static string CreateGroupByKey(const DatabaseEngine::StorageTypes::Row* row, const vector<GroupCondition> &sortConditions);
         static long double ApplyAggregateFunctionToGroup(const vector<DatabaseEngine::StorageTypes::Row*>& rowGroup, const GroupCondition& condition);

    public:
         [[nodiscard]] static bool CompareRows(const DatabaseEngine::StorageTypes::Row* firstRow, const DatabaseEngine::StorageTypes::Row* secondRow, const vector<SortCondition>& sortConditions);
         [[nodiscard]] static bool CompareRowsAscending(const DatabaseEngine::StorageTypes::Row* firstRow, const DatabaseEngine::StorageTypes::Row* secondRow, const column_index_t& columnIndex);
         [[nodiscard]] static bool CompareRowsDescending(const DatabaseEngine::StorageTypes::Row* firstRow, const DatabaseEngine::StorageTypes::Row* secondRow, const column_index_t& columnIndex);
         static void OrderBy(vector<DatabaseEngine::StorageTypes::Row*>& rows, const vector<SortCondition>& sortConditions);
         [[nodiscard]] static unordered_map<string, AggregateResults> GroupBy(const vector<DatabaseEngine::StorageTypes::Row*>& rows, const vector<GroupCondition>& sortConditions);
};