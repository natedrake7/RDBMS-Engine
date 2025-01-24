#pragma once
#include <vector>
#include "../../AdditionalLibraries/AdditionalDataTypes/SortCondition/SortCondition.h"
#include "../Column/Column.h"

namespace DatabaseEngine::StorageTypes {
    class Block;
    class Row;
}

using namespace std;

class SortingFunctions{
        static void QuickSort(vector<DatabaseEngine::StorageTypes::Row>& rows, const int& low, const int& high, const column_index_t& columnIndex, const SortType& sortType);
        static int Partition(vector<DatabaseEngine::StorageTypes::Row>& rows, const int& low, const int& high, const column_index_t& columnIndex, const SortType& sortType);
        static void OrderDescending(vector<DatabaseEngine::StorageTypes::Row>& rows, const SortCondition& condition);
        static bool CompareRowsAscending(const DatabaseEngine::StorageTypes::Row& firstRow, const DatabaseEngine::StorageTypes::Row& secondRow, const column_index_t& columnIndex);
        static bool CompareRowsDescending(const DatabaseEngine::StorageTypes::Row& firstRow, const DatabaseEngine::StorageTypes::Row& secondRow, const column_index_t& columnIndex);
        static bool CompareBlockByDataType(const DatabaseEngine::StorageTypes::Block*& firstBlock, const DatabaseEngine::StorageTypes::Block*& secondBlock);

    public:
        static void OrderBy(vector<DatabaseEngine::StorageTypes::Row>& rows, const vector<SortCondition>& sortConditions);
        static void GroupBy(vector<DatabaseEngine::StorageTypes::Row>& rows, const vector<SortCondition>& sortConditions);
};