#pragma once
#include <cstdint>
#include <vector>
#include "../Constants.h"

using namespace std;

namespace DatabaseEngine::StorageTypes {
    class Block;
    class Row;
}

class AggregateFunctions {
        static void SumByColumnType(long double& sum, const DatabaseEngine::StorageTypes::Block* block);
        static void CompareMaxWithRow(long double& max, const DatabaseEngine::StorageTypes::Block* block);
        static void CompareMinWithRow(long double& max, const DatabaseEngine::StorageTypes::Block* block);
    
    public:
        static long double Average(const vector<DatabaseEngine::StorageTypes::Row*>& rows, const Constants::column_index_t& columnIndex, const long double* constantValue = nullptr);
        static uint64_t Count(const vector<DatabaseEngine::StorageTypes::Row*>& rows, const Constants::column_index_t& columnIndex, const long double* constantValue = nullptr);
        static long double Max(const vector<DatabaseEngine::StorageTypes::Row*>& rows, const Constants::column_index_t& columnIndex, const bool& isSelectedColumnIndexed = false, const long double* constantValue = nullptr);
        static long double Min(const vector<DatabaseEngine::StorageTypes::Row*>& rows, const Constants::column_index_t& columnIndex, const bool& isSelectedColumnIndexed = false, const long double* constantValue = nullptr);
        static long double Sum(const vector<DatabaseEngine::StorageTypes::Row*>& rows, const Constants::column_index_t& columnIndex, const long double* constantValue = nullptr);
};
