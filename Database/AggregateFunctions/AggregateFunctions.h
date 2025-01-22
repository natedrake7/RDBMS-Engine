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

    static void AverageByColumnType(double& sum, const DatabaseEngine::StorageTypes::Block* block);
    
    public:
        static double Average(const vector<DatabaseEngine::StorageTypes::Row>& rows, const Constants::column_index_t& columnIndex, const double* constantValue = nullptr);
        static uint64_t Count(const vector<DatabaseEngine::StorageTypes::Row>& rows, const Constants::column_index_t& columnIndex, const double* constantValue = nullptr);
        static double Max(const vector<DatabaseEngine::StorageTypes::Row>& rows, const Constants::column_index_t& columnIndex, const bool& isSelectedColumnIndexed = false, const double* constantValue = nullptr);
        static double Min(const vector<DatabaseEngine::StorageTypes::Row>& rows, const Constants::column_index_t& columnIndex, const bool& isSelectedColumnIndexed = false, const double* constantValue = nullptr);
        static double Sum(const vector<DatabaseEngine::StorageTypes::Row>& rows, const Constants::column_index_t& columnIndex, const double* constantValue = nullptr);
        
};
