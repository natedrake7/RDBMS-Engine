#pragma once
#include <cstdint>
#include <vector>
#include "DataTypes/DataTypes.h"

namespace CoreEngine::StorageTypes{
    struct RID;
}

class AggregateFunctions {
    public:
        static long double Average(const std::vector<CoreEngine::StorageTypes::RID>& rows, const column_index_t& columnIndex, const long double* constantValue = nullptr);
        static uint64_t Count(const std::vector<CoreEngine::StorageTypes::RID>& rows, const column_index_t& columnIndex, const long double* constantValue = nullptr);
        static long double Max(const std::vector<CoreEngine::StorageTypes::RID>& rows, const column_index_t& columnIndex, const bool& isSelectedColumnIndexed = false, const long double* constantValue = nullptr);
        static long double Min(const std::vector<CoreEngine::StorageTypes::RID>& rows, const column_index_t& columnIndex, const bool& isSelectedColumnIndexed = false, const long double* constantValue = nullptr);
        static long double Sum(const std::vector<CoreEngine::StorageTypes::RID>& rows, const column_index_t& columnIndex, const long double* constantValue = nullptr);
};
