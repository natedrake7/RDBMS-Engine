#pragma once
#include <cstdint>
#include <vector>
#include "DataTypes/DataTypes.h"

namespace Pages{
    struct RowView;
}

class AggregateFunctions {
    public:
        static long double Average(const std::vector<Pages::RowView>& rows, const column_index_t& columnIndex, const long double* constantValue = nullptr);
        static uint64_t Count(const std::vector<Pages::RowView>& rows, const column_index_t& columnIndex, const long double* constantValue = nullptr);
        static long double Max(const std::vector<Pages::RowView>& rows, const column_index_t& columnIndex, const bool& isSelectedColumnIndexed = false, const long double* constantValue = nullptr);
        static long double Min(const std::vector<Pages::RowView>& rows, const column_index_t& columnIndex, const bool& isSelectedColumnIndexed = false, const long double* constantValue = nullptr);
        static long double Sum(const std::vector<Pages::RowView>& rows, const column_index_t& columnIndex, const long double* constantValue = nullptr);
};
