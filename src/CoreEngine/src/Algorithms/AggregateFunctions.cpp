#include "../../include/Algorithms/AggregateFunctions.h"
#include "../../include/DataStorage/Column.h"

#include <limits>

#include "DataStorage/Row.h"

long double AggregateFunctions::Average(
    const std::vector<CoreEngine::StorageTypes::RID>& rows,
    const column_index_t& columnIndex,
    const long double* constantValue
){
    if (constantValue != nullptr)
        return *constantValue ;
    
    long double sum = 0;
    
    // for (const auto& row : rows)
    //     AggregateFunctions::SumByColumnType(sum, row->GetData()[columnIndex]);

    // return sum / static_cast<long double>(rows.size());
    return 0;
}

uint64_t AggregateFunctions::Count(
    const std::vector<CoreEngine::StorageTypes::RID>& rows,
    const column_index_t &columnIndex,
    const long double *constantValue
){
    // if (constantValue != nullptr)
    //     return rows.size();
    
    uint64_t count = 0;
    // for (const auto& row : rows)
    // {
    //     // if (row->GetNullBitMapValue(columnIndex))
    //     //     continue;
    //
    //     count++;
    // }

    return count;
}

long double AggregateFunctions::Max(const std::vector<CoreEngine::StorageTypes::RID> &rows, const column_index_t &columnIndex, const bool &isSelectedColumnIndexed, const long double *constantValue)
{
    if (constantValue != nullptr)
        return *constantValue;

    if (rows.empty())
        return 0;
        
    long double max = std::numeric_limits<long double>::lowest();

    // if (isSelectedColumnIndexed)
    // {
    //     AggregateFunctions::CompareMaxWithRow(max, rows.back()->GetData()[columnIndex]);
    //     return max;
    // }
    //
    // for (const auto& row : rows)
    //     AggregateFunctions::CompareMaxWithRow(max, row->GetData()[columnIndex]);

    return max;
}

long double AggregateFunctions::Min(const std::vector<CoreEngine::StorageTypes::RID> &rows, const column_index_t &columnIndex, const bool &isSelectedColumnIndexed, const long double *constantValue)
{
    if (constantValue != nullptr)
        return *constantValue;

    if (rows.empty())
        return 0;

    long double min = std::numeric_limits<long double>::max();

    if (isSelectedColumnIndexed)
    {
        // AggregateFunctions::CompareMinWithRow(min, rows.front()->GetData()[columnIndex]);
        return min;
    }

    // for (const auto& row : rows)
    //     AggregateFunctions::CompareMinWithRow(min, row->GetData()[columnIndex]);

    return min;
}

long double AggregateFunctions::Sum(const std::vector<CoreEngine::StorageTypes::RID> &rows, const column_index_t &columnIndex, const long double *constantValue)
{
    if (constantValue != nullptr)
        return *constantValue;

    if (rows.empty())
        return 0;

    long double sum = 0;

    // for (const auto& row : rows)
    //     AggregateFunctions::SumByColumnType(sum, row->GetData()[columnIndex]);

    return sum;
}























