#include "AggregateFunctions.h"
#include "../Block/Block.h"
#include "../Column/Column.h"
#include "../Row/Row.h"

using namespace DatabaseEngine::StorageTypes;
using namespace Constants;

void AggregateFunctions::SumByColumnType(long double& sum, const Block* block)
{
    switch (block->GetColumnType())
    {
        case ColumnType::TinyInt:
            sum += *reinterpret_cast<const int8_t*>(block->GetBlockData());
            break;
        case ColumnType::SmallInt:
            sum += *reinterpret_cast<const int16_t*>(block->GetBlockData());
            break;
        case ColumnType::Int:
            sum += *reinterpret_cast<const int32_t*>(block->GetBlockData());
            break;
        case ColumnType::BigInt:
            sum += *reinterpret_cast<const int64_t*>(block->GetBlockData());
            break;
        case ColumnType::Decimal:
            //implement support for decimal class operations
            break;
        default:
            throw invalid_argument("AggregateFunctions::SumByColumnType(): Unsupported column type");
    }
}

void AggregateFunctions::CompareMaxWithRow(long double &max, const Block *block)
{
    switch (block->GetColumnType())
    {
        case ColumnType::TinyInt:
        {
            const auto& castData = *reinterpret_cast<const int8_t*>(block->GetBlockData());
            if (max < castData)
                max = castData;
            break;
        }

        case ColumnType::SmallInt:
        {
            const auto& castData = *reinterpret_cast<const int16_t*>(block->GetBlockData());
            if (max < castData)
                max = castData;
            break;
        }
        case ColumnType::Int:
        {
            const auto& castData = *reinterpret_cast<const int32_t*>(block->GetBlockData());
            if (max < castData)
                max = castData;
            break;
        }
        case ColumnType::BigInt:
        {
            const auto& castData = *reinterpret_cast<const int64_t*>(block->GetBlockData());
            if (max < castData)
                max = castData;
            break;
        }
        case ColumnType::Decimal:
                break;
        default:
            throw invalid_argument("AggregateFunctions::CompareMaxWithRow(): Unsupported column type");
    }
}

void AggregateFunctions::CompareMinWithRow(long double &max, const DatabaseEngine::StorageTypes::Block *block)
{
    switch (block->GetColumnType())
    {
        case ColumnType::TinyInt:
        {
            const auto& castData = *reinterpret_cast<const int8_t*>(block->GetBlockData());
            if (max > castData)
                max = castData;
            break;
        }

        case ColumnType::SmallInt:
        {
            const auto& castData = *reinterpret_cast<const int16_t*>(block->GetBlockData());
            if (max > castData)
                max = castData;
            break;
        }
        case ColumnType::Int:
        {
            const auto& castData = *reinterpret_cast<const int32_t*>(block->GetBlockData());
            if (max > castData)
                max = castData;
            break;
        }
        case ColumnType::BigInt:
        {
            const auto& castData = *reinterpret_cast<const int64_t*>(block->GetBlockData());
            if (max > castData)
                max = castData;
            break;
        }
        case ColumnType::Decimal:
            //implement support for decimal class operations
                break;
        default:
            throw invalid_argument("AggregateFunctions::CompareMaxWithRow(): Unsupported column type");
    }
}

long double AggregateFunctions::Average(const vector<Row>& rows, const column_index_t& columnIndex, const long double* constantValue)
{
    if (constantValue != nullptr)
        return *constantValue ;
    
    long double sum = 0;
    
    for (const auto& row : rows)
        AggregateFunctions::SumByColumnType(sum, row.GetData()[columnIndex]);

    return sum / static_cast<long double>(rows.size());
}

uint64_t AggregateFunctions::Count(const vector<Row> &rows, const column_index_t &columnIndex, const long double *constantValue)
{
    if (constantValue != nullptr)
        return rows.size();
    
    uint64_t count = 0;
    for (const auto& row : rows)
    {
        if (row.GetNullBitMapValue(columnIndex))
            continue;

        count++;
    }

    return count;
}

long double AggregateFunctions::Max(const vector<Row> &rows, const column_index_t &columnIndex, const bool &isSelectedColumnIndexed, const long double *constantValue)
{
    if (constantValue != nullptr)
        return *constantValue;

    if (rows.empty())
        return 0;
        
    long double max = numeric_limits<long double>::lowest();

    if (isSelectedColumnIndexed)
    {
        AggregateFunctions::CompareMaxWithRow(max, rows.back().GetData()[columnIndex]);
        return max;
    }

    for (const auto& row : rows)
        AggregateFunctions::CompareMaxWithRow(max, row.GetData()[columnIndex]);

    return max;
}

long double AggregateFunctions::Min(const vector<DatabaseEngine::StorageTypes::Row> &rows, const Constants::column_index_t &columnIndex, const bool &isSelectedColumnIndexed, const long double *constantValue)
{
    if (constantValue != nullptr)
        return *constantValue;

    if (rows.empty())
        return 0;

    long double min = numeric_limits<long double>::max();

    if (isSelectedColumnIndexed)
    {
        AggregateFunctions::CompareMinWithRow(min, rows.front().GetData()[columnIndex]);
        return min;
    }

    for (const auto& row : rows)
        AggregateFunctions::CompareMinWithRow(min, row.GetData()[columnIndex]);

    return min;
}

long double AggregateFunctions::Sum(const vector<DatabaseEngine::StorageTypes::Row> &rows, const Constants::column_index_t &columnIndex, const long double *constantValue)
{
    if (constantValue != nullptr)
        return *constantValue;

    if (rows.empty())
        return 0;

    long double sum = 0;

    for (const auto& row : rows)
        AggregateFunctions::SumByColumnType(sum, row.GetData()[columnIndex]);

    return sum;
}























