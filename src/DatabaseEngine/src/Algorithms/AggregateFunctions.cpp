#include "../../include/Algorithms/AggregateFunctions.h"
#include "../../include/DataStorage/Block.h"
#include "../../include/DataStorage/Column.h"
#include "../../include/DataStorage/Row.h"

#include <limits>
#include <stdexcept>

void AggregateFunctions::SumByColumnType(long double& sum, const DatabaseEngine::StorageTypes::Block* block)
{
    switch (block->ColumnType())
    {
        case DataType::TinyInt:
            sum += block->AsTinyInt();
            break;
        case DataType::SmallInt:
            sum += block->AsSmallInt();
            break;
        case DataType::Int:
            sum += block->AsInt();
            break;
        case DataType::BigInt:
            sum += block->AsBigInt();
            break;
        case DataType::Decimal:
            //implement support for decimal class operations
            break;
        default:
            throw invalid_argument("AggregateFunctions::SumByColumnType(): Unsupported column type");
    }
}

void AggregateFunctions::CompareMaxWithRow(long double &max, const DatabaseEngine::StorageTypes::Block *block)
{
    switch (block->ColumnType())
    {
        case DataType::TinyInt:
        {
            const auto& castData = block->AsTinyInt();
            if (max < castData)
                max = castData;
            break;
        }

        case DataType::SmallInt:
        {
            const auto& castData = block->AsSmallInt();
            if (max < castData)
                max = castData;
            break;
        }
        case DataType::Int:
        {
            const auto& castData = block->AsInt();
            if (max < castData)
                max = castData;
            break;
        }
        case DataType::BigInt:
        {
            const auto& castData = block->AsBigInt();
            if (max < castData)
                max = castData;
            break;
        }
        case DataType::Decimal:
                break;
        default:
            throw invalid_argument("AggregateFunctions::CompareMaxWithRow(): Unsupported column type");
    }
}

void AggregateFunctions::CompareMinWithRow(long double &max, const DatabaseEngine::StorageTypes::Block *block)
{
    switch (block->ColumnType())
    {
        case DataType::TinyInt:
        {
            const auto& castData = block->AsTinyInt();
            if (max > castData)
                max = castData;
            break;
        }

        case DataType::SmallInt:
        {
            const auto& castData = block->AsSmallInt();
            if (max > castData)
                max = castData;
            break;
        }
        case DataType::Int:
        {
            const auto& castData = block->AsInt();
            if (max > castData)
                max = castData;
            break;
        }
        case DataType::BigInt:
        {
            const auto& castData = block->AsBigInt();
            if (max > castData)
                max = castData;
            break;
        }
        case DataType::Decimal:
            //implement support for decimal class operations
                break;
        default:
            throw invalid_argument("AggregateFunctions::CompareMaxWithRow(): Unsupported column type");
    }
}

long double AggregateFunctions::Average(const vector<DatabaseEngine::StorageTypes::Row*>& rows, const column_index_t& columnIndex, const long double* constantValue)
{
    if (constantValue != nullptr)
        return *constantValue ;
    
    long double sum = 0;
    
    for (const auto& row : rows)
        AggregateFunctions::SumByColumnType(sum, row->GetData()[columnIndex]);

    return sum / static_cast<long double>(rows.size());
}

uint64_t AggregateFunctions::Count(const vector<DatabaseEngine::StorageTypes::Row*>& rows, const column_index_t &columnIndex, const long double *constantValue)
{
    if (constantValue != nullptr)
        return rows.size();
    
    uint64_t count = 0;
    for (const auto& row : rows)
    {
        if (row->GetNullBitMapValue(columnIndex))
            continue;

        count++;
    }

    return count;
}

long double AggregateFunctions::Max(const vector<DatabaseEngine::StorageTypes::Row*> &rows, const column_index_t &columnIndex, const bool &isSelectedColumnIndexed, const long double *constantValue)
{
    if (constantValue != nullptr)
        return *constantValue;

    if (rows.empty())
        return 0;
        
    long double max = numeric_limits<long double>::lowest();

    if (isSelectedColumnIndexed)
    {
        AggregateFunctions::CompareMaxWithRow(max, rows.back()->GetData()[columnIndex]);
        return max;
    }

    for (const auto& row : rows)
        AggregateFunctions::CompareMaxWithRow(max, row->GetData()[columnIndex]);

    return max;
}

long double AggregateFunctions::Min(const vector<DatabaseEngine::StorageTypes::Row*> &rows, const column_index_t &columnIndex, const bool &isSelectedColumnIndexed, const long double *constantValue)
{
    if (constantValue != nullptr)
        return *constantValue;

    if (rows.empty())
        return 0;

    long double min = numeric_limits<long double>::max();

    if (isSelectedColumnIndexed)
    {
        AggregateFunctions::CompareMinWithRow(min, rows.front()->GetData()[columnIndex]);
        return min;
    }

    for (const auto& row : rows)
        AggregateFunctions::CompareMinWithRow(min, row->GetData()[columnIndex]);

    return min;
}

long double AggregateFunctions::Sum(const vector<DatabaseEngine::StorageTypes::Row*> &rows, const column_index_t &columnIndex, const long double *constantValue)
{
    if (constantValue != nullptr)
        return *constantValue;

    if (rows.empty())
        return 0;

    long double sum = 0;

    for (const auto& row : rows)
        AggregateFunctions::SumByColumnType(sum, row->GetData()[columnIndex]);

    return sum;
}























