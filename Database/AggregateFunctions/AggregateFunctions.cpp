#include "AggregateFunctions.h"
#include "../Block/Block.h"
#include "../Column/Column.h"
#include "../Row/Row.h"

using namespace DatabaseEngine::StorageTypes;
using namespace Constants;

void AggregateFunctions::AverageByColumnType(double& sum, const Block* block)
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
            throw invalid_argument("AggregateFunctions::AverageByColumnType(): Unsupported column type");
    }
}

double AggregateFunctions::Average(const vector<Row>& rows, const column_index_t& columnIndex, const double* constantValue)
{
    if (constantValue != nullptr)
        return *constantValue ;
    
    double sum = 0;
    for (const auto& row : rows)
        AggregateFunctions::AverageByColumnType(sum, row.GetData()[columnIndex]);

    return sum / static_cast<double>(rows.size());
}

uint64_t AggregateFunctions::Count(const vector<Row> &rows, const column_index_t &columnIndex, const double *constantValue)
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
