#include "SortingFunctions.h"
#include <ppltasks.h>
#include <ranges>

#include "../../AdditionalLibraries/AdditionalDataTypes/Decimal/Decimal.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/GroupCondition/GroupCondition.h"
#include "./AggregateFunctions/AggregateFunctions.h"
#include "../Block/Block.h"
#include "../Row/Row.h"
#include "SortingAlgorithms/MergeSort/MergeSort.h"
#include "SortingAlgorithms/QuickSort/QuickSort.h"

using namespace DatabaseEngine::StorageTypes;

bool SortingFunctions::CompareRowsAscending(const Row *firstRow, const Row *secondRow, const column_index_t& columnIndex)
{
    const Block* firstRowData = firstRow->GetData()[columnIndex];
    const Block* secondRowData = secondRow->GetData()[columnIndex];
    
    return SortingFunctions::CompareBlockByDataType(firstRowData, secondRowData);
}

bool SortingFunctions::CompareRowsDescending(const Row* firstRow, const Row* secondRow, const column_index_t &columnIndex)
{
    return !SortingFunctions::CompareRowsAscending(firstRow, secondRow, columnIndex);
}

AggregateResults::AggregateResults()
{
    this->average = 0;
    this->min = 0;
    this->max = 0;
    this->sum = 0;
    this->count = 0;
}

int SortingFunctions::CompareBlockByDataType(const Block *&firstBlock, const Block *&secondBlock)
{
    switch (firstBlock->GetColumnType())
    {
        case ColumnType::TinyInt:
        {
            const auto& firstBlockData = *reinterpret_cast<const int8_t*>(firstBlock->GetBlockData());
            const auto& secondBlockData = *reinterpret_cast<const int8_t*>(secondBlock->GetBlockData());

            if (firstBlockData < secondBlockData) return 1;
            if (firstBlockData > secondBlockData) return -1;
            return 0;
        }
        case ColumnType::SmallInt:
        {
            const auto& firstBlockData = *reinterpret_cast<const int16_t*>(firstBlock->GetBlockData());
            const auto& secondBlockData = *reinterpret_cast<const int16_t*>(secondBlock->GetBlockData());

            if (firstBlockData < secondBlockData) return 1;
            if (firstBlockData > secondBlockData) return -1;
            return 0;
        }
        case ColumnType::Int:
        {
            const auto& firstBlockData = *reinterpret_cast<const int32_t*>(firstBlock->GetBlockData());
            const auto& secondBlockData = *reinterpret_cast<const int32_t*>(secondBlock->GetBlockData());

            if (firstBlockData < secondBlockData) return 1;
            if (firstBlockData > secondBlockData) return -1;
            return 0;
        }
        case ColumnType::BigInt:
        {
            const auto& firstBlockData = *reinterpret_cast<const int64_t*>(firstBlock->GetBlockData());
            const auto& secondBlockData = *reinterpret_cast<const int64_t*>(secondBlock->GetBlockData());

            if (firstBlockData < secondBlockData) return 1;
            if (firstBlockData > secondBlockData) return -1;
            return 0;
        }
        case ColumnType::Decimal:
        {
            //implement support for decimal class operations
            return true;
        }
        case ColumnType::DateTime:
        {
            const auto& firstBlockData = *reinterpret_cast<const time_t*>(firstBlock->GetBlockData());
            const auto& secondBlockData = *reinterpret_cast<const time_t*>(secondBlock->GetBlockData());

            if (firstBlockData < secondBlockData) return 1;
            if (firstBlockData > secondBlockData) return -1;
            return 0;
        }
        case ColumnType::Bool:
        {
            const auto& firstBlockData = *reinterpret_cast<const bool*>(firstBlock->GetBlockData());
            const auto& secondBlockData = *reinterpret_cast<const bool*>(secondBlock->GetBlockData());

            if (firstBlockData < secondBlockData) return 1;
            if (firstBlockData > secondBlockData) return -1;
            return 0;
        }
        case ColumnType::String:
        {
            const auto& firstBlockDataSize = firstBlock->GetBlockSize();
            const auto& secondBlockDataSize = secondBlock->GetBlockSize();
            
            if (firstBlockDataSize < secondBlockDataSize) return 1;
            if (firstBlockDataSize > secondBlockDataSize) return -1;

            const int result = memcmp(firstBlock->GetBlockData(), secondBlock->GetBlockData(), firstBlockDataSize);

            if (result > 0) return 1;
            if (result < 0) return -1;
            return 0;
        }
        default:
            throw invalid_argument("AggregateFunctions::CompareMaxWithRow(): Unsupported column type");
    }
}

bool SortingFunctions::CompareRows(const Row *firstRow, const Row *secondRow, const vector<SortCondition> &sortConditions)
{
    for (const auto& condition : sortConditions)
    {
        const column_index_t& columnIndex = condition.GetColumnIndex();

        const Block* firstRowData = firstRow->GetData()[columnIndex];
        const Block* secondRowData = secondRow->GetData()[columnIndex];

        //if column is indexed(and it is the first condition, it is already sorted by it so set the result accordingly result is positive)
        const int result = SortingFunctions::CompareBlockByDataType(firstRowData, secondRowData);
    
        if(result == 0)
            continue;
        
        return (condition.GetSortType() == SortType::DESCENDING)
                        ? (result < 0)
                        : (result > 0);
    }

    return false;
}

void SortingFunctions::OrderBy(vector<Row*> &rows, const vector<SortCondition> &sortConditions)
{
    if(rows.empty())
        return;

    //handle multiple conditions priority
    //after first condition break into multiple arrays where the first condition is satisfied
    //order by the 2nd condition. do it for the rest etc.
    const auto& condition = sortConditions.front();
    const bool& isColumnIndexed = condition.GetIsColumnIndexed();
    const SortType sortType = condition.GetSortType();
    
    if(isColumnIndexed && sortType == SortType::ASCENDING)
        return;
    if(isColumnIndexed && sortType == SortType::DESCENDING)
    {
        ranges::reverse(rows);
        return;
    }

    //if dataset is small use quicksort (is it needed?)
    // if(rows.size() <= 1000)
    // {
    //     QuickSort::Sort(rows, 0, static_cast<int>(rows.size() - 1), sortConditions);
    //     return;
    // }
    //else mergesort
    
    MergeSort::Sort(rows, 0, static_cast<int>(rows.size() - 1), sortConditions);
}

unordered_map<string, AggregateResults> SortingFunctions::GroupBy(const vector<Row*> &rows, const vector<GroupCondition> &sortConditions)
{
    unordered_map<string, AggregateResults> groupedResults;
    unordered_map<string, vector<Row*>> groupedRows;

    //add any aggregate function execution asWell by condition
    //also store the keys of the groupBy used in order to prin them.
    //should be done in a single loop
    for(const auto& row : rows)
        groupedRows[SortingFunctions::CreateGroupByKey(row, sortConditions)].push_back(row);

    for(const auto& [key , rowGroup ] : groupedRows)
    {
        AggregateResults aggregateResults;
        for(const auto& condition : sortConditions)
        {
            switch (condition.GetAggregateFunction())
            {
                case NONE:
                case COUNT:
                default:
                    aggregateResults.count = AggregateFunctions::Count(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
                    break;
                case SUM:
                    aggregateResults.sum = AggregateFunctions::Sum(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
                    break;
                case MIN:
                    aggregateResults.min = AggregateFunctions::Min(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
                    break;
                case MAX:
                    aggregateResults.max = AggregateFunctions::Max(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
                    break;
                case AVERAGE:
                    aggregateResults.average = AggregateFunctions::Average(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
                    break;
            }
        }

        groupedResults[key] = aggregateResults;
    }

    return groupedResults;
}

string SortingFunctions::CreateGroupByKey(const Row* row, const vector<GroupCondition> &sortConditions)
{
    string hashKey;
    const auto& rowData = row->GetData();

    for(const auto& condition : sortConditions)
    {
        const auto& block = rowData[condition.GetColumnIndex()];
        
        hashKey.append(reinterpret_cast<const char*>(block->GetBlockData()), block->GetBlockSize());
    }

    return hashKey;
}

long double SortingFunctions::ApplyAggregateFunctionToGroup(const vector<Row *> &rowGroup, const GroupCondition &condition)
{
    switch (condition.GetAggregateFunction())
    {
        case NONE:
        case COUNT:
        default:
            return static_cast<long double>(AggregateFunctions::Count(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue()));
        case SUM:
            return AggregateFunctions::Sum(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
        case MIN:
            return AggregateFunctions::Min(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
        case MAX:
            return AggregateFunctions::Max(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
    }
}
