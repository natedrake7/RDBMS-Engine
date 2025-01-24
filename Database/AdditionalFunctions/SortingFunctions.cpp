#include "SortingFunctions.h"

#include "../../AdditionalLibraries/AdditionalDataTypes/Decimal/Decimal.h"
#include "../Block/Block.h"
#include "../Row/Row.h"
#include "SortingAlgorithms/MergeSort/MergeSort.h"
#include "SortingAlgorithms/QuickSort/QuickSort.h"

using namespace DatabaseEngine::StorageTypes;

//merge sort (maybe)

void SortingFunctions::OrderDescending(vector<Row*> &rows, const SortCondition &condition)
{
    const bool& isColumnIndexed = condition.GetIsColumnIndexed();
    const SortType sortType = condition.GetSortType();

    if(isColumnIndexed && sortType == SortType::ASCENDING)
        return;
    if(isColumnIndexed && sortType == SortType::DESCENDING)
    {
        std::reverse(rows.begin(), rows.end());
        return;
    }

    const column_index_t& columnIndex = condition.GetColumnIndex();

    //if dataset is small use quicksort
    if(rows.size() <= 1000)
    {
        QuickSort::Sort(rows, 0, static_cast<int>(rows.size() - 1), columnIndex, sortType);
        return;
    }
    //else mergesort
    
    MergeSort::Sort(rows, 0, static_cast<int>(rows.size() - 1), columnIndex, sortType);
}

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

bool SortingFunctions::CompareBlockByDataType(const Block *&firstBlock, const Block *&secondBlock)
{
    switch (firstBlock->GetColumnType())
    {
        case ColumnType::TinyInt:
        {
            const auto& firstBlockData = *reinterpret_cast<const int8_t*>(firstBlock->GetBlockData());
            const auto& secondBlockData = *reinterpret_cast<const int8_t*>(secondBlock->GetBlockData());

            return firstBlockData < secondBlockData;
        }
        case ColumnType::SmallInt:
        {
            const auto& firstBlockData = *reinterpret_cast<const int16_t*>(firstBlock->GetBlockData());
            const auto& secondBlockData = *reinterpret_cast<const int16_t*>(secondBlock->GetBlockData());

            return firstBlockData < secondBlockData;
        }
        case ColumnType::Int:
        {
            const auto& firstBlockData = *reinterpret_cast<const int32_t*>(firstBlock->GetBlockData());
            const auto& secondBlockData = *reinterpret_cast<const int32_t*>(secondBlock->GetBlockData());

            return firstBlockData < secondBlockData;
        }
        case ColumnType::BigInt:
        {
            const auto& firstBlockData = *reinterpret_cast<const int64_t*>(firstBlock->GetBlockData());
            const auto& secondBlockData = *reinterpret_cast<const int64_t*>(secondBlock->GetBlockData());

            return firstBlockData < secondBlockData;
        }
        case ColumnType::Decimal:
        {
            //implement support for decimal class operations
            return true;
        }
        default:
            throw invalid_argument("AggregateFunctions::CompareMaxWithRow(): Unsupported column type");
    }
}

void SortingFunctions::OrderBy(vector<Row> &rows, vector<Row*>& result, const vector<SortCondition> &sortConditions)
{
    if(rows.empty())
        return;

    for(auto& row: rows)
        result.push_back(&row);

    //handle multiple conditions priority
    //after first condition break into multiple arrays where the first condition is satisfied
    //order by the 2nd condition. do it for the rest etc.
    for(const SortCondition &condition : sortConditions)
        SortingFunctions::OrderDescending(result, condition);
}

void SortingFunctions::GroupBy(vector<Row> &rows, const vector<SortCondition> &sortConditions)
{
    
}
