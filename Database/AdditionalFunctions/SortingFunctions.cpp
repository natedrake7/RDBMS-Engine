#include "SortingFunctions.h"

#include "../../AdditionalLibraries/AdditionalDataTypes/Decimal/Decimal.h"
#include "../Block/Block.h"
#include "../Row/Row.h"

using namespace DatabaseEngine::StorageTypes;

//merge sort (maybe)
void SortingFunctions::QuickSort(vector<Row> &rows, const int &low, const int &high, const column_index_t& columnIndex, const SortType& sortType)
{
    if (low >= high)
        return;
      
    const int pi = SortingFunctions::Partition(rows, low, high, columnIndex, sortType);

    SortingFunctions::QuickSort(rows, low, pi - 1, columnIndex, sortType);
    SortingFunctions::QuickSort(rows, pi + 1, high, columnIndex, sortType);
}

int SortingFunctions::Partition(vector<Row> &rows, const int &low, const int &high, const column_index_t& columnIndex, const SortType& sortType)
{
    const auto& pivot = rows[high];
  
    int i = low - 1;

    for (int j = low; j <= high - 1; j++)
    {
        const bool comparison = (sortType == SortType::ASCENDING)
                                    ? SortingFunctions::CompareRowsAscending(rows[j], pivot, columnIndex)
                                    : SortingFunctions::CompareRowsDescending(rows[j], pivot, columnIndex);

        if(!comparison)
            continue;
        
        i++;

        if(i == j)
            continue;

        const Row tempRow = rows[j];

        rows[j] = rows[i];
        rows[i] = tempRow;
        
        // swap(rows[i], rows[j]);
    }

    const Row tempRow = rows[high];
    rows[high] = rows[i + 1];
    rows[i + 1] = tempRow;

    // swap(rows[i + 1], rows[high]);  
    return i + 1;
}

void SortingFunctions::OrderDescending(vector<Row> &rows, const SortCondition &condition)
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

    SortingFunctions::QuickSort(rows, 0, static_cast<int>(rows.size() - 1), columnIndex, sortType);
    
    // if(sortType == SortType::DESCENDING)
    //     reverse(rows.begin(), rows.end());
}

bool SortingFunctions::CompareRowsAscending(const Row &firstRow, const Row &secondRow, const column_index_t& columnIndex)
{
    const Block* firstRowData = firstRow.GetData()[columnIndex];
    const Block* secondRowData = secondRow.GetData()[columnIndex];
    
    return SortingFunctions::CompareBlockByDataType(firstRowData, secondRowData);
}

bool SortingFunctions::CompareRowsDescending(const Row &firstRow, const Row &secondRow, const column_index_t &columnIndex)
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

void SortingFunctions::OrderBy(vector<Row> &rows, const vector<SortCondition> &sortConditions)
{
    if(rows.empty())
        return;

    //handle multiple conditions priority
    for(const SortCondition &condition : sortConditions)
        SortingFunctions::OrderDescending(rows, condition);
}

void SortingFunctions::GroupBy(vector<Row> &rows, const vector<SortCondition> &sortConditions)
{
    
}
