#include "QuickSort.h"
#include "../../SortingFunctions.h"
#include "../../../Constants.h"
#include "../../../Row/Row.h"

using namespace DatabaseEngine::StorageTypes;

void QuickSort::Sort(vector<Row*> &rows, const int &low, const int &high, const column_index_t& columnIndex, const SortType& sortType)
{
    if (low >= high)
        return;
      
    const int pi = QuickSort::Partition(rows, low, high, columnIndex, sortType);

    QuickSort::Sort(rows, low, pi - 1, columnIndex, sortType);
    QuickSort::Sort(rows, pi + 1, high, columnIndex, sortType);
}

int QuickSort::Partition(vector<Row*> &rows, const int &low, const int &high, const column_index_t& columnIndex, const SortType& sortType)
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
        
        swap(rows[i], rows[j]);
    }

    swap(rows[i + 1], rows[high]);  
    return i + 1;
}