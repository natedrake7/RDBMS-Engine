#include "../../../include/Algorithms/Sort/QuickSort.h"
#include "../../../include/Algorithms/Sort/SortingFunctions.h"

#include "../../../include/DataStorage/Row.h"

void QuickSort::Sort(std::vector<QueryResult> &rows, const Int low, const Int high, const std::vector<SortCondition>& sortConditions)
{
    if (low >= high)
        return;
      
    const int pi = QuickSort::Partition(rows, low, high, sortConditions);

    QuickSort::Sort(rows, low, pi - 1, sortConditions);
    QuickSort::Sort(rows, pi + 1, high, sortConditions);
}

int QuickSort::Partition(std::vector<QueryResult> &rows, const Int low, const Int high, const std::vector<SortCondition>& sortConditions)
{
    const auto& pivot = rows[high];
  
    int i = low - 1;

    for (int j = low; j <= high - 1; j++)
    {
        // if(!SortingFunctions::CompareRows(rows[j], pivot, sortConditions))
        //     continue;
        
        i++;

        if(i == j)
            continue;
        
        std::swap(rows[i], rows[j]);
    }

    std::swap(rows[i + 1], rows[high]);
    return i + 1;
}