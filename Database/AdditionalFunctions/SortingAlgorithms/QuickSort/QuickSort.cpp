#include "QuickSort.h"
#include "../../SortingFunctions.h"
#include "../../../Constants.h"
#include "../../../Row/Row.h"

using namespace DatabaseEngine::StorageTypes;

void QuickSort::Sort(vector<Row*> &rows, const int &low, const int &high, const vector<SortCondition>& sortConditions)
{
    if (low >= high)
        return;
      
    const int pi = QuickSort::Partition(rows, low, high, sortConditions);

    QuickSort::Sort(rows, low, pi - 1, sortConditions);
    QuickSort::Sort(rows, pi + 1, high, sortConditions);
}

int QuickSort::Partition(vector<Row*> &rows, const int &low, const int &high, const vector<SortCondition>& sortConditions)
{
    const auto& pivot = rows[high];
  
    int i = low - 1;

    for (int j = low; j <= high - 1; j++)
    {
        if(!SortingFunctions::CompareRows(rows[j], pivot, sortConditions))
            continue;
        
        i++;

        if(i == j)
            continue;
        
        swap(rows[i], rows[j]);
    }

    swap(rows[i + 1], rows[high]);  
    return i + 1;
}