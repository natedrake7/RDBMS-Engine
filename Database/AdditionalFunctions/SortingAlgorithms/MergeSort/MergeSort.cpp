#include "MergeSort.h"
#include "../../../Row/Row.h"
#include "../../SortingFunctions.h"

using namespace DatabaseEngine::StorageTypes;

void MergeSort::Merge(vector<Row*> &rows, const int &left, const int &mid, const int &right, const column_index_t &columnIndex, const SortType &sortType)
{
    int i, j;
    const int n1 = mid - left + 1;
    const int n2 = right - mid;

    vector<Row*> leftVec, rightVec;

    for (i = 0; i < n1; i++)
        leftVec.push_back(rows[left + i]);
    
    for (j = 0; j < n2; j++)
        rightVec.push_back(rows[mid + 1 + j]);

    i = 0;
    j = 0;
    int k = left;

    while (i < n1 && j < n2)
    {
        const bool comparison = (sortType == SortType::ASCENDING)
                            ? SortingFunctions::CompareRowsAscending(leftVec[i], rightVec[j], columnIndex)
                            : SortingFunctions::CompareRowsDescending(leftVec[i], rightVec[j], columnIndex);

        if (comparison)
        {
            rows[k] = leftVec[i];
            i++;
        }
        else
        {
            rows[k] = rightVec[j];
            j++;
        }
        k++;
    }

    while (i < n1)
    {
        rows[k] = leftVec[i];
        i++;
        k++;
    }

    while (j < n2)
    {
        rows[k] = rightVec[j];
        j++;
        k++;
    }
}

void MergeSort::Sort(vector<Row*> &rows, const int &left, const int &right, const column_index_t &columnIndex, const SortType &sortType)
{
    if(left >= right)
        return;

    // Calculate the midpoint
    const int mid = left + (right - left) / 2;

    // Sort first and second halves
    MergeSort::Sort(rows, left, mid, columnIndex, sortType);
    MergeSort::Sort(rows, mid + 1, right, columnIndex, sortType);

    // Merge the sorted halves
    MergeSort::Merge(rows, left, mid, right, columnIndex, sortType);
}
