#include "../../../include/DataStorage/Row.h"
#include "../../../include/Algorithms/Sort/MergeSort.h"
#include "../../../include/Algorithms/Sort/SortingFunctions.h"

void MergeSort::Merge(std::vector<QueryResult> &rows, const int &left, const int &mid, const int &right, const std::vector<QueryPipeline::Statements::OrderColumn*>& sortConditions)
{
    int i, j;
    const int n1 = mid - left + 1;
    const int n2 = right - mid;

    std::vector<QueryResult> leftVec, rightVec;

    for (i = 0; i < n1; i++)
        leftVec.push_back(std::move(rows[left + i]));
    
    for (j = 0; j < n2; j++)
        rightVec.push_back(std::move(rows[mid + 1 + j]));

    i = 0;
    j = 0;
    int k = left;

    while (i < n1 && j < n2)
    {
        if (SortingFunctions::CompareRows(leftVec[i], rightVec[j], sortConditions))
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

void MergeSort::Sort(std::vector<QueryResult> &rows, const int &left, const int &right, const std::vector<QueryPipeline::Statements::OrderColumn*>& sortConditions)
{
    if(left >= right)
        return;

    // Calculate the midpoint
    const int mid = left + (right - left) / 2;

    // Sort first and second halves
    MergeSort::Sort(rows, left, mid, sortConditions);
    MergeSort::Sort(rows, mid + 1, right, sortConditions);

    // Merge the sorted halves
    MergeSort::Merge(rows, left, mid, right, sortConditions);
}
