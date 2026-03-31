#include "../../../include/DataStorage/Row.h"
#include "../../../include/Algorithms/Sort/MergeSort.h"
#include "../../../include/Algorithms/Sort/SortingFunctions.h"

void MergeSort::Merge(const MergeSortParameters& parameters){
    Int i, j;
    const Int n1 = parameters.mid - parameters.left + 1;
    const Int n2 = parameters.right - parameters.mid;

    std::vector<QueryResult> leftVec, rightVec;

    for (i = 0; i < n1; i++)
        leftVec.push_back(std::move((*parameters.rows)[parameters.left + i]));
    
    for (j = 0; j < n2; j++)
        rightVec.push_back(std::move((*parameters.rows)[parameters.mid + 1 + j]));

    i = 0;
    j = 0;
    Int k = parameters.left;

    while (i < n1 && j < n2){
        if (SortingFunctions::CompareRows(
            *parameters.properties,
            leftVec[i],
            rightVec[j],
            *parameters.sortConditions
        )){
            (*parameters.rows)[k] = leftVec[i];
            i++;
        }
        else
        {
            (*parameters.rows)[k] = rightVec[j];
            j++;
        }
        k++;
    }

    while (i < n1)
    {
        (*parameters.rows)[k] = leftVec[i];
        i++;
        k++;
    }

    while (j < n2)
    {
        (*parameters.rows)[k] = rightVec[j];
        j++;
        k++;
    }
}

void MergeSort::Sort(MergeSortParameters& parameters){
    if(parameters.left >= parameters.right)
        return;

    // Calculate the midpoint
    const int mid = parameters.left + (parameters.right - parameters.left) / 2;

    const auto prevRight = parameters.right;

    parameters.right = mid;
    MergeSort::Sort(parameters);

    parameters.left = mid + 1;
    parameters.right = prevRight;
    MergeSort::Sort(parameters);

    // Merge the sorted halves
    MergeSort::Merge(parameters);
}
