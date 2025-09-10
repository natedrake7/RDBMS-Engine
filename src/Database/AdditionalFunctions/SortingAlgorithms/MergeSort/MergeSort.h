#pragma once
#include <vector>
#include "../../../Constants.h"
#include "../../../../AdditionalLibraries/DataTypes/SortCondition/SortCondition.h"

class QueryResult;

namespace DatabaseEngine::StorageTypes {
    class Row;
}

using namespace Constants;
using namespace std;

class MergeSort {
        static void Merge(vector<QueryResult>& rows, const int& left, const int& mid, const int& right, const vector<SortCondition>& sortConditions);
    public:
        static void Sort(vector<QueryResult>& rows, const int& left, const int& right, const vector<SortCondition>& sortConditions);
};
