#pragma once
#include <vector>
#include "../../../include/Database.h"

class SortCondition;

namespace DatabaseEngine::StorageTypes {
    class Row;
}

using namespace std;
using namespace Constants;

class QuickSort {
        static int Partition(vector<QueryResult> &rows, const int &low, const int &high, const vector<SortCondition>& sortConditions);
    public:
        static void Sort(vector<QueryResult>& rows, const int &low, const int &high, const vector<SortCondition>& sortConditions);
    
};
