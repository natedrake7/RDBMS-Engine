#pragma once
#include <vector>

#include "DataTypes/DataTypes.h"
class QueryResult;
class SortCondition;

namespace DatabaseEngine::StorageTypes {
    class Row;
}

class QuickSort {
        static int Partition(std::vector<QueryResult> &rows, Int low, Int high, const std::vector<SortCondition>& sortConditions);
    public:
        static void Sort(std::vector<QueryResult>& rows, Int low, Int high, const std::vector<SortCondition>& sortConditions);
    
};
