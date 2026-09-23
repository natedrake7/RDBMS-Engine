#pragma once
#include <vector>

#include "DataTypes/DataTypes.h"
class MaterializedRow;
class SortCondition;

namespace CoreEngine::StorageTypes {
    class Row;
}

class QuickSort {
        static int Partition(std::vector<MaterializedRow> &rows, Int low, Int high, const std::vector<SortCondition>& sortConditions);
    public:
        static void Sort(std::vector<MaterializedRow>& rows, Int low, Int high, const std::vector<SortCondition>& sortConditions);
    
};
