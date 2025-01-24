#pragma once
#include <vector>
#include "../../../Database.h"

namespace DatabaseEngine::StorageTypes {
    class Row;
}

using namespace std;
using namespace Constants;

class QuickSort {
        static int Partition(vector<DatabaseEngine::StorageTypes::Row*> &rows, const int &low, const int &high, const column_index_t& columnIndex, const SortType& sortType);
    public:
        static void Sort(vector<DatabaseEngine::StorageTypes::Row*>& rows, const int &low, const int &high, const column_index_t& columnIndex, const SortType& sortType);
    
};
