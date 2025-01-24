#pragma once
#include <vector>
#include "../../../Constants.h"

namespace DatabaseEngine::StorageTypes {
    class Row;
}

using namespace Constants;
using namespace std;

class MergeSort {
        static void Merge(vector<DatabaseEngine::StorageTypes::Row*>& rows, const int& left, const int& mid, const int& right, const column_index_t& columnIndex, const SortType& sortType);
    public:
        static void Sort(vector<DatabaseEngine::StorageTypes::Row*>& rows, const int& left, const int& right, const column_index_t& columnIndex, const SortType& sortType);
};
