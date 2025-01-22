#pragma once
#include "../../../Database/Constants.h"

using namespace Constants;

enum SortType : uint8_t {
    ASCENDING = 0,
    DESCENDING = 1,
};

class SortCondition final{
    column_index_t columnIndex;
    SortType sortType;

    public:
        SortCondition(const column_index_t& columnIndex, const SortType& sortType);
};