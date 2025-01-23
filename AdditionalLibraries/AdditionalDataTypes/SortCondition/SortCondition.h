#pragma once
#include "../../../Database/Constants.h"

using namespace Constants;

class SortCondition final{
    column_index_t columnIndex;
    SortType sortType;
    bool isColumnIndexed;

    public:
        SortCondition(const column_index_t& columnIndex, const SortType& sortType, const bool& isColumnIndexed);
        [[nodiscard]] const SortType& GetSortType() const;
        [[nodiscard]] const column_index_t& GetColumnIndex() const;
        [[nodiscard]] const bool& GetIsColumnIndexed() const;
};