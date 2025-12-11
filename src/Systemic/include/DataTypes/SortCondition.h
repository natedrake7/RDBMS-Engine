#pragma once
#include "../../../Database/include/Constants.h"

using namespace Constants;

class SortCondition final{
    column_index_t columnIndex;
    OrderType sortType;
    bool isColumnIndexed;

    public:
        SortCondition(const column_index_t& columnIndex, const OrderType& sortType, const bool& isColumnIndexed);
        [[nodiscard]] const OrderType& GetSortType() const;
        [[nodiscard]] const column_index_t& GetColumnIndex() const;
        [[nodiscard]] const bool& GetIsColumnIndexed() const;
};