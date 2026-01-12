#pragma once
#include "Constants.h"
#include "DataTypes.h"

namespace Constants{
    enum OrderType : uint8_t;
}

class SortCondition final{
    column_index_t columnIndex;
    Constants::OrderType sortType;
    bool isColumnIndexed;

    public:
        SortCondition(const column_index_t& columnIndex, const Constants::OrderType& sortType, const bool& isColumnIndexed);
        [[nodiscard]] const Constants::OrderType& GetSortType() const;
        [[nodiscard]] const column_index_t& GetColumnIndex() const;
        [[nodiscard]] const bool& GetIsColumnIndexed() const;
};
