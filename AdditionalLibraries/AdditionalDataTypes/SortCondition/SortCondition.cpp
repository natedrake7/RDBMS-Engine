#include "SortCondition.h"

SortCondition::SortCondition(const column_index_t& columnIndex, const SortType& sortType, const bool& isColumnIndexed)
{
    this->columnIndex = columnIndex;
    this->sortType = sortType;
    this->isColumnIndexed = isColumnIndexed;
}

const SortType & SortCondition::GetSortType() const { return this->sortType; }

const column_index_t & SortCondition::GetColumnIndex() const { return this->columnIndex; }

const bool & SortCondition::GetIsColumnIndexed() const { return this->isColumnIndexed; }
