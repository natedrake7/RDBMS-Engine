#include "SortCondition.h"

SortCondition::SortCondition(const column_index_t& columnIndex, const SortCondition& sortType)
{
    this->columnIndex = columnIndex;
    this->sortType = sortType;
}
