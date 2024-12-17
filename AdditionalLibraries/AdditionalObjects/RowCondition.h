#pragma once
#include <vector>

#include "../../Database/Constants.h"

class Block;
using namespace Constants;

class RowCondition {
    column_index_t columnIndex;
    object_t* condition;

    public:
        RowCondition();
        RowCondition(const void *condition, const block_size_t& conditionSize, const column_index_t &columnIndex);
        ~RowCondition();
        const column_index_t& GetColumnIndex() const;
        bool operator==(const Block* block) const;
        bool operator!=(const Block* block) const;
};
