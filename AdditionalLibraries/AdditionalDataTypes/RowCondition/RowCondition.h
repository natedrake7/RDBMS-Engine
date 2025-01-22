#pragma once
#include "../../../Database/Constants.h"

using namespace Constants;

namespace DatabaseEngine::StorageTypes {
    class Block;
}

class RowCondition {
    column_index_t columnIndex;
    object_t* condition;

    public:
        RowCondition();
        RowCondition(const void *condition, const block_size_t& conditionSize, const column_index_t &columnIndex);
        ~RowCondition();
        [[nodiscard]] const column_index_t& GetColumnIndex() const;
        bool operator==(const DatabaseEngine::StorageTypes::Block* block) const;
        bool operator!=(const DatabaseEngine::StorageTypes::Block* block) const;
};
