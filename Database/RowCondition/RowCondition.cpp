#include "RowCondition.h"
#include <cstring>
#include "../Block/Block.h"

using namespace DatabaseEngine::StorageTypes;

RowCondition::RowCondition()
{
    this->columnIndex = 0;
    this->condition = nullptr;
}

RowCondition::RowCondition(const void* condition, const block_size_t& conditionSize, const column_index_t &columnIndex)
{
    this->columnIndex = columnIndex;
    this->condition = new object_t[conditionSize];
    memcpy(this->condition, condition, conditionSize);
}

RowCondition::~RowCondition()
{
    delete[] condition;
}

const column_index_t& RowCondition::GetColumnIndex() const { return columnIndex; }

bool RowCondition::operator==(const Block* block) const { return memcmp(this->condition, block->GetBlockData(), block->GetBlockSize()) == 0; }

bool RowCondition::operator!=(const Block* block) const { return memcmp(this->condition, block->GetBlockData(), block->GetBlockSize()) != 0; }




