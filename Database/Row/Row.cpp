#include "Row.h"

Row::Row()
{
    this->rowSize = new size_t(0);
    this->maxRowSize = new size_t(0);
    this->data = nullptr;
}

Row::Row(const size_t& numberOfColumns)
{
    this->rowSize = new size_t(0);
    this->maxRowSize = new size_t(0);
    this->data = new vector<Block*>(numberOfColumns);
}

Row::Row(const Row* row)
{
    this->rowSize = new size_t(*row->rowSize);
    this->maxRowSize = new size_t(*row->maxRowSize);
    this->data = new vector<Block*>(*row->data);

    delete row;
}

Row::~Row()
{
    for (const auto& columnData : *this->data)
        delete columnData;

    delete this->data;
    delete this->rowSize;
    delete this->maxRowSize;
}

vector<Block*>& Row::GetRowData() const { return *this->data; }

void Row::InsertColumnData(const Block* block) const
{
    const size_t hashIndex = block->GetBlockColumnHashIndex();

    this->ValidateOutOfBoundColumnHashIndex(hashIndex);

    Block* vectorBlock = this->GetBlock(hashIndex);

    delete vectorBlock;

    vectorBlock = new Block(block);

    delete block;

    //update current block size
    const size_t newRowSize = *this->rowSize + vectorBlock->GetBlockSize();
    this->SetRowSize(newRowSize);
}

//copies data from inserted block to the existing one to keep the same memory positions
void Row::UpdateColumnData(Block* block) const
{
    const size_t hashIndex = block->GetBlockColumnHashIndex();

    this->ValidateOutOfBoundColumnHashIndex(hashIndex);

    this->GetBlock(hashIndex)->SetData(block->GetBlockData(), block->GetBlockSize());

    delete block;
}

void Row::DeleteColumnData(const size_t& columnHashIndex) const
{
    this->ValidateOutOfBoundColumnHashIndex(columnHashIndex);

    Block* block = this->GetBlock(columnHashIndex);

    const size_t currentRowSize = *this->rowSize - block->GetBlockSize();
    const size_t currentMaxRowSize = *this->maxRowSize - block->GetColumnSize();

    this->SetRowSize(currentRowSize);
    this->SetMaxRowSize(currentMaxRowSize);

    delete this->GetBlock(columnHashIndex);
}

void Row::DeleteColumn(const size_t& columnHashIndex)
{
    this->DeleteColumnData(columnHashIndex);

    this->data->erase(this->data->begin() + columnHashIndex);
}

void Row::ValidateOutOfBoundColumnHashIndex(const size_t& hashIndex) const
{
    if(hashIndex >= this->data->size())
        throw invalid_argument("Invalid Column specified");
}

Block* Row::GetBlock(const size_t& index) const { return data->at(index); }

void Row::SetRowSize(const size_t& rowSize) const { *this->rowSize = rowSize; }

void Row::SetMaxRowSize(const size_t& maxRowSize) const { *this->maxRowSize = maxRowSize; }