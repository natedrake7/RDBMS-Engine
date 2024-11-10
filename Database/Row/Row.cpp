#include "Row.h"

Row::Row(const Table& table)
{
    this->rowSize = new size_t(0);
    this->table = &table;
    this->data.resize(this->table->GetNumberOfColumns());
}

Row::~Row()
{
    for (const auto& columnData : this->data)
        delete columnData;

    delete this->rowSize;
}

vector<Block*>& Row::GetRowData() { return this->data; }

void Row::InsertColumnData(Block* block)
{
    const size_t hashIndex = block->GetBlockIndex();

    this->ValidateOutOfBoundColumnHashIndex(hashIndex);

    this->data[hashIndex] = block;

    //update current block size
    const size_t newRowSize = *this->rowSize + this->data[hashIndex]->GetBlockSize();

    this->SetRowSize(newRowSize);
}

//copies data from inserted block to the existing one to keep the same memory positions
void Row::UpdateColumnData(Block* block)
{
    const size_t hashIndex = block->GetBlockIndex();

    this->ValidateOutOfBoundColumnHashIndex(hashIndex);

    delete this->data[hashIndex];

    this->data[hashIndex] = block;

    // this->data[hashIndex]->SetData(block->GetBlockData(), block->GetBlockSize());

    // delete block;
}

void Row::DeleteColumnData(const size_t& columnHashIndex)
{
    this->ValidateOutOfBoundColumnHashIndex(columnHashIndex);

    Block* block = this->GetBlock(columnHashIndex);

    const size_t currentRowSize = *this->rowSize - block->GetBlockSize();

    this->SetRowSize(currentRowSize);

    delete this->GetBlock(columnHashIndex);
}

void Row::DeleteColumn(const size_t& columnHashIndex)
{
    this->DeleteColumnData(columnHashIndex);

    this->data.erase(this->data.begin() + columnHashIndex);
}

void Row::ValidateOutOfBoundColumnHashIndex(const size_t& hashIndex) const
{
    if(hashIndex >= this->data.size())
        throw invalid_argument("Invalid Column specified");
}

Block* Row::GetBlock(const size_t& index) const { return data[index]; }

void Row::SetRowSize(const size_t& rowSize) const { *this->rowSize = rowSize; }