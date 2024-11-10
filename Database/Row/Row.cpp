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
    const size_t blockIndex = block->GetBlockIndex();

    this->ValidateOutOfBoundColumnIndex(blockIndex);

    this->data[blockIndex] = block;

    //update current block size
    const size_t newRowSize = *this->rowSize + this->data[blockIndex]->GetBlockSize();

    this->SetRowSize(newRowSize);
}

//copies data from inserted block to the existing one to keep the same memory positions
void Row::UpdateColumnData(Block* block)
{
    const size_t blockIndex = block->GetBlockIndex();

    this->ValidateOutOfBoundColumnIndex(blockIndex);

    delete this->data[blockIndex];

    this->data[blockIndex] = block;

    // this->data[hashIndex]->SetData(block->GetBlockData(), block->GetBlockSize());

    // delete block;
}

void Row::DeleteColumnData(const size_t& columnIndex)
{
    this->ValidateOutOfBoundColumnIndex(columnIndex);

    Block* block = this->data[columnIndex];

    const size_t currentRowSize = *this->rowSize - block->GetBlockSize();

    this->SetRowSize(currentRowSize);

    delete block;
}

void Row::DeleteColumn(const size_t& columnIndex)
{
    this->DeleteColumnData(columnIndex);

    this->data.erase(this->data.begin() + columnIndex);
}

void Row::ValidateOutOfBoundColumnIndex(const size_t& columnIndex) const
{
    if(columnIndex >= this->data.size())
        throw invalid_argument("Invalid Column specified");
}

const Block* Row::GetBlock(const size_t& index) const { return data[index]; }

void Row::SetRowSize(const size_t& rowSize) const { *this->rowSize = rowSize; }
