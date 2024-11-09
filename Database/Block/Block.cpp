#include "Block.h"

Block::Block(void* data, const size_t& size, Column* column)
{
    this->data = data;
    this->size = size;
    this->column = column;
}

Block::Block(Column* column)
{
    this->data = nullptr;
    this->size = 0;
    this->column = column;
}

Block::Block(const Block* block)
{
    this->SetData(block->data, block->size);
    this->size = block->size;
    this->column = block->column;
}

Block::~Block()
{
    free(this->data);
}

void Block::SetData(void* inputData, const size_t& inputSize)
{
    if(this->data != nullptr)
        free(this->data);

    this->data = malloc(inputSize);
    memcpy(this->data, inputData, inputSize);

    this->size = inputSize;
}

void* Block::GetBlockData() const { return this->data; }

size_t& Block::GetBlockSize(){ return this->size; }

size_t& Block::GetBlockColumnHashIndex() const { return this->column->GetColumnHashIndex(); }

size_t& Block::GetColumnSize() const { return this->column->GetColumnSize(); }

void Block::PrintBlockData() const
{
    if(this->data == nullptr)
        return;

    const string& columnType = this->column->GetColumnType();

    if(columnType == "int")
    {
        int* intValue = static_cast<int*>(this->data);
        cout << *intValue << " ";;
    }
    else if(columnType == "string")
    {
        string* stringValue = static_cast<string*>(this->data);
        cout << *stringValue << " ";
    }

    cout << " || ";
}
