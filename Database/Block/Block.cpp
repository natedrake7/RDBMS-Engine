#include "Block.h"

Block::Block(const void* data, const size_t& size, const Column* column)
{
    this->size = new size_t(size);
    this->column = column;
    this->SetData(data, size);
}

Block::Block(const Column* column)
{
    this->size = new size_t(0);
    this->column = column;
    this->data = nullptr;
    // this->SetData(data, size);
}

// Block::Block(Column* column)
// {
//     this->data = nullptr;
//     this->size = new size_t(0);
//     this->column = column;
// }

Block::~Block()
{
    delete[] this->data;

    this->data = nullptr;

    delete this->size;
}

void Block::SetData(const void* inputData,const size_t& inputSize)
{
    // if(this->data)
    //     delete[] this->data;

    this->data = new unsigned char[inputSize];
    memcpy(this->data, inputData, inputSize);

    *this->size = inputSize;
}

unsigned char* Block::GetBlockData() const { return this->data; }

size_t& Block::GetBlockSize() const { return *this->size; }

const size_t& Block::GetColumnIndex() const { return this->column->GetColumnIndex(); }

const size_t& Block::GetColumnSize() const { return this->column->GetColumnSize(); }

void Block::PrintBlockData() const
{
    if(this->data == nullptr)
        return;

    const ColumnType columnType = this->column->GetColumnType();

    if(columnType == ColumnType::Integer)
        cout << *reinterpret_cast<const int*>(this->data) << " ";
    else if(columnType == ColumnType::String)
    {
        cout << this->data << " ";
    }

    cout << "|| ";
}
