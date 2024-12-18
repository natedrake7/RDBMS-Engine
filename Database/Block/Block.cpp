#include "Block.h"
#include "../Database.h"
#include "../Column/Column.h"

Block::Block(const void* data, const block_size_t& size, const Column* column)
{
    this->size = size;
    this->column = column;
    this->SetData(data, size);
}

Block::Block(const Column* column)
{
    this->size = 0;
    this->column = column;
    this->data = nullptr;
    // this->SetData(data, size);
}

Block::Block(const Block *block)
{
    this->size = block->size;
    this->column = block->column;
    this->SetData(block->data, block->size);
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
}

void Block::SetData(const void* inputData, const block_size_t& inputSize)
{
    if (inputData == nullptr)
    {
        this->data = nullptr;
        this->size = 0;

        return;
    }

    this->data = new object_t[inputSize];
    memcpy(this->data, inputData, inputSize);

    this->size = inputSize;
}

object_t* Block::GetBlockData() const { return this->data; }

block_size_t Block::GetBlockSize() const { return this->size; }

const column_index_t& Block::GetColumnIndex() const { return this->column->GetColumnIndex(); }

const row_size_t& Block::GetColumnSize() const { return this->column->GetColumnSize(); }

const ColumnType & Block::GetColumnType() const { return this->column->GetColumnType(); }

void Block::PrintBlockData() const
{
    if(this->data == nullptr)
        return;

    const ColumnType columnType = this->column->GetColumnType();

    if (columnType == ColumnType::TinyInt)
        cout << *reinterpret_cast<const int8_t*>(this->data);
    else if (columnType == ColumnType::SmallInt)
        cout << *reinterpret_cast<const int16_t*>(this->data);
    else if(columnType == ColumnType::Int)
        cout << *reinterpret_cast<const int32_t*>(this->data);
    else if (columnType == ColumnType::BigInt)
        cout << *reinterpret_cast<const int64_t*>(this->data);
    else if(columnType == ColumnType::String)
    {
        // uint64_t hashKey;
        // memcpy(&hashKey, this->data, sizeof(uint64_t));
        // // const uint64_t hashKey = reinterpret_cast<const uint64_t>(this->data);
        // cout << db->GetStringByHashKey(hashKey);
    }

    cout << " || ";
}
