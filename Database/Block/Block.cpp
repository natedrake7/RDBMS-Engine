#include "Block.h"

Block::Block(const void* data, const uint16_t& size, const Column* column)
{
    this->size = size;
    this->column = column;
    this->isLargeObject = false;
    this->SetData(data, size);
}

Block::Block(const Column* column)
{
    this->size = 0;
    this->column = column;
    this->data = nullptr;
    this->isLargeObject = false;
    // this->SetData(data, size);
}

Block::Block(const Block *block)
{
    this->size = block->size;
    this->column = block->column;
    this->SetData(block->data, block->size);
    this->isLargeObject = false;
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

void Block::SetData(const void* inputData, const uint16_t& inputSize, const bool& isLargeObject)
{
    // if(this->data)
    //     delete[] this->data;

    this->data = new unsigned char[inputSize];
    memcpy(this->data, inputData, inputSize);

    this->size = inputSize;

    this->isLargeObject = isLargeObject;
}

unsigned char* Block::GetBlockData() const { return this->data; }

const uint16_t& Block::GetBlockSize() const { return this->size; }

const uint16_t& Block::GetColumnIndex() const { return this->column->GetColumnIndex(); }

const uint32_t& Block::GetColumnSize() const { return this->column->GetColumnSize(); }

const ColumnType & Block::GetColumnType() const { return this->column->GetColumnType(); }

const bool & Block::IsLargeObject() const { return this->isLargeObject; }

void Block::SetIsLargeObject(const bool &isLargeObject) { this->isLargeObject = isLargeObject; }

void Block::PrintBlockData(const Database* db) const
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
