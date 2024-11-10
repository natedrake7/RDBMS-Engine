﻿#include "Block.h"

Block::Block(void* data, const size_t& size, Column* column)
{
    this->SetData(data, size);
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
    delete[] this->data;
    this->data = nullptr;
}

void Block::SetData(const void* inputData, const size_t& inputSize)
{
    // if(this->data)
    //     delete[] this->data;

    this->data = new unsigned char[inputSize];
    memcpy(this->data, inputData, inputSize);

    this->size = inputSize;
}

void* Block::GetBlockData() const { return this->data; }

size_t& Block::GetBlockSize(){ return this->size; }

size_t& Block::GetBlockIndex() const { return this->column->GetColumnIndex(); }

size_t& Block::GetColumnSize() const { return this->column->GetColumnSize(); }

void Block::PrintBlockData() const
{
    if(this->data == nullptr)
        return;

    const ColumnType columnType = this->column->GetColumnType();

    if(columnType == ColumnType::Integer)
        cout << *reinterpret_cast<const int*>(this->data) << " ";
    else if(columnType == ColumnType::String)
        cout << this->data << " ";

    cout << "|| ";
}
