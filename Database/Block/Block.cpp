#include "Block.h"
#include "../Database.h"
#include "../Column/Column.h"
#include "../Pages/LargeObject/LargeDataPage.h"

#include <cstring>
#include <iostream>

namespace DatabaseEngine::StorageTypes {
    
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

    bool Block::GetBool() const { return *reinterpret_cast<bool*>(this->data); }

    int8_t Block::GetTinyInt() const { return *reinterpret_cast<int8_t*>(this->data); }

    int16_t Block::GetSmallInt() const { return *reinterpret_cast<int16_t*>(this->data); }

    int32_t Block::GetInt() const { return *reinterpret_cast<int32_t*>(this->data); }

    int64_t Block::GetBigInt() const { return *reinterpret_cast<int64_t*>(this->data); }

    string Block::GetString() const { return std::string(reinterpret_cast<char*>(this->data), this->size);}

    u16string Block::GetUnicodeString() const { return std::u16string(reinterpret_cast<char16_t*>(this->data), this->size / 2); }

    DataTypes::DateTime Block::GetDateTime() const { return DataTypes::DateTime(*reinterpret_cast<time_t*>(this->data)); }

    Pages::DataObjectPointer Block::GeObjectPointer() const { return *reinterpret_cast<Pages::DataObjectPointer*>(this->data); }

    DataTypes::Decimal Block::GetDecimal() const { return DataTypes::Decimal(this->data, this->size); }

    const column_index_t& Block::GetColumnIndex() const { return this->column->GetColumnIndex(); }

    const row_size_t& Block::GetColumnSize() const { return this->column->GetColumnSize(); }

    const ColumnType & Block::GetColumnType() const { return this->column->GetColumnType(); }

    void Block::SetColumn(const Column* column) { this->column = column; };

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
}

bool operator!=(const DatabaseEngine::StorageTypes::Block &block, const Field &field){
    return !(block == field);
}

bool operator==(const DatabaseEngine::StorageTypes::Block &block, const Field &field){
    switch (block.GetColumnType()){
        case ColumnType::TinyInt:
            return block.GetTinyInt() == field.GetTinyInt();
        case ColumnType::SmallInt:
            return block.GetSmallInt() == field.GetSmallInt();
        case ColumnType::Int:
            return block.GetInt() == field.GetInt();
        case ColumnType::BigInt:
            return block.GetInt() == field.GetInt();
        case ColumnType::Decimal:
            return block.GetDecimal() == field.GetDecimal();
        case ColumnType::String:
            return block.GetString() == field.GetString();
        case ColumnType::UnicodeString:
            return block.GetUnicodeString() == field.GetUnicodeString();
        case ColumnType::Bool:
            return block.GetBool() == field.GetBool();
        case ColumnType::DateTime:
            return true;
            // return block.GetDateTime() == field.GetDateTime();
        case ColumnType::ColumnTypeCount:
        default:
            throw invalid_argument("invalid column type");
    }
}

bool operator>=(const DatabaseEngine::StorageTypes::Block &block, const Field &field){
    return !(block < field);
}

bool operator<=(const DatabaseEngine::StorageTypes::Block &block, const Field &field){
    return !(block > field);
}

bool operator>(const DatabaseEngine::StorageTypes::Block &block, const Field &field){
    switch (block.GetColumnType()){
        case ColumnType::TinyInt:
            return block.GetTinyInt() > field.GetTinyInt();
        case ColumnType::SmallInt:
            return block.GetSmallInt() > field.GetSmallInt();
        case ColumnType::Int:
            return block.GetInt() > field.GetInt();
        case ColumnType::BigInt:
            return block.GetInt() > field.GetInt();
        case ColumnType::Decimal:
            return block.GetDecimal() > field.GetDecimal();
        case ColumnType::String:
            return block.GetString() > field.GetString();
        case ColumnType::UnicodeString:
            return block.GetUnicodeString() > field.GetUnicodeString();
        case ColumnType::Bool:
            return block.GetBool() > field.GetBool();
        case ColumnType::DateTime:
            return true;
            // return block.GetDateTime() >  field.GetDateTime();
        case ColumnType::ColumnTypeCount:
        default:
            throw invalid_argument("invalid column type");
    }
}

bool operator<(const DatabaseEngine::StorageTypes::Block &block, const Field &field){
    return block <= field;
}