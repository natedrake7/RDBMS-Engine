#include "Block.h"
#include "../Database.h"
#include "../../AdditionalLibraries/Converter/Converter.h"

#include <cstring>
#include <iostream>

namespace DatabaseEngine::StorageTypes {
    
    Block::Block(const void* data, const block_size_t& size, const Column* column)
    {
        this->size = size;
        this->column = column;
        this->data = nullptr;
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
        this->data = nullptr;
        this->SetData(block->data, block->size);
    }

    Block::~Block()
    {
        delete this->data;
        this->data = nullptr;
    }

    void Block::SetData(const void* inputData, const block_size_t& inputSize)
    {
        delete this->data;
        this->data = nullptr;

        if (inputData == nullptr)
        {
            this->size = 0;
            return;
        }

        this->data = new object_t[inputSize];
        memcpy(this->data, inputData, inputSize);

        this->size = inputSize;
    }

    void Block::SetData(const Value &value){
        delete this->data;
        this->data = nullptr;

        if (value.GetIsNull()) {
            this->size = 0;
            return;
        }
        this->SetDataByType(value);
    }

    void Block::SetDataByType(const Value &value){
        switch (this->GetColumnType()) {
            case DataType::TinyInt:
                this->CopyToBuffer(value.GetTinyInt());
                return;
            case DataType::SmallInt:
                this->CopyToBuffer(value.GetSmallInt());
                return;
            case DataType::Int:
                this->CopyToBuffer(value.GetInt());
                return;
            case DataType::BigInt:
                this->CopyToBuffer(value.GetBigInt());
                return;
            case DataType::Decimal:
                this->CopyToBuffer(value.GetDecimal());
                return;
            case DataType::String:
                this->CopyToBuffer(value.GetString());
                return;
            case DataType::UnicodeString:
                this->CopyToBuffer(value.GetUnicodeString());
                return;
            case DataType::Bool:
                this->CopyToBuffer(value.GetBool());
                return;
            case DataType::DateTime:
                this->CopyToBuffer(value.GetDateTime());
                return;
            case DataType::Guid:
                this->CopyToBuffer(value.GetGuid());
                return;
            case DataType::RowIdentifier:
            case DataType::Invalid:
            default:
                throw std::runtime_error("Invalid Datatype for column");
        }
    }

    object_t* Block::GetBlockData() const { return this->data; }

    block_size_t Block::GetBlockSize() const { return this->size; }

    bool Block::GetBool() const { return *reinterpret_cast<bool*>(this->data); }

    int8_t Block::GetTinyInt() const { return *reinterpret_cast<int8_t*>(this->data); }

    int16_t Block::GetSmallInt() const { return *reinterpret_cast<int16_t*>(this->data); }

    int32_t Block::GetInt() const { return *reinterpret_cast<int32_t*>(this->data); }

    int64_t Block::GetBigInt() const { return *reinterpret_cast<int64_t*>(this->data); }

    string Block::GetString() const { return { reinterpret_cast<char*>(this->data), static_cast<size_t>(this->size) };}

    u16string Block::GetUnicodeString() const { return { reinterpret_cast<char16_t*>(this->data), static_cast<size_t>(this->size / 2) }; }

    DataTypes::DateTime Block::GetDateTime() const { return DataTypes::DateTime(*reinterpret_cast<time_t*>(this->data)); }

    DataTypes::Guid Block::GetGuid() const{  return { this->data, this->size }; }

    Pages::DataObjectPointer Block::GeObjectPointer() const { return *reinterpret_cast<Pages::DataObjectPointer*>(this->data); }

    Pages::OverflowPointer Block::GetOverflowPointer() const { return *reinterpret_cast<Pages::OverflowPointer*>(this->data); }

    DataTypes::Decimal Block::GetDecimal() const { return DataTypes::Decimal(this->data, this->size); }

    const column_index_t& Block::GetColumnIndex() const { return this->column->GetColumnIndex(); }

    const row_size_t& Block::GetColumnSize() const { return this->column->GetColumnSize(); }

    const DataType & Block::GetColumnType() const { return this->column->GetColumnType(); }


    const Column * Block::GetColumn() const{ return this->column; }

    bool Block::GetIsNull() const{ return this->data == nullptr; }

    void Block::SetColumn(const Column* column) { this->column = column; };

    void Block::PrintBlockData() const
    {
        if(this->data == nullptr)
            return;

        const DataType columnType = this->column->GetColumnType();

        if (columnType == DataType::TinyInt)
            cout << *reinterpret_cast<const int8_t*>(this->data);
        else if (columnType == DataType::SmallInt)
            cout << *reinterpret_cast<const int16_t*>(this->data);
        else if(columnType == DataType::Int)
            cout << *reinterpret_cast<const int32_t*>(this->data);
        else if (columnType == DataType::BigInt)
            cout << *reinterpret_cast<const int64_t*>(this->data);
        else if(columnType == DataType::String)
        {
            // uint64_t hashKey;
            // memcpy(&hashKey, this->data, sizeof(uint64_t));
            // // const uint64_t hashKey = reinterpret_cast<const uint64_t>(this->data);
            // cout << db->GetStringByHashKey(hashKey);
        }

        cout << " || ";
    }
}

bool operator!=(const DatabaseEngine::StorageTypes::Block &block, const Value &field){
    return !(block == field);
}

//Tiny Int Comparison -> string, bool, unicodeString, decimal

bool operator==(const DatabaseEngine::StorageTypes::Block &block, const Value &field){
    if (field.GetIsNull()
        || block.GetBlockData() == nullptr)
        return block.GetBlockData() == nullptr;

    switch (block.GetColumnType()){
        case DataType::TinyInt:
            return block.GetTinyInt() == field.GetTinyInt();
        case DataType::SmallInt:
            return block.GetSmallInt() == field.GetSmallInt();
        case DataType::Int:
            return block.GetInt() == field.GetInt();
        case DataType::BigInt:
            return block.GetBigInt() == field.GetBigInt();
        case DataType::Decimal:
            return block.GetDecimal() == field.GetDecimal();
        case DataType::String:
            return block.GetString() == field.GetString();
        case DataType::UnicodeString:
            return block.GetUnicodeString() == field.GetUnicodeString();
        case DataType::Bool:
            return block.GetBool() == field.GetBool();
        case DataType::DateTime:
            return block.GetDateTime() == field.GetDateTime();
        case DataType::Guid:
            return block.GetGuid() == field.GetGuid();
        case DataType::Invalid:
        default:
            throw invalid_argument("invalid column type");
    }
}

bool operator>=(const DatabaseEngine::StorageTypes::Block &block, const Value &field){
    return !(block < field);
}

bool operator<=(const DatabaseEngine::StorageTypes::Block &block, const Value &field){
    return !(block > field);
}

bool operator>(const DatabaseEngine::StorageTypes::Block &block, const Value &field){
    if (field.GetIsNull()
        || block.GetBlockData() == nullptr)
        return block.GetBlockData() != nullptr;

    switch (block.GetColumnType()){
        case DataType::TinyInt:
            return block.GetTinyInt() > field.GetTinyInt();
        case DataType::SmallInt:
            return block.GetSmallInt() > field.GetSmallInt();
        case DataType::Int:
            return block.GetInt() > field.GetInt();
        case DataType::BigInt:
            return block.GetBigInt() > field.GetBigInt();
        case DataType::Decimal:
            return block.GetDecimal() > field.GetDecimal();
        case DataType::String:
            return block.GetString() > field.GetString();
        case DataType::UnicodeString:
            return block.GetUnicodeString() > field.GetUnicodeString();
        case DataType::Bool:
            return block.GetBool() > field.GetBool();
        case DataType::DateTime:
            return block.GetDateTime() >  field.GetDateTime();
        case DataType::Guid:
            return block.GetGuid() > field.GetGuid();
        case DataType::Invalid:
        default:
            throw invalid_argument("invalid column type");
    }
}

bool operator<(const DatabaseEngine::StorageTypes::Block &block, const Value &field){
    return block <= field;
}