#include "Row.h"
#include "../../Systemic/DataStructures/BitMap/BitMap.h"
#include "../Table/Table.h"
#include "../Block/Block.h"
#include "../../Systemic/DataTypes/DateTime/DateTime.h"
#include "../../Systemic/DataTypes/Decimal/Decimal.h"
#include "../Column/Column.h"
#include "../Pages/LargeObject/LargeObjectPage.h"
#include <cstdint>
#include <cstring>
#include <ctime>
#include <iostream>
#include <stdexcept>
#include "../Pages/OverflowPage/OverflowPage.h"

using namespace Pages;
using namespace DataTypes;
using namespace ByteMaps;

namespace DatabaseEngine::StorageTypes {
    RowHeader::RowHeader()
    {
        this->rowSize = 0;
        this->maxRowSize = 0;
        this->nullBitMap = nullptr;
        this->largeObjectBitMap = nullptr;
        this->overflowBitMap = nullptr;
    }

    RowHeader::~RowHeader()
    {
        delete this->nullBitMap;
        delete this->largeObjectBitMap;
        delete this->overflowBitMap;
    }

    RowHeader & RowHeader::operator=(const RowHeader &otherHeader)
    {
        if (this == &otherHeader)
            return *this;

        this->rowSize = otherHeader.rowSize;
        this->maxRowSize = otherHeader.maxRowSize;
        this->nullBitMap = new ByteMaps::BitMap(otherHeader.nullBitMap);
        this->largeObjectBitMap = new ByteMaps::BitMap(otherHeader.largeObjectBitMap);
        this->overflowBitMap = new ByteMaps::BitMap(otherHeader.overflowBitMap);

        return *this;
    }

    CachedValue::CachedValue(){
        this->isMaterialized = false;
    }

    bool Row::IsBlockMaterialized(const int &indexPos)const {
        return this->cache.size() > indexPos && this->cache.at(indexPos).isMaterialized;
    }

    const Value & Row::GetMaterializedValue(const int &indexPos) const{ return this->cache.at(indexPos).value; }

    const Value & Row::Materialize(const int &indexPos) const{
        if (this->cache.empty())
            this->cache.resize(this->data.size());

        //only for joins
        if (this->cache.size() <= indexPos)
            this->cache.resize(indexPos + 1);

        const auto* dataBlock = this->data.at(indexPos);

        auto&[value, isMaterialized] = this->cache.at(indexPos);

        isMaterialized = true;

        if (this->header.largeObjectBitMap->Get(indexPos)) {
            const auto ptr = dataBlock->GetLargeObjectPointer();

            uint32_t objectSize;

            const auto* rawObject = this->GetLargeObjectValue(ptr, &objectSize);

            value = Value(rawObject, objectSize, dataBlock->GetColumnType());

            return value;
        }

        if (this->header.overflowBitMap->Get(indexPos)) {
            const auto ptr = this->data.at(indexPos)->GetOverflowPointer();
            const auto* overflowRow = this->GetOverflowValue(ptr);

            value = Value(overflowRow->object, overflowRow->objectSize, dataBlock->GetColumnType());

            return value;
        }

        //base scenario
        value = Value(dataBlock->GetBlockData(), dataBlock->GetBlockSize(), dataBlock->GetColumnType());
        return value;
    }

    Row::Row(const Table& table)
    {
        this->table = &table;

        const auto numberOfColumns = this->table->GetNumberOfColumns();

        this->data.resize(numberOfColumns);

        this->header.nullBitMap = new BitMap(numberOfColumns);
        this->header.largeObjectBitMap = new BitMap(numberOfColumns);
        this->header.overflowBitMap = new BitMap(numberOfColumns);

        this->isCopy = false;
    }

    Row::Row(const Table& table, const vector<Block*>& data, const BitMap* nullBitMap)
    {
        this->table = &table;

        this->header.nullBitMap = new BitMap(*nullBitMap);

        for (const auto& block : data)
            this->data.push_back(new Block(block));

        this->UpdateRowSize();
        this->header.maxRowSize = 0;
        this->isCopy = false;
    }

    Row::Row(const std::vector<const Column *> &columns){
        const auto& size = columns.size();

        this->table = nullptr;
        this->header.nullBitMap = new BitMap(size, true);
        this->header.overflowBitMap = new BitMap(size, false);
        this->header.largeObjectBitMap = new BitMap(size, false);

        for (const auto* column: columns)
            this->data.push_back(new Block(column));

        this->UpdateRowSize();
        this->header.maxRowSize = 0;
        this->isCopy = true;
    }

    Row::Row(const Row &copyRow)
    {
        this->table = copyRow.table;
        this->header = copyRow.header;

        for (const auto& block : copyRow.data)
            this->data.push_back(new Block(block));

        this->isCopy = true;
    }

    Row::Row(const Row *row){
        this->table = row->table;
        this->header = row->header;
        // this->cache = row->cache;

        for (const auto& block : row->data)
            this->data.push_back(new Block(block));

        this->isCopy = true;
    }

    Row & Row::operator=(const Row &copyRow)
    {
        if (this == &copyRow)
            return *this;

        this->header = copyRow.header;
        this->table = copyRow.table;

        this->data.clear();

        for (const auto& block : copyRow.data)
            this->data.push_back(new Block(block));

        this->isCopy = true;

        return *this;
    }

    Row::~Row()
    {
        for(const auto& block : this->data)
            delete block;
    }

    void Row::InsertColumnData(Block *block, const  column_index_t& columnIndex)
    {
        if (this->data[columnIndex] != nullptr)
        {
            this->header.rowSize -= this->data[columnIndex]->GetBlockSize();
            delete this->data[columnIndex];
        }

        this->data[columnIndex] = block;

        this->header.rowSize = this->GetTotalRowSize();
    }

    int Row::InsertNewColumn(Block* block)
    {
        this->header.nullBitMap->Set(this->data.size(), block->GetBlockData() == nullptr);
        this->header.largeObjectBitMap->Set(this->data.size(), false);
        this->header.overflowBitMap->Set(this->data.size(), false);

        this->data.push_back(block);

        const auto newSize = this->GetTotalRowSize();

        const auto diff = static_cast<int>(newSize) - static_cast<int>(this->header.rowSize);

        this->header.rowSize = newSize;

        return diff;
    }

    int Row::InsertNewColumnAtBeginning(Block *block){
        this->header.nullBitMap->Set(this->data.size(), block->GetBlockData() == nullptr);
        this->header.largeObjectBitMap->Set(this->data.size(), false);
        this->header.overflowBitMap->Set(this->data.size(), false);

        this->data.insert(this->data.begin(), block);

        const auto newSize = this->GetTotalRowSize();

        const auto diff = static_cast<int>(newSize) - static_cast<int>(this->header.rowSize);

        this->header.rowSize = newSize;

        return diff;
    }

    void Row::UpdateColumnData(Block *block)
    {
        const column_index_t& columnIndex = block->GetColumnIndex();
        
        this->header.rowSize -= this->data[columnIndex]->GetBlockSize();

        if (this->data[columnIndex] != nullptr)
            delete this->data[columnIndex];
        
        this->data[columnIndex] = block;
        this->header.rowSize += block->GetBlockSize();
    }

    const vector<Block *> & Row::GetData() const { return this->data; }

    vector<Block *> &Row::GetData() { return this->data; }

    void Row::PrintRow() const
    {
        for(size_t i = 0; i < this->data.size(); i++)
        {
            const DataType columnType = this->data[i]->GetColumnType();
            const object_t* blockData = this->data[i]->GetBlockData();
            const block_size_t& blockSize = this->data[i]->GetBlockSize();

            if(blockData == nullptr
                || blockSize == 0)
            {
                cout << "NULL";
                
                if(i == this->data.size() - 1)
                    cout << '\n';
                else
                    cout << " || ";

                continue;
            }

            switch (columnType) 
            {
                case DataType::TinyInt:
                {
                    std::cout << *reinterpret_cast<const int8_t*>(blockData);
                    break;
                }
                case DataType::SmallInt:
                {
                    std::cout << *reinterpret_cast<const int16_t*>(blockData);
                    break;
                }
                case DataType::Int:
                {
                    std::cout << *reinterpret_cast<const int32_t*>(blockData);
                    break;
                }
                case DataType::BigInt:
                {
                    std::cout << *reinterpret_cast<const int64_t*>(blockData);
                    break;
                }
                case DataType::Decimal:
                {
                    std::cout << Decimal(blockData, blockSize).ToString();
                    break;
                }
                case DataType::Guid: {
                    std::cout << this->data[i]->GetGuid();
                    break;
                }
                case DataType::String:
                {
                    std::cout.write(reinterpret_cast<const char*>(blockData), blockSize);
                    break;
                }
                case DataType::UnicodeString:
                {
                    std::wcout.write(reinterpret_cast<const wchar_t*>(blockData), blockSize / sizeof(char16_t));
                    break;
                }
                case DataType::Bool:
                {
                    std::cout << (*reinterpret_cast<const bool*>(blockData) ? "true" : "false");
                    break;
                }
                case DataType::DateTime:
                {
                    auto time = *reinterpret_cast<const time_t*>(blockData);
                    std::cout << DateTime(time);
                    break;
                }
                case DataType::Invalid:
                default:
                    throw invalid_argument("Row::PrintRow Invalid Column specified");
            }

            if(i == this->data.size() - 1)
                cout << '\n';
            else
                cout << " || ";
        }
    }

    const row_size_t& Row::GetRowSize() const { return this->header.rowSize; }

    vector<column_index_t> Row::GetLargeBlocks()const
    {
        vector<column_index_t> largeBlocksIndexes;
        for(const auto& block : this->data)
        {
            const column_index_t& blockIndex = block->GetColumnIndex();
            const block_size_t& blockSize = block->GetBlockSize();

            if(blockSize >= LARGE_DATA_OBJECT_SIZE)
                largeBlocksIndexes.push_back(blockIndex);
        }

        return largeBlocksIndexes;
    }

    void Row::UpdateRowSize()
    {
        this->header.rowSize = this->GetTotalRowSize();
    }

    unsigned char* Row::GetLargeObjectValue(const DataObjectPointer &objectPointer, uint32_t* objectSize) const
    {
        auto page = this->table->GetLargeDataPage(objectPointer.pageId);

        const LargeDataObject* object = page->GetObject();

        uint32_t currentObjectSize = object->objectSize;

        auto* buffer = new unsigned char[currentObjectSize];

        memcpy(buffer, object->object, currentObjectSize);

        while (object->nextPageId != 0)
        {
            const page_id_t nextPageId = object->nextPageId;

            page = this->table->GetLargeDataPage(nextPageId);
            object = page->GetObject();

            const page_size_t nextObjectSize = object->objectSize;

            unsigned char* prevValue = buffer;

            buffer = new unsigned char[currentObjectSize + nextObjectSize];

            memcpy(buffer, prevValue, currentObjectSize);
            delete[] prevValue;

            memcpy(buffer + currentObjectSize, object->object, nextObjectSize);
        
            currentObjectSize += object->objectSize;
        }

        *objectSize = currentObjectSize;
        
        return buffer;
    }

    Block * Row::GetLargeObject(const Pages::DataObjectPointer &objectPointer, const Column *column)const{
        auto page = this->table->GetLargeDataPage(objectPointer.pageId);

        const auto* object = page->GetObject();

        uint32_t currentObjectSize = object->objectSize;

        auto* buffer = new unsigned char[currentObjectSize];

        memcpy(buffer, object->object, currentObjectSize);

        while (object->nextPageId != 0)
        {
            const page_id_t nextPageId = object->nextPageId;

            page = this->table->GetLargeDataPage(nextPageId);
            object = page->GetObject();

            const page_size_t nextObjectSize = object->objectSize;

            unsigned char* prevValue = buffer;

            buffer = new unsigned char[currentObjectSize + nextObjectSize];

            memcpy(buffer, prevValue, currentObjectSize);
            delete[] prevValue;

            memcpy(buffer + currentObjectSize, object->object, nextObjectSize);

            currentObjectSize += object->objectSize;
        }

        return new Block(buffer, currentObjectSize, column);
    }

    Pages::OverflowRow* Row::GetOverflowValue(const Pages::OverflowPointer & objectPointer) const{
        const auto page = this->table->GetOverflowPage(objectPointer.pageId);
        return page->GetObject(objectPointer.index);
    }

    void Row::SetNullBitMapValue(const bit_map_pos_t &position, const bool &value) const { this->header.nullBitMap->Set(position, value); }

    void Row::SetOverflowBitMapValue(const bit_map_pos_t & position, const bool & value) const{ this->header.overflowBitMap->Set(position, value); }

    bool Row::GetNullBitMapValue(const bit_map_pos_t &position) const { return this->header.nullBitMap->Get(position); }

    bool Row::GetOverflowBitMapValue(const bit_map_pos_t & position) const{ return this->header.overflowBitMap->Get(position); }

    RowHeader* Row::GetHeader() { return &this->header; }

    row_size_t Row::GetTotalRowSize() const
    {
        row_size_t currentRowSize = this->GetRowHeaderSize();

        for(const auto& block: this->data) {
            if (block == nullptr)
                continue;

            currentRowSize += block->GetBlockSize() + sizeof(block_size_t);
        }

        return currentRowSize;
    }

    row_header_size_t Row::GetRowHeaderSize() const
    {
        row_header_size_t rowHeaderSize = sizeof(row_size_t);
        rowHeaderSize += sizeof(size_t);

        rowHeaderSize += this->header.nullBitMap->GetSizeInBytes();

        if (this->header.largeObjectBitMap != nullptr)
            rowHeaderSize += this->header.largeObjectBitMap->GetSizeInBytes();

        if (this->header.overflowBitMap != nullptr)
            rowHeaderSize += this->header.overflowBitMap->GetSizeInBytes();

        return rowHeaderSize;
    }

    Errors::RuntimeStatus Row::Update( const vector<Value> & updates, int& diff){
        const auto prevRowSize = this->GetRowSize();

        for (const auto & value : updates){
            const column_index_t &associatedColumnIndex = value.GetColumnIndex();

            auto *block = this->data.at(associatedColumnIndex);

            if (value.GetIsNull())
            {
              block->SetData(nullptr, 0);
              this->SetNullBitMapValue(associatedColumnIndex, true);
              continue;
            }

            if (block->GetIsNull())
                this->SetNullBitMapValue(associatedColumnIndex, false);

            const auto result = block->SetData(value);
            if (result.code != Errors::RuntimeError::Ok)
                return result;
        }

        this->UpdateRowSize();
        diff = static_cast<int>(this->header.rowSize - prevRowSize);

        return {};
    }

    Errors::RuntimeStatus Row::Update(const std::vector<QueryPipeline::Statements::UpdateColumn*> &updates, int& diff){
        const auto prevRowSize = this->GetRowSize();

        for (const auto & update : updates)
        {
            const auto value = update->value->Evaluate(this);

            const column_index_t &associatedColumnIndex = update->name.index;

            auto *block = this->data.at(associatedColumnIndex);

            if (value.GetIsNull())
            {
                block->SetData(nullptr, 0);
                this->SetNullBitMapValue(associatedColumnIndex, true);
                continue;
            }

            if (block->GetIsNull())
                this->SetNullBitMapValue(associatedColumnIndex, false);

            const auto result = block->SetData(value);
            if (result.code != Errors::RuntimeError::Ok)
                return result;
        }

        this->UpdateRowSize();
        diff = static_cast<int>(this->header.rowSize - prevRowSize);

        return {};
    }

    Block* Row::FindLargestVariableLengthColumn() const{
        Block* largestColumn = nullptr;
        int size = 0;

        for(const auto& block : this->data){

            const auto& columnType = block->GetColumnType();
            const auto& columnIndex = block->GetColumnIndex();

            if(columnType != DataType::String
              && columnType != DataType::UnicodeString
              && this->header.largeObjectBitMap->Get(columnIndex))
              continue;

            if(block->GetBlockSize() <= size)
              continue;

            largestColumn = block;
            size = block->GetBlockSize();
        }

      return largestColumn;
    }

  vector<Block*> Row::GetBlockCopies() const{

      vector<Block*> copyBlocks;
      for (const auto &block : this->data)
      {
        auto *blockCopy = new Block(block);
        if (this->header.largeObjectBitMap->Get(block->GetColumnIndex()))
        {
            DataObjectPointer objectPointer;
            memcpy(&objectPointer, block->GetBlockData(), sizeof(DataObjectPointer));

            uint32_t objectSize;
            unsigned char *largeValue = this->GetLargeObjectValue(objectPointer, &objectSize);
            blockCopy->SetData(largeValue, objectSize);

            delete[] largeValue;
        }
        else if(this->header.overflowBitMap->Get(block->GetColumnIndex())){
            OverflowPointer overflowPointer;
           memcpy(&overflowPointer, block->GetBlockData(), sizeof(OverflowPointer));

          const auto* largeValue = this->GetOverflowValue(overflowPointer);
          blockCopy->SetData(largeValue->object, largeValue->objectSize);
        }

        copyBlocks.push_back(blockCopy);
      }

    return copyBlocks;
  }

    const Value& Row::GetColumnByIndex(const int &indexPos) const{
        return (this->IsBlockMaterialized(indexPos))
                ? this->GetMaterializedValue(indexPos)
                : this->Materialize(indexPos);
    }

    void Row::Serialize(std::vector<char>* buffer, uint32_t& pos)const{
        memcpy(buffer->data() + pos, &this->header.rowSize, sizeof(row_size_t));
        pos += sizeof(row_size_t);
        memcpy(buffer->data() + pos, &this->header.maxRowSize, sizeof(size_t));
        pos += sizeof(size_t);

        this->header.nullBitMap->WriteDataToFile(buffer, pos);
        this->header.largeObjectBitMap->WriteDataToFile(buffer, pos);
        this->header.overflowBitMap->WriteDataToFile(buffer, pos);

        column_index_t columnIndex = 0;
        for (const auto &block : this->data)
        {
            if (this->header.nullBitMap->Get(columnIndex))
            {
                columnIndex++;
                continue;
            }

            const auto& dataSize = block->GetBlockSize();

            memcpy(buffer->data() + pos, &dataSize, sizeof(block_size_t));
            pos += sizeof(block_size_t);

            memcpy(buffer->data() + pos, block->GetBlockData(), dataSize);
            pos += dataSize;

            columnIndex++;
        }
    }

    void Row::Deserialize(const std::vector<char> *buffer, uint32_t &pos){
        auto *rowHeader = this->GetHeader();

        memcpy(&rowHeader->rowSize, buffer->data() + pos, sizeof(row_size_t));
        pos += sizeof(row_size_t);

        memcpy(&rowHeader->maxRowSize, buffer->data() + pos, sizeof(size_t));
        pos += sizeof(size_t);

        rowHeader->nullBitMap->GetDataFromFile(*buffer, pos);
        rowHeader->largeObjectBitMap->GetDataFromFile(*buffer, pos);
        rowHeader->overflowBitMap->GetDataFromFile(*buffer, pos);

        const auto& columns = this->table->GetColumns();

        for (int j = 0; j < columns.size(); j++)
        {
            if (rowHeader->nullBitMap->Get(j))
            {
                auto *block = new StorageTypes::Block(columns[j]);
                this->InsertColumnData(block, j);

                continue;
            }

            block_size_t bytesToRead;

            memcpy(&bytesToRead, buffer->data() + pos, sizeof(block_size_t));
            pos += sizeof(block_size_t);

            auto *bytes = new unsigned char[bytesToRead];
            memcpy(bytes, buffer->data() + pos, bytesToRead);
            pos += bytesToRead;

            auto *block = new StorageTypes::Block(bytes, bytesToRead, columns[j]);

            this->InsertColumnData(block, j);

            delete[] bytes;
        }
    }

    Row* Row::Join(const Row *row)const{
        auto* joinedRow = new Row(this);

        for (const auto* block : row->GetData())
            joinedRow->InsertNewColumn(new Block(block));

        return joinedRow;
    }

    Row* Row::LeftJoin(const std::vector<const Column*>& innerTableColumns) const{
        auto* joinedRow = new Row(this);

        for (const auto& column : innerTableColumns)
            joinedRow->InsertNewColumn(new Block(column));

        return joinedRow;

    }

    Row * Row::RightJoin(const std::vector<const Column *> &innerTableColumns) const{
        auto* joinedRow = new Row(innerTableColumns);

        for (const auto& block : this->data)
            joinedRow->InsertNewColumn(new Block(block));

        return joinedRow;
    }

    const bool & Row::IsCopy() const{ return this->isCopy; }

    std::ostream & operator<<(std::ostream &os, const Row &row){

        for(size_t i = 0; i < row.data.size(); i++)
        {
            const DataType columnType = row.data[i]->GetColumnType();
            const object_t* blockData = row.data[i]->GetBlockData();
            const block_size_t& blockSize = row.data[i]->GetBlockSize();

            if(blockData == nullptr)
            {
                cout << "NULL";

                if(i == row.data.size() - 1)
                    cout << '\n';
                else
                    cout << " || ";

                continue;
            }

            switch (columnType)
            {
                case DataType::TinyInt:
                {
                    cout << *reinterpret_cast<const int8_t*>(blockData);
                    break;
                }
                case DataType::SmallInt:
                {
                    cout << *reinterpret_cast<const int16_t*>(blockData);
                    break;
                }
                case DataType::Int:
                {
                    cout << *reinterpret_cast<const int32_t*>(blockData);
                    break;
                }
                case DataType::BigInt:
                {
                    cout << *reinterpret_cast<const int64_t*>(blockData);
                    break;
                }
                case DataType::Decimal:
                {
                    cout << Decimal(blockData, blockSize).ToString();
                    break;
                }
                case DataType::Guid: {
                    cout << row.data[i]->GetGuid();
                    break;
                }
                case DataType::String:
                {
                    cout.write(reinterpret_cast<const char*>(blockData), blockSize);
                    break;
                }
                case DataType::UnicodeString:
                {
                    wcout.write(reinterpret_cast<const wchar_t*>(blockData), blockSize / sizeof(char16_t));
                    break;
                }
                case DataType::Bool:
                {
                    cout << (*reinterpret_cast<const bool*>(blockData) ? "true" : "false");
                    break;
                }
                case DataType::DateTime:
                {
                    cout << DateTime(reinterpret_cast<time_t>(blockData)).ToString();
                    break;
                }
                case DataType::Invalid:
                default:
                    throw invalid_argument("Row::PrintRow Invalid Column specified");
            }

            if(i == row.data.size() - 1)
                cout << '\n';
            else
                cout << " || ";
        }

        return os;
    }

}