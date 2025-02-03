#include "Row.h"
#include "../../AdditionalLibraries/BitMap/BitMap.h"
#include "../Table/Table.h"
#include "../Block/Block.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/DateTime/DateTime.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Decimal/Decimal.h"
#include "../Column/Column.h"
#include "../Pages/LargeObject/LargeDataPage.h"
#include <cstdint>
#include <ctime>
#include <stdexcept>

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
    }

    RowHeader::~RowHeader()
    {
        delete this->nullBitMap;
        delete this->largeObjectBitMap;
    }

    RowHeader & RowHeader::operator=(const RowHeader &otherHeader)
    {
        if (this == &otherHeader)
            return *this;

        this->rowSize = otherHeader.rowSize;
        this->maxRowSize = otherHeader.maxRowSize;
        this->nullBitMap = new BitMap(*otherHeader.nullBitMap);
        // this->largeObjectBitMap = new BitMap(*otherHeader.largeObjectBitMap);

        return *this;
    }

    Row::Row(const Table& table)
    {
        this->table = &table;

        const auto numberOfColumns = this->table->GetNumberOfColumns();
        
        this->data.resize(numberOfColumns);

        this->header.nullBitMap = new BitMap(numberOfColumns);
        this->header.largeObjectBitMap = new BitMap(numberOfColumns);
    }

    Row::Row(const Table& table, const vector<Block*>& data, const BitMap* nullBitMap)
    {
        this->table = &table;

        this->header.nullBitMap = new BitMap(*nullBitMap);
        
        for (const auto& block : data)
            this->data.push_back(new Block(block));
        
        this->UpdateRowSize();
        this->header.maxRowSize = 0;
    }

    Row::Row(const Row &copyRow)
    {
        this->table = copyRow.table;
        this->header = copyRow.header;

        for (const auto& block : copyRow.data)
            this->data.push_back(new Block(block));
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

        this->header.rowSize += block->GetBlockSize();
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

    void Row::PrintRow() const
    {
        for(size_t i = 0; i < this->data.size(); i++)
        {
            const ColumnType columnType = this->data[i]->GetColumnType();
            const object_t* blockData = this->data[i]->GetBlockData();
            const block_size_t& blockSize = this->data[i]->GetBlockSize();

            if(blockData == nullptr)
            {
                cout << "NULL";
                continue;
            }

            switch (columnType) 
            {
                case ColumnType::TinyInt:
                {
                    cout << *reinterpret_cast<const int8_t*>(blockData);
                    break;
                }
                case ColumnType::SmallInt:
                {
                    cout << *reinterpret_cast<const int16_t*>(blockData);
                    break;
                }
                case ColumnType::Int:
                {
                    cout << *reinterpret_cast<const int32_t*>(blockData);
                    break;
                }
                case ColumnType::BigInt:
                {
                    cout << *reinterpret_cast<const int64_t*>(blockData);
                    break;
                }
                case ColumnType::Decimal:
                {
                    cout << Decimal(blockData, blockSize).ToString();
                    break;
                }
                case ColumnType::String:
                {
                    cout.write(reinterpret_cast<const char*>(blockData), blockSize);
                    break;
                }
                case ColumnType::UnicodeString:
                {
                    wcout.write(reinterpret_cast<const wchar_t*>(blockData), blockSize / sizeof(char16_t));
                    break;
                }
                case ColumnType::Bool:
                {
                    cout << *reinterpret_cast<const bool*>(blockData);
                    break;
                }
                case ColumnType::DateTime:
                {
                    cout << DateTime(reinterpret_cast<time_t>(blockData)).ToString();
                    break;
                }
                case ColumnType::ColumnTypeCount:
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

    vector<column_index_t> Row::GetLargeBlocks()
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
        this->header.rowSize = 0;
        for (const auto& block : this->data)
            this->header.rowSize += block->GetBlockSize();
    }

    unsigned char* Row::GetLargeObjectValue(const DataObjectPointer &objectPointer, uint32_t* objectSize) const
    {
        LargeDataPage* page = this->table->GetLargeDataPage(objectPointer.pageId);

        DataObject* object = page->GetObject(objectPointer.objectIndex);

        uint32_t currentObjectSize = object->objectSize;

        unsigned char* buffer = new unsigned char[currentObjectSize];

        memcpy(buffer, object->object, currentObjectSize);

        while (object->nextPageId != 0)
        {
            const page_id_t nextPageId = object->nextPageId;
            const large_page_index_t nextObjectIndex = object->nextObjectIndex;
            
            page = this->table->GetLargeDataPage(nextPageId);
            object = page->GetObject(nextObjectIndex);

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

    void Row::SetNullBitMapValue(const bit_map_pos_t &position, const bool &value) { this->header.nullBitMap->Set(position, value); }

    bool Row::GetNullBitMapValue(const bit_map_pos_t &position) const { return this->header.nullBitMap->Get(position); }

    RowHeader* Row::GetHeader() { return &this->header; }

    row_size_t Row::GetTotalRowSize() const
    {
        row_size_t currentRowSize = this->GetRowHeaderSize();
        currentRowSize += this->table->GetNumberOfColumns() * sizeof(block_size_t); //decrease by the null blocks here

        for(const auto& block: this->data)
            currentRowSize += block->GetBlockSize();
        //currentRowSize += this->header.rowSize;
        
        return currentRowSize;
    }

    row_header_size_t Row::GetRowHeaderSize() const
    {
        row_header_size_t rowHeaderSize = sizeof(row_size_t);
        rowHeaderSize += sizeof(size_t);
        rowHeaderSize += this->header.nullBitMap->GetSizeInBytes();
        rowHeaderSize += this->header.largeObjectBitMap->GetSizeInBytes();

        return rowHeaderSize;
    }
}