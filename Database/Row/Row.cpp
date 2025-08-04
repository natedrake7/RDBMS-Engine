#include "Row.h"
#include "../../AdditionalLibraries/BitMap/BitMap.h"
#include "../Table/Table.h"
#include "../Block/Block.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/DateTime/DateTime.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Decimal/Decimal.h"
#include "../Column/Column.h"
#include "../Pages/LargeObject/LargeDataPage.h"
#include <cstdint>
#include <cstring>
#include <ctime>
#include <iostream>
#include <stdexcept>
#include "../../AdditionalLibraries/AdditionalDataTypes/Expression/Expression.h"
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
        this->header.overflowBitMap = new BitMap(numberOfColumns);
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

    int Row::InsertNewColumn(Block* block)
    {
        this->header.nullBitMap->Set(this->data.size(), block->GetBlockData() == nullptr);
        this->header.largeObjectBitMap->Set(this->data.size(), false);
        this->header.overflowBitMap->Set(this->data.size(), false);

        this->data.push_back(block);

        this->header.rowSize += block->GetBlockSize();

        return block->GetBlockSize();
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
            const ColumnType columnType = this->data[i]->GetColumnType();
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
                case ColumnType::TinyInt:
                {
                    std::cout << *reinterpret_cast<const int8_t*>(blockData);
                    break;
                }
                case ColumnType::SmallInt:
                {
                    std::cout << *reinterpret_cast<const int16_t*>(blockData);
                    break;
                }
                case ColumnType::Int:
                {
                    std::cout << *reinterpret_cast<const int32_t*>(blockData);
                    break;
                }
                case ColumnType::BigInt:
                {
                    std::cout << *reinterpret_cast<const int64_t*>(blockData);
                    break;
                }
                case ColumnType::Decimal:
                {
                    std::cout << Decimal(blockData, blockSize).ToString();
                    break;
                }
                case ColumnType::Guid: {
                    std::cout << this->data[i]->GetGuid();
                    break;
                }
                case ColumnType::String:
                {
                    std::cout.write(reinterpret_cast<const char*>(blockData), blockSize);
                    break;
                }
                case ColumnType::UnicodeString:
                {
                    std::wcout.write(reinterpret_cast<const wchar_t*>(blockData), blockSize / sizeof(char16_t));
                    break;
                }
                case ColumnType::Bool:
                {
                    std::cout << (*reinterpret_cast<const bool*>(blockData) ? "true" : "false");
                    break;
                }
                case ColumnType::DateTime:
                {
                    auto time = *reinterpret_cast<const time_t*>(blockData);
                    std::cout << DateTime(time);
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
        this->header.rowSize = 0;
        for (const auto& block : this->data)
            this->header.rowSize += block->GetBlockSize();
    }

    unsigned char* Row::GetLargeObjectValue(const DataObjectPointer &objectPointer, uint32_t* objectSize) const
    {
        LargeDataPage* page = this->table->GetLargeDataPage(objectPointer.pageId);

        const DataObject* object = page->GetObject();

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

    Pages::OverflowRow* Row::GetOverflowValue(const Pages::OverflowPointer & objectPointer) const{
        OverflowPage* page = this->table->GetOverflowPage(objectPointer.pageId);
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
        rowHeaderSize += this->header.overflowBitMap->GetSizeInBytes();

        return rowHeaderSize;
    }

    bool Row::Evaluate(const Expressions::Expression *expression) const{
        if (expression == nullptr)
            return true;

        switch (expression->type) {
            case Expressions::ExpressionType::Predicate: {
                const auto& actualData = this->GetData()[expression->columnIndex];

                const auto& expected = expression->value;
                switch (expression->operation) {
                    case Expressions::ExpressionOperator::Equal:
                        return *actualData == expected;
                    case Expressions::ExpressionOperator::NotEqual:
                        return *actualData != expected;
                    case Expressions::ExpressionOperator::Greater:
                        return *actualData > expected;
                    case Expressions::ExpressionOperator::GreaterEqual:
                        return *actualData >= expected;
                    case Expressions::ExpressionOperator::Less:
                        return *actualData < expected;
                    case Expressions::ExpressionOperator::LessEqual:
                        return *actualData <= expected;
                    default:
                        throw std::runtime_error("Unknown operator specified");
                }
            }
            case Expressions::ExpressionType::And:
                return this->Evaluate(expression->left) && this->Evaluate(expression->right);
            case Expressions::ExpressionType::Or:
                return this->Evaluate(expression->left) || this->Evaluate(expression->right);
            default:
                throw std::runtime_error("Invalid expression type");
        }
    }

    int Row::Update( const vector<Field> & updates){
      const auto prevRowSize = this->GetRowSize();

      for (const auto & i : updates)
      {
        const column_index_t &associatedColumnIndex = i.GetColumnIndex();

        auto *block = this->data.at(associatedColumnIndex);

        const ColumnType columnType = block->GetColumnType();

        if (columnType > Constants::ColumnType::ColumnTypeCount)
          throw invalid_argument("Table::InsertRow: Unsupported Column Type");

        if (i.GetIsNull())
        {
          block->SetData(nullptr, 0);
          this->SetNullBitMapValue(associatedColumnIndex, true);
          continue;
        }

        block->SetData(i.GetRawData(), i.GetSize());
      }

      this->UpdateRowSize();

      const auto rowSize = this->GetRowSize();

      return static_cast<int>(rowSize - prevRowSize);
    }

    Block* Row::FindLargestVariableLengthColumn() const{
        Block* largestColumn = nullptr;
        int size = 0;

        for(const auto& block : this->data){

            const auto& columnType = block->GetColumnType();
            const auto& columnIndex = block->GetColumnIndex();

            if(columnType != ColumnType::String
              && columnType != ColumnType::UnicodeString
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
                auto *block = new StorageTypes::Block(nullptr, 0, columns[j]);

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

    std::ostream & operator<<(std::ostream &os, const Row &row){

        for(size_t i = 0; i < row.data.size(); i++)
        {
            const ColumnType columnType = row.data[i]->GetColumnType();
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
                case ColumnType::Guid: {
                    cout << row.data[i]->GetGuid();
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
                    cout << (*reinterpret_cast<const bool*>(blockData) ? "true" : "false");
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

            if(i == row.data.size() - 1)
                cout << '\n';
            else
                cout << " || ";
        }

        return os;
    }

}