#include "../../include/DataStorage/Row.h"

#include <cstdint>
#include <cstring>
#include <ctime>
#include <iostream>
#include <stdexcept>

#include "../../../Systemic/include/DataTypes/Decimal.h"
#include "../../../Systemic/include/DataStructures/BitMap.h"
#include "../../../Systemic/include/DataTypes/DateTime.h"
#include "../../include/DataStorage/Table.h"
#include "../../include/DataStorage/Block.h"
#include "../../include/DataStorage/Column.h"
#include "../../include/Pages/LargeObjectPage.h"
#include "../../include/Pages/OverflowPage.h"
#include "../../Server/include/Server.h"

namespace DatabaseEngine::StorageTypes {
    RowHeader::RowHeader()
    {
        this->tableId = 0;
        this->numberOfColumns = 0;
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

        this->tableId = otherHeader.tableId;
        this->numberOfColumns = otherHeader.numberOfColumns;
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
        value = Value(dataBlock->GetRawData(), dataBlock->GetSize(), dataBlock->GetColumnType());
        return value;
    }

    Row::Row(const Table& table)
    {
        this->table = &table;
        this->header.tableId = table.GetTableId();

        const auto numberOfColumns = this->table->GetNumberOfColumns();

        this->data.resize(numberOfColumns);

        this->header.numberOfColumns = numberOfColumns;

        this->header.nullBitMap = new ByteMaps::BitMap(numberOfColumns);
        this->header.largeObjectBitMap = new ByteMaps::BitMap(numberOfColumns);
        this->header.overflowBitMap = new ByteMaps::BitMap(numberOfColumns);

        this->isCopy = false;
    }

    Row::Row(const Table& table, const vector<Block*>& data, const ByteMaps::BitMap* nullBitMap)
    {
        this->table = &table;
        this->header.nullBitMap = new ByteMaps::BitMap(*nullBitMap);

        for (const auto& block : data)
            this->data.push_back(new Block(block));

        this->header.numberOfColumns = static_cast<column_number_t>(data.size());
        this->header.tableId = table.GetTableId();

        this->isCopy = false;
    }

    Row::Row(const std::vector<const Column *> &columns){
        const auto& size = columns.size();

        this->table = nullptr;
        this->header.nullBitMap = new ByteMaps::BitMap(size, true);
        this->header.overflowBitMap = new ByteMaps::BitMap(size, false);
        this->header.largeObjectBitMap = new ByteMaps::BitMap(size, false);

        for (const auto* column: columns)
            this->data.push_back(new Block(column));

        this->header.numberOfColumns = static_cast<column_number_t>(size);

        this->isCopy = true;
    }

    Row::Row(const Row &copyRow)
    {
        this->table = copyRow.table;
        this->header = copyRow.header;
        this->versionHeader = copyRow.versionHeader;

        for (const auto& block : copyRow.data)
            this->data.push_back(new Block(block));

        this->isCopy = true;
    }

    Row::Row(const Row *row){
        this->table = row->table;
        this->header = row->header;
        this->versionHeader = row->versionHeader;
        // this->cache = row->cache;

        for (const auto& block : row->data)
            this->data.push_back(new Block(block));

        this->isCopy = true;
    }

    Row::Row() {
        this->table = nullptr;
        this->isCopy = false;
        this->header.nullBitMap = new ByteMaps::BitMap(0, true);
        this->header.overflowBitMap = new ByteMaps::BitMap(0, false);
        this->header.largeObjectBitMap = new ByteMaps::BitMap(0, false);
    }

    Row & Row::operator=(const Row &copyRow)
    {
        if (this == &copyRow)
            return *this;

        this->header = copyRow.header;
        this->versionHeader = copyRow.versionHeader;
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
            delete this->data[columnIndex];

        this->data[columnIndex] = block;
    }

    void Row::InsertColumnAtEnd(Block *block) {
        this->header.nullBitMap->Set(this->data.size(), block->GetRawData() == nullptr);
        this->header.largeObjectBitMap->Set(this->data.size(), false);
        this->header.overflowBitMap->Set(this->data.size(), false);

        this->data.push_back(block);
    }

    int Row::InsertNewColumn(Block* block)
    {
        const auto oldSize = this->GetTotalSize();

        this->header.nullBitMap->Set(this->data.size(), block->GetRawData() == nullptr);
        this->header.largeObjectBitMap->Set(this->data.size(), false);
        this->header.overflowBitMap->Set(this->data.size(), false);

        this->data.push_back(block);

        this->header.numberOfColumns = static_cast<column_number_t>(this->data.size());

        const auto newSize = this->GetTotalSize();

        const auto diff = static_cast<int>(newSize) - static_cast<int>(oldSize);

        return diff;
    }

    int Row::InsertNewColumnAtBeginning(Block *block){
        const auto oldSize = this->GetTotalSize();

        this->header.nullBitMap->Set(this->data.size(), block->GetRawData() == nullptr);
        this->header.largeObjectBitMap->Set(this->data.size(), false);
        this->header.overflowBitMap->Set(this->data.size(), false);

        this->data.insert(this->data.begin(), block);

        this->header.numberOfColumns = static_cast<column_number_t>(this->data.size());

        const auto newSize = this->GetTotalSize();

        const auto diff = static_cast<int>(newSize) - static_cast<int>(oldSize);

        return diff;
    }

    void Row::UpdateColumnData(Block *block)
    {
        const column_index_t& columnIndex = block->GetColumnIndex();
        
        if (this->data[columnIndex] != nullptr)
            delete this->data[columnIndex];
        
        this->data[columnIndex] = block;
    }

    const vector<Block *> & Row::GetData() const { return this->data; }

    vector<Block *> &Row::GetData() { return this->data; }

    void Row::Print() const{
        for(size_t i = 0; i < this->data.size(); i++)
        {
            const DataType columnType = this->data[i]->GetColumnType();
            const object_t* blockData = this->data[i]->GetRawData();
            const block_size_t& blockSize = this->data[i]->GetSize();

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
                    std::cout << DataTypes::Decimal(blockData, blockSize).ToString();
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
                    std::cout << DataTypes::DateTime(time);
                    break;
                }
                case DataType::Unknown:
                default:
                    throw invalid_argument("Row::PrintRow Invalid Column specified");
            }

            if(i == this->data.size() - 1)
                cout << '\n';
            else
                cout << " || ";
        }
    }

    vector<column_index_t> Row::GetLargeBlocks()const
    {
        vector<column_index_t> largeBlocksIndexes;
        for(const auto& block : this->data)
        {
            if(block->GetSize() >= LARGE_DATA_OBJECT_SIZE)
                largeBlocksIndexes.push_back(block->GetColumnIndex());
        }

        return largeBlocksIndexes;
    }

    unsigned char* Row::GetLargeObjectValue(const Pages::DataObjectPointer &objectPointer, uint32_t* objectSize) const
    {
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

            const unsigned char* prevValue = buffer;

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

    void Row::ReadHeaderFromDisk(const std::vector<char> &buffer, page_offset_t &offSet) {
        memcpy(&this->header.tableId, buffer.data() + offSet, sizeof(table_id_t));
        offSet += sizeof(table_id_t);
        memcpy(&this->header.numberOfColumns, buffer.data() + offSet, sizeof(column_number_t));
        offSet += sizeof(column_number_t);

        this->header.nullBitMap->GetDataFromFile(buffer, offSet);
        this->header.largeObjectBitMap->GetDataFromFile(buffer, offSet);
        this->header.overflowBitMap->GetDataFromFile(buffer, offSet);
    }

    void Row::ReadVersionHeaderFromDisk(const std::vector<char> &buffer, page_offset_t &offSet) {
        memcpy(&this->versionHeader.createdTransactionId, buffer.data() + offSet, sizeof(transaction_id_t));
        offSet += sizeof(transaction_id_t);

        memcpy(&this->versionHeader.deletedTransactionId, buffer.data() + offSet, sizeof(transaction_id_t));
        offSet += sizeof(transaction_id_t);

        memcpy(&this->versionHeader.olderVersionPointer.pageId, buffer.data() + offSet, sizeof(page_id_t));
        offSet += sizeof(page_id_t);

        memcpy(&this->versionHeader.olderVersionPointer.offset, buffer.data() + offSet, sizeof(page_offset_t));
        offSet += sizeof(page_offset_t);
    }

    void Row::ReadDataFromDisk(
        const std::vector<char> &buffer,
        page_offset_t &offSet,
        const std::vector<Column*>& columns
    ) {
        for (int j = 0; j < columns.size(); j++)
        {
            if (this->header.nullBitMap->Get(j))
            {
                auto *block = new Block(columns[j]);

                this->InsertColumnData(block, j);

                continue;
            }

            block_size_t bytesToRead;

            memcpy(&bytesToRead, buffer.data() + offSet, sizeof(block_size_t));
            offSet += sizeof(block_size_t);

            auto *bytes = new unsigned char[bytesToRead];
            memcpy(bytes, buffer.data() + offSet, bytesToRead);

            offSet += bytesToRead;

            auto *block = new Block(bytes, bytesToRead, columns[j]);

            this->InsertColumnData(block, j);
        }
    }

    void Row::ReadDataFromDisk(const std::vector<char> &buffer, page_offset_t &offSet) {
        for (int j = 0; j < this->header.numberOfColumns; j++)
        {
            if (this->header.nullBitMap->Get(j))
            {
                auto *block = new Block();

                const auto _ = this->InsertNewColumn(block);

                continue;
            }

            block_size_t bytesToRead;

            memcpy(&bytesToRead, buffer.data() + offSet, sizeof(block_size_t));
            offSet += sizeof(block_size_t);

            auto *bytes = new unsigned char[bytesToRead];
            memcpy(bytes, buffer.data() + offSet, bytesToRead);

            offSet += bytesToRead;

            auto *block = new Block(bytes, bytesToRead, nullptr);

            this->InsertColumnAtEnd(block);
        }
    }

    void Row::WriteHeaderToDisk(fstream* filePtr) const {
        filePtr->write(reinterpret_cast<const char *>(&this->header.tableId), sizeof(table_id_t));
        filePtr->write(reinterpret_cast<const char *>(&this->header.numberOfColumns), sizeof(column_number_t));

        this->header.nullBitMap->WriteDataToFile(filePtr);
        this->header.largeObjectBitMap->WriteDataToFile(filePtr);
        this->header.overflowBitMap->WriteDataToFile(filePtr);
    }

    void Row::WriteVersionHeaderToDisk(fstream *filePtr) const {
        filePtr->write(reinterpret_cast<const char *>(&this->versionHeader.createdTransactionId), sizeof(transaction_id_t));
        filePtr->write(reinterpret_cast<const char *>(&this->versionHeader.deletedTransactionId), sizeof(transaction_id_t));
        filePtr->write(reinterpret_cast<const char *>(&this->versionHeader.olderVersionPointer.pageId), sizeof(page_id_t));
        filePtr->write(reinterpret_cast<const char *>(&this->versionHeader.olderVersionPointer.offset), sizeof(page_offset_t));
    }

    void Row::WriteDataToDisk(fstream *filePtr) const {
        for (int i = 0;i < this->data.size(); i++) {
            if (this->header.nullBitMap->Get(i))
                continue;

            const auto& block = this->data[i];

            const auto dataSize = block->GetSize();

            filePtr->write(reinterpret_cast<const char *>(&dataSize), sizeof(block_size_t));
            filePtr->write(reinterpret_cast<const char *>(block->GetRawData()), dataSize);
        }
    }

    RowHeader* Row::GetHeader() { return &this->header; }

    row_size_t Row::GetTotalSize() const
    {
        row_size_t currentRowSize = this->GetHeaderSize();

        for(const auto& block: this->data) {
            if (block == nullptr)
                continue;

            currentRowSize += block->GetSize() + sizeof(block_size_t);
        }

        return currentRowSize;
    }

    row_header_size_t Row::GetHeaderSize() const
    {
        row_header_size_t rowHeaderSize = sizeof(row_size_t);
        rowHeaderSize += sizeof(size_t);

        rowHeaderSize += this->header.nullBitMap->GetSizeInBytes();

        if (this->header.largeObjectBitMap != nullptr)
            rowHeaderSize += this->header.largeObjectBitMap->GetSizeInBytes();

        if (this->header.overflowBitMap != nullptr)
            rowHeaderSize += this->header.overflowBitMap->GetSizeInBytes();

        rowHeaderSize += Constants::ROW_VERSION_HEADER_SIZE;

        return rowHeaderSize;
    }

    Errors::RuntimeStatus Row::Update( const vector<Value> & updates, int& diff)const{
        const auto prevRowSize = this->GetTotalSize();

        for (const auto & value : updates){
            const column_index_t &associatedColumnIndex = value.GetColumnIndex();

            auto *block = this->data.at(associatedColumnIndex);

            if (associatedColumnIndex < this->cache.size())
                this->cache.at(associatedColumnIndex).isMaterialized = false;

            if (value.IsNull())
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

        const auto rowSize = this->GetHeaderSize();

        diff = static_cast<int>(rowSize - prevRowSize);

        return {};
    }

    Errors::RuntimeStatus Row::Update(const std::vector<QueryPipeline::Statements::UpdateColumn*> &updates, int& diff)const{
        const auto prevRowSize = this->GetTotalSize();

        Expressions::EvaluationContext context(this);
        for (const auto & update : updates)
        {
            const auto value = update->value->Evaluate(context);

            const column_index_t &associatedColumnIndex = update->name.index;

            auto *block = this->data.at(associatedColumnIndex);

            if (associatedColumnIndex < this->cache.size())
                this->cache.at(associatedColumnIndex).isMaterialized = false;

            if (value.IsNull())
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

        const auto rowSize = this->GetHeaderSize();
        diff = static_cast<int>(rowSize - prevRowSize);

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

            if(block->GetSize() <= size)
              continue;

            largestColumn = block;
            size = block->GetSize();
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
            Pages::DataObjectPointer objectPointer;
            memcpy(&objectPointer, block->GetRawData(), sizeof(Pages::DataObjectPointer));

            uint32_t objectSize;
            unsigned char *largeValue = this->GetLargeObjectValue(objectPointer, &objectSize);
            blockCopy->SetData(largeValue, objectSize);

            delete[] largeValue;
        }
        else if(this->header.overflowBitMap->Get(block->GetColumnIndex())){
            Pages::OverflowPointer overflowPointer;
            memcpy(&overflowPointer, block->GetRawData(), sizeof(Pages::OverflowPointer));

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
        memcpy(buffer->data() + pos, &this->header.numberOfColumns, sizeof(column_number_t));
        pos += sizeof(column_number_t);

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

            const auto& dataSize = block->GetSize();

            memcpy(buffer->data() + pos, &dataSize, sizeof(block_size_t));
            pos += sizeof(block_size_t);

            memcpy(buffer->data() + pos, block->GetRawData(), dataSize);
            pos += dataSize;

            columnIndex++;
        }
    }

    void Row::Deserialize(const std::vector<char> *buffer, uint32_t &pos){
        memcpy(&this->header.numberOfColumns, buffer->data() + pos, sizeof(column_number_t));
        pos += sizeof(column_number_t);

        this->header.nullBitMap->GetDataFromFile(*buffer, pos);
        this->header.largeObjectBitMap->GetDataFromFile(*buffer, pos);
        this->header.overflowBitMap->GetDataFromFile(*buffer, pos);

        const auto& columns = this->table->GetColumns();

        for (int j = 0; j < columns.size(); j++)
        {
            if (this->header.nullBitMap->Get(j))
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

    void Row::SetCurrentTransactionId(const transaction_id_t &transactionId){
        this->versionHeader.createdTransactionId = transactionId;
    }

    void Row::SetDeletedTransactionId(const transaction_id_t &transactionId) {
        this->versionHeader.deletedTransactionId = transactionId;
    }

    void Row::SetOlderVersionPointer(const page_id_t& pageId, const page_offset_t& offset) {
        this->versionHeader.olderVersionPointer.pageId = pageId;
        this->versionHeader.olderVersionPointer.offset = offset;
    }

    const Row* Row::GetVisibleVersionForTransaction(const Snapshot& snapshot) const {
       if (this->IsVisibleForTransaction(snapshot))
           return this;

        if (!this->versionHeader.HasOlderVersion())
            return nullptr;

        return Network::Server::Get().GetVersionDatabase()->RetrieveRow(snapshot, this->versionHeader.olderVersionPointer, this->table);
    }

    bool Row::IsDeleted(const Snapshot &snapshot) const{
        return this->versionHeader.deletedTransactionId != FIRST_TRANSACTION_ID
               && this->versionHeader.deletedTransactionId < snapshot.maximumTransactionId
               && !snapshot.activeTransactionIds.Contains(this->versionHeader.deletedTransactionId)
               && this->versionHeader.deletedTransactionId != snapshot.transactionId;
    }

    bool Row::IsVisibleForTransaction(const Snapshot& snapshot) const {
        if (snapshot.IsSystemTransaction())
            return true;

        if (this->versionHeader.createdTransactionId < snapshot.minimumTransactionId)
            return !this->IsDeleted(snapshot);

        if (this->versionHeader.createdTransactionId == snapshot.transactionId
            || this->versionHeader.createdTransactionId >= snapshot.maximumTransactionId
            || snapshot.activeTransactionIds.Contains(this->versionHeader.createdTransactionId))
            return false;

        return !this->IsDeleted(snapshot);
    }

    const RowVersioningHeader & Row::GetVersionHeader() const { return this->versionHeader; }

    const bool & Row::IsCopy() const{ return this->isCopy; }

    std::ostream & operator<<(std::ostream &os, const Row &row){

        for(size_t i = 0; i < row.data.size(); i++)
        {
            const DataType columnType = row.data[i]->GetColumnType();
            const object_t* blockData = row.data[i]->GetRawData();
            const block_size_t& blockSize = row.data[i]->GetSize();

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
                    cout << DataTypes::Decimal(blockData, blockSize).ToString();
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
                    cout << DataTypes::DateTime(reinterpret_cast<time_t>(blockData)).ToString();
                    break;
                }
                case DataType::Unknown:
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