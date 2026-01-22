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
    RowHeader & RowHeader::operator=(const RowHeader &otherHeader){
        if (this == &otherHeader)
            return *this;

        this->nullBitMap = ByteMaps::BitMap(otherHeader.nullBitMap);
        this->largeObjectBitMap = ByteMaps::BitMap(otherHeader.largeObjectBitMap);
        this->overflowBitMap = ByteMaps::BitMap(otherHeader.overflowBitMap);

        this->version = otherHeader.version;

        return *this;
    }

    Value Row::Materialize(const Int indexPos) const{
        const auto* dataBlock = this->data.at(indexPos);

        if (this->header.largeObjectBitMap.Get(indexPos)) {
            const auto ptr = dataBlock->AsLargeObjectPointer();
            UnsignedInt objectSize;

            const auto* rawObject = this->GetLargeObjectValue(ptr, &objectSize);
            return Value(rawObject, objectSize, dataBlock->ColumnType());
        }

        if (this->header.overflowBitMap.Get(indexPos)) {
            const auto ptr = this->data.at(indexPos)->AsOverflowPointer();
            const auto* overflowRow = this->GetOverflowValue(ptr);
            return Value(overflowRow->object, overflowRow->objectSize, dataBlock->ColumnType());
        }

        //base scenario
        return Value(dataBlock->Data(), dataBlock->Size(), dataBlock->ColumnType());
    }

    bool Row::IsDeleted(const Snapshot &snapshot) const{
        const auto& versionHeader = this->header.version;
        return versionHeader.deletedTransactionId != FIRST_TRANSACTION_ID
            && versionHeader.deletedTransactionId < snapshot.maximumTransactionId
            && !snapshot.activeTransactionIds.Contains(versionHeader.deletedTransactionId)
            && versionHeader.deletedTransactionId != snapshot.transactionId;
    }

    void Row::WriteVersionToBuffer(object_t*& buffer, page_offset_t& offSet) const{
        std::memcpy(buffer + offSet, &this->header.version, RowVersioningHeader::Size);
        offSet += RowVersioningHeader::Size;
    }

    void Row::WriteVersionToBuffer(std::vector<char>& buffer, page_offset_t& offSet) const{
        std::memcpy(buffer.data() + offSet, &this->header.version, RowVersioningHeader::Size);
        offSet += RowVersioningHeader::Size;
    }

    Row::Row() {
        this->table = nullptr;
        this->header.nullBitMap = ByteMaps::BitMap(0, true);
        this->header.overflowBitMap = ByteMaps::BitMap(0, false);
        this->header.largeObjectBitMap = ByteMaps::BitMap(0, false);
    }

    Row::Row(const Table& table){
        this->table = &table;
        const auto numberOfColumns = this->table->GetNumberOfColumns();

        this->data.resize(numberOfColumns);

        this->header.nullBitMap = ByteMaps::BitMap(numberOfColumns);
        this->header.largeObjectBitMap = ByteMaps::BitMap(numberOfColumns);
        this->header.overflowBitMap = ByteMaps::BitMap(numberOfColumns);
    }

    Row::Row(const std::vector<const Column *> &columns){
        const auto& size = columns.size();

        this->table = nullptr;
        this->header.nullBitMap = ByteMaps::BitMap(size, true);
        this->header.overflowBitMap = ByteMaps::BitMap(size, false);
        this->header.largeObjectBitMap = ByteMaps::BitMap(size, false);

        for (const auto* column: columns)
            this->data.push_back(new Block(column));
    }

    Row::Row(const Table& table, const vector<Block*>& data, const ByteMaps::BitMap* nullBitMap){
        this->table = &table;
        this->header.nullBitMap = ByteMaps::BitMap(nullBitMap);

        for (const auto& block : data)
            this->data.push_back(new Block(block));
    }

    Row::Row(const Row *row){
        this->table = row->table;
        this->header = row->header;

        for (const auto& block : row->data)
            this->data.push_back(new Block(block));
    }

    Row::Row(const Row &copyRow){
        this->Id = copyRow.Id;
        this->table = copyRow.table;
        this->header = copyRow.header;

        for (const auto& block : copyRow.data)
            this->data.push_back(new Block(block));
    }

    Row::Row(Row&& otherRow) noexcept{
        this->data = std::move(otherRow.data);
        this->header = otherRow.header;
        this->table = otherRow.table;

        otherRow.table = nullptr;
        otherRow.data = {};
    }

    Row & Row::operator=(const Row &copyRow){
        if (this == &copyRow)
            return *this;

        this->Id = copyRow.Id;
        this->header = copyRow.header;
        this->table = copyRow.table;
        this->data.clear();

        for (const auto& block : copyRow.data)
            this->data.push_back(new Block(block));

        return *this;
    }

    Row& Row::operator=(Row&& otherRow) noexcept{
        if (this == &otherRow)
            return *this;

        this->Id = otherRow.Id;
        this->data = std::move(otherRow.data);
        this->header = otherRow.header;
        this->table = otherRow.table;

        otherRow.table = nullptr;
        otherRow.data = {};

        return *this;
    }

    Row::~Row(){
        for(const auto& block : this->data)
            delete block;
    }

    void Row::Print() const{
        for(Int i = 0; i < this->data.size(); i++){
            const auto columnType = this->data[i]->ColumnType();
            const auto* blockData = this->data[i]->Data();
            const auto& blockSize = this->data[i]->Size();

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
                std::cout << this->data[i]->AsGuid();
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

    std::ostream & operator<<(std::ostream &os, const Row &row){

        for(size_t i = 0; i < row.data.size(); i++){
            const auto& block = row.data[i];

            if(block->Null()){
                std::cout << "NULL";

                if(i == row.data.size() - 1)
                    std::cout << '\n';
                else
                    std::cout << " || ";

                continue;
            }

            switch (block->ColumnType()){
            case DataType::TinyInt:{
                std::cout << static_cast<Int>(block->AsTinyInt());
                break;
            }
            case DataType::SmallInt:{
                std::cout << block->AsSmallInt();
                break;
            }
            case DataType::Int:{
                std::cout << block->AsInt();
                break;
            }
            case DataType::BigInt:{
                std::cout << block->AsBigInt();
                break;
            }
            case DataType::Decimal:{
                std::cout << block->AsDecimal();
                break;
            }
            case DataType::Guid: {
                std::cout << row.data[i]->AsGuid();
                break;
            }
            case DataType::UnicodeString:
            case DataType::String:{
                std::cout << block->AsString();
                break;
            }
            case DataType::Bool:{
                std::cout << block->AsBool();
                break;
            }
            case DataType::DateTime:{
                std::cout << block->AsDateTime();
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

    RowHeader* Row::GetHeader() { return &this->header; }

    row_size_t Row::TotalSize() const{
        row_size_t currentRowSize = this->GetHeaderSize();

        for(const auto& block: this->data) {
            if (block == nullptr)
                continue;

            currentRowSize += block->Size() + sizeof(block_size_t);
        }

        return currentRowSize;
    }

    row_header_size_t Row::GetHeaderSize() const{
        row_header_size_t rowHeaderSize = 0;

        rowHeaderSize += sizeof(column_number_t);
        rowHeaderSize += this->header.nullBitMap.GetSizeInBytes();

        if(!this->header.largeObjectBitMap.Empty())
            rowHeaderSize += this->header.largeObjectBitMap.GetSizeInBytes();

        if(!this->header.overflowBitMap.Empty())
            rowHeaderSize += this->header.overflowBitMap.GetSizeInBytes();

        rowHeaderSize += RowVersioningHeader::Size;

        return rowHeaderSize;
    }

    Block* Row::FindLargestVariableLengthColumn() const{
        Block* largestColumn = nullptr;
        int size = 0;

        for(const auto& block : this->data){

            const auto& columnType = block->ColumnType();
            const auto& columnIndex = block->ColumnIndex();

            if(columnType != DataType::String
                && columnType != DataType::UnicodeString
                && this->header.largeObjectBitMap.Get(columnIndex))
                continue;

            if(block->Size() <= size)
                continue;

            largestColumn = block;
            size = block->Size();
        }

        return largestColumn;
    }

    bool Row::IsVisibleForTransaction(const Snapshot& snapshot) const {
        if (snapshot.IsSystemTransaction())
            return true;

        auto& versionHeader = this->header.version;
        if (versionHeader.createdTransactionId < snapshot.minimumTransactionId)
            return !this->IsDeleted(snapshot);

        if (versionHeader.createdTransactionId == snapshot.transactionId
            || versionHeader.createdTransactionId >= snapshot.maximumTransactionId
            || snapshot.activeTransactionIds.Contains(versionHeader.createdTransactionId))
            return false;

        return !this->IsDeleted(snapshot);
    }

    QueryResult Row::AsQueryResult() const{
        QueryResult result;

        for (int i = 0;i < this->data.size(); i++){
            auto value = this->GetColumnByIndex(i);
            result.AddColumn(value);
        }

        return result;
    }

    bool Row::IsInvalid() const{
        return this->Id.IsInvalid();
    }

    bool Row::HasOlderVersion() const{
        return this->header.version.olderVersionPointer.pageId != INVALID_PAGE_ID;
    }

    //TODO change to void as an extension method and remove moves and copies
    //row is temporary object
    void Row::Join(const Row* row){
        for (const auto* block : row->GetData())
            this->InsertNewColumn(new Block(block));
    }

    void Row::LeftJoin(const std::vector<const Column*>& innerTableColumns){
        for (const auto& column : innerTableColumns)
            this->InsertNewColumn(new Block(column));

    }

    void Row::RightJoin(const std::vector<const Column *> &innerTableColumns){
        for (const auto& column : innerTableColumns)
            this->InsertNewColumnAtBeginning(new Block(column));
    }

    void Row::InsertColumnAtEnd(Block *block){
        this->header.nullBitMap.Set(this->data.size(), block->Data() == nullptr);
        this->header.largeObjectBitMap.Set(this->data.size(), false);
        this->header.overflowBitMap.Set(this->data.size(), false);

        this->data.push_back(block);
    }

    void Row::InsertColumnData(Block *block, const column_index_t columnIndex){
        if (this->data[columnIndex] != nullptr)
            delete this->data[columnIndex];

        this->data[columnIndex] = block;
    }

    int Row::InsertNewColumn(Block* block){
        const auto oldSize = this->TotalSize();

        this->header.nullBitMap.Set(this->data.size(), block->Data() == nullptr);
        this->header.largeObjectBitMap.Set(this->data.size(), false);
        this->header.overflowBitMap.Set(this->data.size(), false);

        this->data.push_back(block);

        const auto newSize = this->TotalSize();

        const auto diff = static_cast<int>(newSize) - static_cast<int>(oldSize);

        return diff;
    }

    Int Row::InsertNewColumnAtBeginning(Block *block){
        const auto oldSize = this->TotalSize();

        this->header.nullBitMap.Set(this->data.size(), block->Data() == nullptr);
        this->header.largeObjectBitMap.Set(this->data.size(), false);
        this->header.overflowBitMap.Set(this->data.size(), false);

        this->data.insert(this->data.begin(), block);

        const auto newSize = this->TotalSize();

        const auto diff = static_cast<int>(newSize) - static_cast<int>(oldSize);

        return diff;
    }

    void Row::UpdateColumnData(Block *block){
        const column_index_t& columnIndex = block->ColumnIndex();

        if (this->data[columnIndex] != nullptr)
            delete this->data[columnIndex];

        this->data[columnIndex] = block;
    }

    Errors::RuntimeStatus Row::Update( const vector<Value> & updates, int& diff){
        const auto prevRowSize = this->TotalSize();

        for (const auto & value : updates){
            const column_index_t &associatedColumnIndex = value.GetColumnIndex();

            auto *block = this->data.at(associatedColumnIndex);

            // if (associatedColumnIndex < this->cache.size())
            //     this->cache.at(associatedColumnIndex).isMaterialized = false;

            if (value.IsNull())
            {
                block->SetData(nullptr, 0);
                this->SetNullBitMapValue(associatedColumnIndex, true);
                continue;
            }

            if (block->Null())
                this->SetNullBitMapValue(associatedColumnIndex, false);

            auto result = block->SetData(value);
            if (result.code != Errors::RuntimeError::Ok)
                return result;
        }

        const auto rowSize = this->GetHeaderSize();

        diff = rowSize - prevRowSize;
        return {};
    }

    Errors::RuntimeStatus Row::Update(
        const std::vector<QueryPipeline::Statements::UpdateColumn*> &updates,
        int& diff
    ){
        const auto prevRowSize = this->TotalSize();

        Expressions::EvaluationContext context(this);
        for (const auto & update : updates){
            const auto value = update->value->Evaluate(context);

            const column_index_t &associatedColumnIndex = update->name.index;

            auto *block = this->data.at(associatedColumnIndex);

            // if (associatedColumnIndex < this->cache.size())
            //     this->cache.at(associatedColumnIndex).isMaterialized = false;

            if (value.IsNull()){
                block->SetData(nullptr, 0);
                this->SetNullBitMapValue(associatedColumnIndex, true);
                continue;
            }

            if (block->Null())
                this->SetNullBitMapValue(associatedColumnIndex, false);

            auto result = block->SetData(value);
            if (result.code != Errors::RuntimeError::Ok)
                return result;
        }

        const auto rowSize = this->GetHeaderSize();
        diff = static_cast<int>(rowSize - prevRowSize);

        return {};
    }

    Value Row::GetColumnByIndex(const Int indexPos) const{ return this->Materialize(indexPos);}

    const vector<Block*> & Row::GetData() const { return this->data; }

    vector<Block*> &Row::GetData() { return this->data; }

    vector<column_index_t> Row::GetLargeBlocks()const
    {
        vector<column_index_t> largeBlocksIndexes;
        for(const auto& block : this->data)
        {
            if(block->Size() >= LARGE_DATA_OBJECT_SIZE)
                largeBlocksIndexes.push_back(block->ColumnIndex());
        }

        return largeBlocksIndexes;
    }

    unsigned char* Row::GetLargeObjectValue(const page_id_t pageId, UnsignedInt* objectSize) const{
        auto page = this->table->GetLargeDataPage(pageId);

        const auto* object = page->GetObject();

        auto currentObjectSize = object->objectSize;

        auto* buffer = new unsigned char[currentObjectSize];

        std::memcpy(buffer, object->object, currentObjectSize);

        while (object->nextPageId != 0)
        {
            const page_id_t nextPageId = object->nextPageId;

            page = this->table->GetLargeDataPage(nextPageId);
            object = page->GetObject();

            const page_size_t nextObjectSize = object->objectSize;

            const unsigned char* prevValue = buffer;

            buffer = new unsigned char[currentObjectSize + nextObjectSize];

            std::memcpy(buffer, prevValue, currentObjectSize);
            delete[] prevValue;

            std::memcpy(buffer + currentObjectSize, object->object, nextObjectSize);

            currentObjectSize += object->objectSize;
        }

        *objectSize = currentObjectSize;

        return buffer;
    }

    Block * Row::GetLargeObject(const page_id_t pageId, const Column *column)const{
        auto page = this->table->GetLargeDataPage(pageId);

        const auto* object = page->GetObject();

        UnsignedInt currentObjectSize = object->objectSize;

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

    Pages::OverflowRow* Row::GetOverflowValue(const Pages::OverflowPointer& objectPointer) const{
        const auto page = this->table->GetOverflowPage(objectPointer.pageId);
        return page->GetObject(objectPointer.index);
    }

    Row Row::GetVisibleVersionForTransaction(const Snapshot& snapshot)const {
        if (this->IsVisibleForTransaction(snapshot))
            return *this;

        if (!this->HasOlderVersion())
            return Row();

        return VersionDatabase::Get().RetrieveRow(snapshot, this->header.version.olderVersionPointer, this->table);
    }

    void Row::SetId(const page_id_t pageId, const Int indexId){
        this->Id.pageId = pageId;
        this->Id.indexId = indexId;
    }

    void Row::SetCurrentTransactionId(const transaction_id_t transactionId){
        this->header.version.createdTransactionId = transactionId;
    }

    void Row::SetDeletedTransactionId(const transaction_id_t transactionId) {
        this->header.version.deletedTransactionId = transactionId;
    }

    void Row::SetOlderVersionPointer(const page_id_t pageId, const page_offset_t offset) {
        this->header.version.olderVersionPointer.pageId = pageId;
        this->header.version.olderVersionPointer.offset = offset;
    }

    void Row::SetNullBitMapValue(const bit_map_pos_t position, const bool value){ this->header.nullBitMap.Set(position, value); }

    void Row::SetOverflowBitMapValue(const bit_map_pos_t position, const bool value){ this->header.overflowBitMap.Set(position, value); }

    const Headers::RowIdentifier& Row::GetId() const{
        return this->Id;
    }

    bool Row::GetNullBitMapValue(const bit_map_pos_t position) const { return this->header.nullBitMap.Get(position); }

    bool Row::GetOverflowBitMapValue(const bit_map_pos_t position) const{ return this->header.overflowBitMap.Get(position); }

    const Table* Row::GetTable() const{
        return this->table;
    }

    void Row::Serialize(std::vector<char>* buffer, page_offset_t& pos)const{
        this->WriteHeaderToBuffer(*buffer, pos);

        column_index_t columnIndex = 0;
        for (const auto &block : this->data){
            if (this->header.nullBitMap.Get(columnIndex)){
                columnIndex++;
                continue;
            }

            const auto& dataSize = block->Size();

            std::memcpy(buffer->data() + pos, &dataSize, sizeof(block_size_t));
            pos += sizeof(block_size_t);

            std::memcpy(buffer->data() + pos, block->Data(), dataSize);
            pos += dataSize;

            columnIndex++;
        }
    }

    void Row::Serialize(object_t*& buffer, page_offset_t& offSet) const{
        this->WriteHeaderToBuffer(buffer, offSet);

        column_index_t columnIndex = 0;
        for (const auto &block : this->data){
            if (this->header.nullBitMap.Get(columnIndex))
            {
                columnIndex++;
                continue;
            }

            const auto& dataSize = block->Size();

            std::memcpy(buffer + offSet, &dataSize, sizeof(block_size_t));
            offSet += sizeof(block_size_t);

            std::memcpy(buffer + offSet, block->Data(), dataSize);
            offSet += dataSize;

            columnIndex++;
        }
    }

    void Row::WriteHeaderToBuffer(object_t*& buffer, page_offset_t& offSet) const {
        this->WriteVersionToBuffer(buffer, offSet);
        this->header.nullBitMap.WriteDataToBuffer(buffer, offSet);
        this->header.largeObjectBitMap.WriteDataToBuffer(buffer, offSet);
        this->header.overflowBitMap.WriteDataToBuffer(buffer, offSet);
    }

    void Row::WriteHeaderToBuffer(std::vector<char>& buffer, page_offset_t& offSet) const{
        this->WriteVersionToBuffer(buffer, offSet);
        this->header.nullBitMap.WriteDataToFile(&buffer, offSet);
        this->header.largeObjectBitMap.WriteDataToFile(&buffer, offSet);
        this->header.overflowBitMap.WriteDataToFile(&buffer, offSet);
    }

    void Row::Deserialize(const std::vector<char>& buffer, page_offset_t &pos){
        this->ReadVersionHeaderFromDisk(buffer, pos);
        this->header.nullBitMap.GetDataFromFile(buffer, pos);
        this->header.largeObjectBitMap.GetDataFromFile(buffer, pos);
        this->header.overflowBitMap.GetDataFromFile(buffer, pos);

        const auto& columns = this->table->GetColumns();

        for (int j = 0; j < columns.size(); j++)
        {
            if (this->header.nullBitMap.Get(j))
            {
                auto *block = new StorageTypes::Block(columns[j]);
                this->InsertColumnData(block, j);

                continue;
            }

            block_size_t bytesToRead;

            std::memcpy(&bytesToRead, buffer.data() + pos, sizeof(block_size_t));
            pos += sizeof(block_size_t);

            auto *bytes = new unsigned char[bytesToRead];
            std::memcpy(bytes, buffer.data() + pos, bytesToRead);
            pos += bytesToRead;

            auto *block = new StorageTypes::Block(bytes, bytesToRead, columns[j]);

            this->InsertColumnData(block, j);

            delete[] bytes;
        }
    }

    inline RowVersioningHeader Row::PeakVersionHeaderFromDisk(const object_t* buffer, const page_offset_t offSet){
        auto header = RowVersioningHeader();
        std::memcpy(&header, buffer + offSet, RowVersioningHeader::Size);
        return header;
    }

    inline void Row::ReadVersionHeaderFromDisk(const object_t* buffer, page_offset_t &offSet) {
        std::memcpy(&this->header.version, buffer + offSet, RowVersioningHeader::Size);
        offSet += RowVersioningHeader::Size;
    }

    void Row::ReadVersionHeaderFromDisk(const std::vector<char>& buffer, page_offset_t& offSet){
        std::memcpy(&this->header.version, buffer.data() + offSet, RowVersioningHeader::Size);
        offSet += RowVersioningHeader::Size;
    }

    void Row::ReadHeaderFromDisk(const object_t* buffer, page_offset_t &offSet){
        this->ReadVersionHeaderFromDisk(buffer, offSet);
        this->header.nullBitMap.GetDataFromFile(buffer, offSet);
        this->header.largeObjectBitMap.GetDataFromFile(buffer, offSet);
        this->header.overflowBitMap.GetDataFromFile(buffer, offSet);
    }

    void Row::ReadDataFromDisk(
        const object_t* buffer,
        page_offset_t &offSet,
        const std::vector<Column*>& columns
    ){
        for (int index = 0; index < columns.size(); index++){
            if (this->header.nullBitMap.Get(index))
            {
                auto *block = new Block(columns[index]);

                this->InsertColumnData(block, index);

                continue;
            }

            block_size_t bytesToRead;

            std::memcpy(&bytesToRead, buffer + offSet, sizeof(block_size_t));
            offSet += sizeof(block_size_t);

            auto *bytes = new unsigned char[bytesToRead];
            std::memcpy(bytes, buffer + offSet, bytesToRead);

            offSet += bytesToRead;

            auto *block = new Block(bytes, bytesToRead, columns[index]);

            this->InsertColumnData(block, index);
        }
    }

}
