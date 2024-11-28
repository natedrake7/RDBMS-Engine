#include "Row.h"

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

Row::Row(const Table& table)
{
    this->table = &table;

    const auto numberOfColumns = this->table->GetNumberOfColumns();
    
    this->data.resize(numberOfColumns);

    this->header.nullBitMap = new BitMap(numberOfColumns);
    this->header.largeObjectBitMap = new BitMap(numberOfColumns);
}

Row::Row(const Table& table, const vector<Block*>& data)
{
    this->table = &table;
    
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

Row::~Row()
{
    for(const auto& block : this->data)
        delete block;
}

void Row::InsertColumnData(Block *block, const  uint16_t& columnIndex)
{
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
        
        if(blockData == nullptr)
        {
            cout<< "NULL";
        }
        else if (columnType == ColumnType::TinyInt)
        {
            int8_t tinyIntValue;
            memcpy(&tinyIntValue, blockData, sizeof(int8_t));
            cout << static_cast<int>(tinyIntValue);
        }
        else if (columnType == ColumnType::SmallInt)
        {
            int16_t smallIntValue;
            memcpy(&smallIntValue, blockData, sizeof(int16_t));
            cout << smallIntValue;
        }
        else if(columnType == ColumnType::Int)
        {
            int32_t intValue;
            memcpy(&intValue, blockData, sizeof(int32_t));
            cout << intValue;
        }
        else if (columnType == ColumnType::BigInt)
        {
            int64_t bigIntValue;
            memcpy(&bigIntValue, blockData, sizeof(int64_t));
            cout << bigIntValue;
        }
        else if(columnType == ColumnType::String)
            cout << reinterpret_cast<const char*>(blockData);

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

char* Row::GetLargeObjectValue(const DataObjectPointer &objectPointer) const
{
    LargeDataPage* page = this->table->GetLargeDataPage(objectPointer.pageId);

    DataObject* object = page->GetObject(objectPointer.objectIndex);

    // string largeValue;
    // largeValue.resize(object->objectSize);
    char* largeValue = new char[object->objectSize + 1];

    memcpy(largeValue, object->object, object->objectSize);

    largeValue[object->objectSize] = '\0';

    while (object->nextPageId != 0)
    {
        page = this->table->GetLargeDataPage(object->nextPageId);
        object = page->GetObject(object->nextObjectIndex);

        const auto& stringSize = strlen(largeValue);
        char* prevValue = largeValue;

        largeValue = new char[stringSize + object->objectSize + 1];

        memcpy(largeValue, prevValue, stringSize);
        memcpy(largeValue + stringSize, object->object, object->objectSize);

        largeValue[stringSize + object->objectSize] = '\0';

        delete[] prevValue;
    }

    return largeValue;
}

void Row::SetNullBitMapValue(const bit_map_pos_t &position, const bool &value) { this->header.nullBitMap->Set(position, value); }

bool Row::GetNullBitMapValue(const bit_map_pos_t &position) const { return this->header.nullBitMap->Get(position); }

RowHeader* Row::GetHeader() { return &this->header; }

row_size_t Row::GetTotalRowSize() const
{
    row_size_t currentRowSize = this->GetRowHeaderSize();
    currentRowSize += this->table->GetNumberOfColumns() * sizeof(block_size_t); //decrease by the null blocks here
    currentRowSize += this->header.rowSize;
    
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




