#include "Row.h"

RowMetaData::RowMetaData()
{
    this->rowSize = 0;
    this->maxRowSize = 0;
    this->nullBitMap = nullptr;
    this->largeObjectBitMap = nullptr;
}

RowMetaData::~RowMetaData()
{
    delete this->nullBitMap;
    delete this->largeObjectBitMap;
}

Row::Row(const Table& table)
{
    this->table = &table;

    const auto numberOfColumns = this->table->GetNumberOfColumns();
    
    this->data.resize(numberOfColumns);

    this->metaData.nullBitMap = new BitMap(numberOfColumns);
    this->metaData.largeObjectBitMap = new BitMap(numberOfColumns);
}

Row::Row(const Table& table, const vector<Block*>& data)
{
    this->table = &table;
    this->data = data;
    this->UpdateRowSize();
    this->metaData.maxRowSize = 0;
}

Row::~Row()
{
    for(const auto& block : this->data)
        delete block;
}

void Row::InsertColumnData(Block *block, const  uint16_t& columnIndex)
{
    this->data[columnIndex] = block;
    this->metaData.rowSize += block->GetBlockSize();
}

const vector<Block *> & Row::GetData() const { return this->data; }

void Row::PrintRow() const
{
    for(size_t i = 0; i < this->data.size(); i++)
    {
        //why this breaks check it column may be null
        const ColumnType columnType = this->data[i]->GetColumnType();

        if (columnType == ColumnType::TinyInt)
        {
            int8_t tinyIntValue;
            memcpy(&tinyIntValue, this->data[i]->GetBlockData(), sizeof(int8_t));
            cout << static_cast<int>(tinyIntValue);
        }
        else if (columnType == ColumnType::SmallInt)
        {
            int16_t smallIntValue;
            memcpy(&smallIntValue, this->data[i]->GetBlockData(), sizeof(int16_t));
            cout << smallIntValue;
        }
        else if(columnType == ColumnType::Int)
        {
            int32_t intValue;
            memcpy(&intValue, this->data[i]->GetBlockData(), sizeof(int32_t));
            cout << intValue;
        }
        else if (columnType == ColumnType::BigInt)
        {
            int64_t bigIntValue;
            memcpy(&bigIntValue, this->data[i]->GetBlockData(), sizeof(int64_t));
            cout << bigIntValue;
        }
        else if(columnType == ColumnType::String)
            cout << reinterpret_cast<const char*>(this->data[i]->GetBlockData());

        if(i == this->data.size() - 1)
            cout << '\n';
        else
            cout << " || ";
    }
}

const uint32_t& Row::GetRowSize() const { return this->metaData.rowSize; }

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
    this->metaData.rowSize = 0;
     for (const auto& block : this->data)
         this->metaData.rowSize += block->GetBlockSize();
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
        object = page->GetObject(objectPointer.objectIndex);

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

void Row::SetBitMapValue(const bit_map_pos_t &position, const bool &value) { this->metaData.nullBitMap->Set(position, value); }

bool Row::GetBitMapValue(const bit_map_pos_t &position) const { return this->metaData.nullBitMap->Get(position); }

RowMetaData* Row::GetMetaData() { return &this->metaData; }




