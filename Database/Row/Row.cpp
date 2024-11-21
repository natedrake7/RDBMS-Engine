#include "Row.h"

Row::Row(const Table& table)
{
    this->table = &table;
    this->data.resize(this->table->GetNumberOfColumns());
    this->rowSize = 0;
    this->maxRowSize = 0;
    this->isOriginalRow = true;
}

Row::Row(const Table& table, const vector<Block*>& data) {
    this->table = &table;
    this->data = data;
    this->isOriginalRow = false;
}

Row::~Row(){
    if(this->isOriginalRow)
        for(const auto& block : this->data)
            delete block;
}

void Row::InsertColumnData(Block *block, const  uint16_t& columnIndex)
{
    this->data[columnIndex] = block;
    this->rowSize += block->GetBlockSize();
}

const vector<Block *> & Row::GetData() const { return this->data; }

void Row::PrintRow() const
{
    for(size_t i = 0; i < this->data.size(); i++)
    {
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
        {
            // const char* hashKey;
            // memcpy(&hashKey, this->data[i]->GetBlockData(), sizeof(uint64_t));
            // const uint64_t hashKey = reinterpret_cast<const uint64_t>(this->data);
            // if (this->data[i]->IsLargeObject())
            // {
            //     DataObjectPointer objectPointer;
            //     memcpy(&objectPointer, this->data[i]->GetBlockData(), sizeof(DataObjectPointer));
            //     cout<< this->GetLargeObjectValue(objectPointer);
            //     continue;
            // }

            cout << reinterpret_cast<const char*>(this->data[i]->GetBlockData());
        }

        if(i == this->data.size() - 1)
            cout << '\n';
        else
            cout << " || ";
    }
}

const uint32_t & Row::GetRowSize() const { return this->rowSize; }

vector<uint16_t> Row::GetLargeBlocks()
{
    vector<uint16_t> largeBlocksIndexes;
    for(const auto& block : this->data)
    {
        const uint16_t& blockIndex = block->GetColumnIndex();
        const block_size_t& blockSize = block->GetBlockSize();

        if(blockSize >= LARGE_DATA_OBJECT_SIZE)
            largeBlocksIndexes.push_back(blockIndex);
    }

    return largeBlocksIndexes;
}

void Row::UpdateRowSize()
{
    this->rowSize = 0;
     for (const auto& block : this->data)
         this->rowSize += block->GetBlockSize();
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



