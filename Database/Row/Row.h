#pragma once

#include "../../AdditionalLibraries/BitMap/BitMap.h"
#include "../Table/Table.h"
#include "../Block/Block.h"

class Block;
class Table;
class Database;

using namespace std;

typedef struct RowMetaData {
    BitMap* nullBitMap;
    BitMap* largeObjectBitMap;
    uint32_t rowSize;
    size_t maxRowSize;

    RowMetaData();
    ~RowMetaData();
}RowMetaData;

class Row
{
    RowMetaData metaData;
    vector<Block*> data;
    const Table* table;

    public:
        explicit Row(const Table& table);

        explicit Row(const Table& table, const vector<Block*>& data);

        ~Row();

        void InsertColumnData(Block* block, const uint16_t& columnIndex);

        const vector<Block*>& GetData() const;

        void PrintRow() const;

        const uint32_t& GetRowSize() const;

        vector<column_index_t> GetLargeBlocks();

        void UpdateRowSize();

        char* GetLargeObjectValue(const DataObjectPointer &objectPointer) const;

        void SetBitMapValue(const bit_map_pos_t& position, const bool& value);

        bool GetBitMapValue(const bit_map_pos_t& position) const;

        RowMetaData* GetMetaData();
};