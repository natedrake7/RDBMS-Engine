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
    row_size_t rowSize;
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

        explicit Row(const Row& copyRow);

        ~Row();

        void InsertColumnData(Block* block, const column_index_t& columnIndex);

        const vector<Block*>& GetData() const;

        void PrintRow() const;

        const uint32_t& GetRowSize() const;

        vector<column_index_t> GetLargeBlocks();

        void UpdateRowSize();

        char* GetLargeObjectValue(const DataObjectPointer &objectPointer) const;

        void SetNullBitMapValue(const bit_map_pos_t& position, const bool& value);

        bool GetNullBitMapValue(const bit_map_pos_t& position) const;

        RowMetaData* GetMetaData();

        row_size_t GetTotalRowSize() const;

        row_metadata_size_t GetRowMetaDataSize() const;
};