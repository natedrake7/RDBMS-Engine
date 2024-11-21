#pragma once

#include "../Table/Table.h"
#include "../Block/Block.h"

class Block;
class Table;
class Database;

using namespace std;

class Row
{
    vector<Block*> data;
    uint32_t rowSize;
    size_t maxRowSize;
    const Table* table;
    bool isOriginalRow;

    public:
        explicit Row(const Table& table);

        explicit Row(const Table& table, const vector<Block*>& data);

        ~Row();

        void InsertColumnData(Block* block, const uint16_t& columnIndex);

        const vector<Block*>& GetData() const;

        void PrintRow() const;

        const uint32_t& GetRowSize() const;

        vector<uint16_t> GetLargeBlocks();

        void UpdateRowSize();

        char* GetLargeObjectValue(const DataObjectPointer &objectPointer) const;

};