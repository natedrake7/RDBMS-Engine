#pragma once
#include <vector>
#include "../Constants.h"

namespace DatabaseEngine {
    class Database;
}

namespace Pages {
    struct DataObjectPointer;
}

namespace ByteMaps {
    class BitMap;
}

using namespace std;
using namespace Constants;

namespace DatabaseEngine::StorageTypes {
    class Table;
    class Block;

    typedef struct RowHeader {
        ByteMaps::BitMap* nullBitMap;
        ByteMaps::BitMap* largeObjectBitMap;
        row_size_t rowSize;
        size_t maxRowSize;

        RowHeader();
        ~RowHeader();
    }RowHeader;

    class Row
    {
        RowHeader header;
        vector<Block*> data;
        const Table* table;

    public:
        explicit Row(const Table& table);

        explicit Row(const Table& table, const vector<Block*>& data);

        Row(const Row& copyRow);

        ~Row();

        void InsertColumnData(Block* block, const column_index_t& columnIndex);

        void UpdateColumnData(Block *block);

        const vector<Block*>& GetData() const;

        void PrintRow() const;

        const uint32_t& GetRowSize() const;

        vector<column_index_t> GetLargeBlocks();

        void UpdateRowSize();

        unsigned char* GetLargeObjectValue(const Pages::DataObjectPointer& objectPointer, uint32_t* objectSize) const;

        void SetNullBitMapValue(const bit_map_pos_t& position, const bool& value);

        bool GetNullBitMapValue(const bit_map_pos_t& position) const;

        RowHeader* GetHeader();

        row_size_t GetTotalRowSize() const;

        row_header_size_t GetRowHeaderSize() const;
    };
}