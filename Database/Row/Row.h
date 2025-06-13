#pragma once
#include <vector>
#include "../Constants.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Expression//Expression.h"
#include "../Pages/OverflowPage/OverflowPage.h"

namespace DatabaseEngine
{
    class Database;
}

namespace Pages
{
    struct DataObjectPointer;
}

namespace ByteMaps
{
    class BitMap;
}

using namespace std;
using namespace Constants;

namespace DatabaseEngine::StorageTypes
{
    class Table;
    class Block;

    typedef struct RowHeader
    {
        ByteMaps::BitMap *nullBitMap;
        ByteMaps::BitMap *largeObjectBitMap;
        ByteMaps::BitMap *overflowBitMap;
        row_size_t rowSize;
        size_t maxRowSize;

        RowHeader();
        ~RowHeader();
        RowHeader& operator= (const RowHeader& otherHeader);
    } RowHeader;

    class Row
    {
        RowHeader header;
        vector<Block *> data;
        const Table *table;

    public:
        explicit Row(const Table &table);

        explicit Row(const Table &table, const vector<Block *> &data, const ByteMaps::BitMap* nullBitMap);

        Row(const Row &copyRow);

        Row& operator=(const Row &copyRow);

        ~Row();

        void PopulateAutoComputedColumns(Table* tablePtr);

        void InsertColumnData(Block *block, const column_index_t &columnIndex);

        //primarily used by the join operation
        void InsertNewColumn(Block* block);

        void UpdateColumnData(Block *block);

        [[nodiscard]] const vector<Block *> &GetData() const;

        [[nodiscard]] vector<Block *> &GetData();

        void PrintRow() const;

        [[nodiscard]] const uint32_t &GetRowSize() const;

        vector<column_index_t> GetLargeBlocks()const;

        void UpdateRowSize();

        unsigned char *GetLargeObjectValue(const Pages::DataObjectPointer &objectPointer, uint32_t *objectSize) const;

        [[nodiscard]] Pages::OverflowRow* GetOverflowValue(const Pages::OverflowPointer &objectPointer) const;

        void SetNullBitMapValue(const bit_map_pos_t &position, const bool &value) const;

        void SetOverflowBitMapValue(const bit_map_pos_t &position, const bool &value) const;

        [[nodiscard]] bool GetNullBitMapValue(const bit_map_pos_t &position) const;

        [[nodiscard]] bool GetOverflowBitMapValue(const bit_map_pos_t &position) const;

        RowHeader *GetHeader();

        [[nodiscard]] row_size_t GetTotalRowSize() const;

        [[nodiscard]] row_header_size_t GetRowHeaderSize() const;

        [[nodiscard]] bool Evaluate(const Expressions::Expression* expression) const;

        [[nodiscard]] int Update(const vector<Field> & updates);

        void DeleteLargeObjectFromPage(const HashSet<column_index_t>& updatedColumns);

        [[nodiscard]] Block* FindLargestVariableLengthColumn() const;

        [[nodiscard]] vector<Block*> GetBlockCopies() const;
    };
}