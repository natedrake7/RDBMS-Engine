#pragma once
#include <vector>
#include "../Constants.h"
#include "../../QueryPipeline/Statements/Statements.h"
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

    struct CachedValue {
        Value value;
        bool isMaterialized;

        CachedValue();
    };

    class Row
    {
        RowHeader header;
        std::vector<Block *> data;

        mutable std::vector<CachedValue> cache;
        const Table *table;

        bool isCopy;

        [[nodiscard]] bool IsBlockMaterialized(const int& indexPos)const;
        [[nodiscard]] const Value& GetMaterializedValue(const int& indexPos)const;
        [[nodiscard]] const Value& Materialize(const int& indexPos)const;

    public:
        explicit Row(const Table &table);

        explicit Row(
            const Table &table,
            const vector<Block *> &data,
            const ByteMaps::BitMap* nullBitMap
        );

        Row(const Row &copyRow);

        explicit Row(const Row* row);

        Row& operator=(const Row &copyRow);

        ~Row();

        void InsertColumnData(Block *block, const column_index_t &columnIndex);

        //primarily used by the join operation
        [[nodiscard]] int InsertNewColumn(Block* block);

        void UpdateColumnData(Block *block);

        [[nodiscard]] const std::vector<Block *> &GetData() const;

        [[nodiscard]] std::vector<Block *> &GetData();

        void PrintRow() const;

        [[nodiscard]] const uint32_t &GetRowSize() const;

        [[nodiscard]] std::vector<column_index_t> GetLargeBlocks()const;

        void UpdateRowSize();

        unsigned char *GetLargeObjectValue(const Pages::DataObjectPointer &objectPointer, uint32_t *objectSize) const;

        [[nodiscard]] Block* GetLargeObject(const Pages::DataObjectPointer &objectPointer, const Column* column)const;

        [[nodiscard]] Pages::OverflowRow* GetOverflowValue(const Pages::OverflowPointer &objectPointer) const;

        void SetNullBitMapValue(const bit_map_pos_t &position, const bool &value) const;

        void SetOverflowBitMapValue(const bit_map_pos_t &position, const bool &value) const;

        [[nodiscard]] bool GetNullBitMapValue(const bit_map_pos_t &position) const;

        [[nodiscard]] bool GetOverflowBitMapValue(const bit_map_pos_t &position) const;

        RowHeader *GetHeader();

        [[nodiscard]] row_size_t GetTotalRowSize() const;

        [[nodiscard]] row_header_size_t GetRowHeaderSize() const;

        [[nodiscard]] AdditionalDataTypes::ResultStatus Update(const std::vector<Value> & updates, int& diff);

        [[nodiscard]] AdditionalDataTypes::ResultStatus Update(const std::vector<QueryPipeline::Statements::UpdateColumn*> & updates, int& diff);

        [[nodiscard]] Block* FindLargestVariableLengthColumn() const;

        [[nodiscard]] std::vector<Block*> GetBlockCopies() const;

        [[nodiscard]] const Value& GetColumnByIndex(const int& indexPos) const;

        void Serialize(std::vector<char>* buffer, uint32_t& pos)const;

        void Deserialize(const std::vector<char>* buffer, uint32_t& pos);

        [[nodiscard]] Row* Join(const Row* row) const;

        void LeftJoin(const Row* row) const;

        [[nodiscard]] const bool& IsCopy()const;

        friend std::ostream& operator<<(std::ostream& os, const Row& row);
    };
}