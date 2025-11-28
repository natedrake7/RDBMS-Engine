#pragma once
#include <vector>
#include "../Constants.h"
#include "../../QueryPipeline/Statements/Statements.h"
#include "../../Systemic/Errors/Errors.h"
#include "../Pages/Page.h"
#include "../Pages/LargeObject/LargeObjectPage.h"


namespace QueryPipeline::PhysicalPlan {
    struct Snapshot;
}

namespace Server {
    class ServerInstance;
}

namespace Pages {
    struct OverflowPointer;
    struct OverflowRow;
}

namespace ByteMaps{
    class BitMap;
}

namespace DatabaseEngine::StorageTypes
{
    class Table;
    class Block;

    struct RowVersioningHeader {
        Constants::transaction_id_t createdTransactionId;
        Constants::transaction_id_t deletedTransactionId;

        Pages::RowVersionPointer olderVersionPointer;

        RowVersioningHeader() {
            this->createdTransactionId = Constants::INVALID_TRANSACTION_ID;
            this->deletedTransactionId = 0;
        }

        [[nodiscard]] bool HasOlderVersion()const { return this->olderVersionPointer.pageId != Constants::INVALID_PAGE_ID; }
    };

    struct RowHeader{
        table_id_t tableId;
        column_number_t numberOfColumns;

        ByteMaps::BitMap *nullBitMap;
        ByteMaps::BitMap *largeObjectBitMap;
        ByteMaps::BitMap *overflowBitMap;

        RowHeader();
        ~RowHeader();
        RowHeader& operator= (const RowHeader& otherHeader);
    };

    struct CachedValue {
        Value value;
        bool isMaterialized;

        CachedValue();
    };

    class Row{
        RowHeader header;
        RowVersioningHeader versionHeader;

        std::vector<Block *> data;

        mutable std::vector<CachedValue> cache;
        const Table *table;

        bool isCopy;

        [[nodiscard]] bool IsBlockMaterialized(const int& indexPos)const;
        [[nodiscard]] const Value& GetMaterializedValue(const int& indexPos)const;
        [[nodiscard]] const Value& Materialize(const int& indexPos)const;

        bool IsDeleted(const QueryPipeline::PhysicalPlan::Snapshot& snapshot)const;

    public:
        explicit Row(const Table &table);

        explicit Row(
            const Table &table,
            const vector<Block *> &data,
            const ByteMaps::BitMap* nullBitMap
        );

        explicit Row(const std::vector<const Column*>& columns);

        Row(const Row &copyRow);

        explicit Row(const Row* row);

        explicit Row();

        Row& operator=(const Row &copyRow);

        ~Row();

        void InsertColumnData(Block *block, const column_index_t &columnIndex);

        //used by versionDb
        void InsertColumnAtEnd(Block* block);

        //primarily used by the join operation
        [[nodiscard]] int InsertNewColumn(Block* block);

        [[nodiscard]] int InsertNewColumnAtBeginning(Block* block);

        void UpdateColumnData(Block *block);

        [[nodiscard]] const std::vector<Block *> &GetData() const;

        [[nodiscard]] std::vector<Block *> &GetData();

        void PrintRow() const;

        [[nodiscard]] std::vector<column_index_t> GetLargeBlocks()const;

        unsigned char *GetLargeObjectValue(const Pages::DataObjectPointer &objectPointer, uint32_t *objectSize) const;

        [[nodiscard]] Block* GetLargeObject(const Pages::DataObjectPointer &objectPointer, const Column* column)const;

        [[nodiscard]] Pages::OverflowRow* GetOverflowValue(const Pages::OverflowPointer &objectPointer) const;

        void SetNullBitMapValue(const bit_map_pos_t &position, const bool &value) const;

        void SetOverflowBitMapValue(const bit_map_pos_t &position, const bool &value) const;

        [[nodiscard]] bool GetNullBitMapValue(const bit_map_pos_t &position) const;

        [[nodiscard]] bool GetOverflowBitMapValue(const bit_map_pos_t &position) const;

        RowHeader* GetHeader();

        [[nodiscard]] row_size_t GetTotalRowSize() const;

        [[nodiscard]] row_header_size_t GetRowHeaderSize() const;

        [[nodiscard]] Errors::RuntimeStatus Update(const std::vector<Value> & updates, int& diff)const;

        [[nodiscard]] Errors::RuntimeStatus Update(const std::vector<QueryPipeline::Statements::UpdateColumn*> & updates, int& diff)const;

        [[nodiscard]] Block* FindLargestVariableLengthColumn() const;

        [[nodiscard]] std::vector<Block*> GetBlockCopies() const;

        [[nodiscard]] const Value& GetColumnByIndex(const int& indexPos) const;

        [[nodiscard]] Row* Join(const Row* row) const;

        [[nodiscard]] Row* LeftJoin(const std::vector<const Column*>& innerTableColumns) const;

        [[nodiscard]] Row* RightJoin(const std::vector<const Column*>& innerTableColumns) const;

        [[nodiscard]] const bool& IsCopy()const;

        friend std::ostream& operator<<(std::ostream& os, const Row& row);

        void SetCurrentTransactionId(const Constants::transaction_id_t& transactionId);

        void SetDeletedTransactionId(const Constants::transaction_id_t& transactionId);

        void SetOlderVersionPointer(const page_id_t& pageId, const page_offset_t& offset);

        const Row* GetVisibleVersionForTransaction(const QueryPipeline::PhysicalPlan::Snapshot& snapshot) const;

        bool IsVisibleForTransaction(const QueryPipeline::PhysicalPlan::Snapshot& snapshot) const;

        const RowVersioningHeader& GetVersionHeader() const;

        //Getters Setters Serializers etc

        void Serialize(std::vector<char>* buffer, uint32_t& pos)const;

        void Deserialize(const std::vector<char>* buffer, uint32_t& pos);

        void ReadHeaderFromDisk(const std::vector<char>& buffer, Constants::page_offset_t& offSet);

        void ReadVersionHeaderFromDisk(const std::vector<char>& buffer, Constants::page_offset_t& offSet);

        void ReadDataFromDisk(const std::vector<char>& buffer, Constants::page_offset_t& offSet, const std::vector<Column*>& columns);

        void ReadDataFromDisk(const std::vector<char>& buffer, Constants::page_offset_t& offSet);

        void WriteHeaderToDisk(fstream* filePtr)const;

        void WriteVersionHeaderToDisk(fstream* filePtr)const;

        void WriteDataToDisk(fstream* filePtr)const;
    };
}