#pragma once
#include <vector>
#include "../DatabaseConstants.h"
#include "../../../QueryPipeline/include/Statements.h"
#include "../../../Systemic/include/Errors.h"
#include "../Pages/Page.h"
#include "../Pages/LargeObjectPage.h"


namespace DatabaseEngine {
    struct Snapshot;
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
        transaction_id_t createdTransactionId;
        transaction_id_t deletedTransactionId;

        Pages::RowVersionPointer olderVersionPointer;

        RowVersioningHeader() {
            this->createdTransactionId = INVALID_TRANSACTION_ID;
            this->deletedTransactionId = 0;
        }

        [[nodiscard]] bool HasOlderVersion()const { return this->olderVersionPointer.pageId != INVALID_PAGE_ID; }
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
        Headers::RowIdentifier Id;
        RowVersioningHeader versionHeader;

        std::vector<Block*> data;

        // mutable std::vector<CachedValue> cache;
        const Table *table;

        bool isCopy;

        // [[nodiscard]] bool IsBlockMaterialized(const int& indexPos)const;
        // [[nodiscard]] const Value& GetMaterializedValue(const int& indexPos)const;
        [[nodiscard]] Value Materialize(Int indexPos)const;

        bool IsDeleted(const Snapshot& snapshot)const;

    public:
        explicit Row(const Table &table);

        explicit Row(
            const Table &table,
            const vector<Block *> &data,
            const ByteMaps::BitMap* nullBitMap
        );

        //constructors
        explicit Row();
        explicit Row(const Row* row);
        Row(const Row &copyRow);
        Row(Row &&otherRow)noexcept;
        explicit Row(const std::vector<const Column*>& columns);

        //assignment operators
        Row& operator=(const Row &copyRow);
        Row& operator=(Row &&otherRow) noexcept;

        ~Row();

        void InsertColumnData(Block *block, column_index_t columnIndex);

        //used by versionDb
        void InsertColumnAtEnd(Block* block);

        //primarily used by the join operation
        Int InsertNewColumn(Block* block);

        Int InsertNewColumnAtBeginning(Block* block);

        void UpdateColumnData(Block *block);

        [[nodiscard]] const std::vector<Block *> &GetData() const;

        [[nodiscard]] std::vector<Block *> &GetData();

        void Print() const;

        [[nodiscard]] std::vector<column_index_t> GetLargeBlocks()const;

        unsigned char *GetLargeObjectValue(const Pages::DataObjectPointer &objectPointer, UnsignedInt *objectSize) const;

        [[nodiscard]] Block* GetLargeObject(const Pages::DataObjectPointer &objectPointer, const Column* column)const;

        [[nodiscard]] Pages::OverflowRow* GetOverflowValue(const Pages::OverflowPointer &objectPointer) const;

        void SetNullBitMapValue(bit_map_pos_t position, bool value) const;

        void SetOverflowBitMapValue(bit_map_pos_t position, bool value) const;

        [[nodiscard]] bool GetNullBitMapValue(bit_map_pos_t position) const;

        [[nodiscard]] bool GetOverflowBitMapValue(bit_map_pos_t position) const;

        RowHeader* GetHeader();

        [[nodiscard]] row_size_t TotalSize() const;

        [[nodiscard]] row_header_size_t GetHeaderSize() const;

        [[nodiscard]] Errors::RuntimeStatus Update(const std::vector<Value> & updates, int& diff)const;

        [[nodiscard]] Errors::RuntimeStatus Update(const std::vector<QueryPipeline::Statements::UpdateColumn*> & updates, int& diff)const;

        [[nodiscard]] Block* FindLargestVariableLengthColumn() const;

        [[nodiscard]] std::vector<Block*> GetBlockCopies() const;

        [[nodiscard]] Value GetColumnByIndex(Int indexPos) const;

        void Join(const Row* row);

        void LeftJoin(const std::vector<const Column*>& innerTableColumns);

        void RightJoin(const std::vector<const Column*>& innerTableColumns);

        [[nodiscard]] const bool& IsCopy()const;

        friend std::ostream& operator<<(std::ostream& os, const Row& row);

        void SetCurrentTransactionId(transaction_id_t transactionId);

        void SetDeletedTransactionId(transaction_id_t transactionId);

        void SetOlderVersionPointer(page_id_t pageId, page_offset_t offset);

        [[nodiscard]] Row GetVisibleVersionForTransaction(const Snapshot& snapshot)const;

        bool IsVisibleForTransaction(const Snapshot& snapshot) const;

        void SetId(page_id_t pageId, Int indexId);

        [[nodiscard]] const Headers::RowIdentifier& GetId() const;

        const RowVersioningHeader& GetVersionHeader() const;

        bool HasOlderVersion()const;

        const Table* GetTable()const;

        //Getters Setters Serializers etc

        void Serialize(std::vector<char>* buffer, page_offset_t& pos)const;

        void Serialize(object_t*& buffer, page_offset_t &offSet)const;

        void Deserialize(const std::vector<char>* buffer, page_offset_t& pos);

        void ReadHeaderFromDisk(const object_t* buffer, page_offset_t &offSet);

        void ReadVersionHeaderFromDisk(const object_t* buffer, page_offset_t &offSet);

        void ReadDataFromDisk(const object_t* buffer, page_offset_t &offSet, const std::vector<Column*>& columns);

        void ReadDataFromDisk(const object_t*& buffer, page_offset_t &offSet);

        void WriteHeaderToDisk(fstream* filePtr)const;

        void WriteVersionHeaderToDisk(fstream* filePtr)const;

        void WriteDataToDisk(fstream* filePtr)const;

        QueryResult AsQueryResult()const;

        [[nodiscard]] bool IsInvalid()const;
    };
}