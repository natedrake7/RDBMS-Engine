#pragma once
#include <vector>
#include "../DatabaseConstants.h"
#include "../../../QueryPipeline/include/Statements.h"
#include "../../../Systemic/include/Errors.h"
#include "../../../Systemic/include/DataStructures/BitMap.h"

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
    class Column;
    class Table;
    class Block;

    struct RowVersionPointer {
        page_id_t pageId;
        page_offset_t offset;

        RowVersionPointer() {
            this->pageId = INVALID_PAGE_ID;
            this->offset = 0;
        }
    };

    struct RowVersioningHeader {
        transaction_id_t createdTransactionId;
        transaction_id_t deletedTransactionId;

        RowVersionPointer olderVersionPointer;

        RowVersioningHeader() {
            this->createdTransactionId = INVALID_TRANSACTION_ID;
            this->deletedTransactionId = 0;
        }

        [[nodiscard]] bool IsVisibleForTransaction(const Snapshot& snapshot)const;
        [[nodiscard]] bool IsDeletedForTransaction(const Snapshot& snapshot)const;

        [[nodiscard]] bool HasOlderVersion()const { return this->olderVersionPointer.pageId != INVALID_PAGE_ID; }
    };

    struct RowHeader{
        ByteMaps::BitMap nullBitMap;
        ByteMaps::BitMap largeObjectBitMap;
        ByteMaps::BitMap overflowBitMap;

        RowVersioningHeader version;

        explicit RowHeader(Int bitMapsSize = 0);
        RowHeader& operator=(const RowHeader& otherHeader);
        RowHeader(const RowHeader& otherHeader);
        RowHeader(RowHeader&& otherHeader) noexcept;
        RowHeader& operator=(RowHeader&& otherHeader) noexcept;

        [[nodiscard]] bool Size()const;
    };

    class Row{
        DataTypes::RowIdentifier Id;
        RowHeader header;

        std::vector<Value> data;
        std::vector<Block*> blocks;

        [[nodiscard]] Value Materialize(Int indexPos)const;
        inline void WriteVersionToBuffer(object_t*& buffer, page_offset_t& offSet)const;
        inline void WriteVersionToBuffer(std::vector<char>& buffer, page_offset_t& offSet)const;

    public:
        /**
         * @name Constructors - Destructors Functions
         * Constructor and Destructor functions
         * @{
         */
            explicit Row();
            explicit Row(const Table &table);
            explicit Row(const std::vector<const Column*>& columns);
            // explicit Row(
            //     const std::vector<Block *> &data,
            //     const ByteMaps::BitMap* nullBitMap
            // );
            // explicit Row(const Row* row);

            Row(const Row &copyRow);
            Row(Row &&otherRow)noexcept;

            Row& operator=(const Row &copyRow);
            Row& operator=(Row &&otherRow) noexcept;

            ~Row();
        /** @} End of Constructors - Destructors Functions */

        /**
         * @name Logging Functions
         * Logging and debugging functions
         * @{
         */
            void Print() const;
            friend std::ostream& operator<<(std::ostream& os, const Row& row);
        /** @} End of Logging Functions */

        /**
         * @name Helper Functions
         * General helper functions for row operations
         * @{
         */
            RowHeader* GetHeader();
            [[nodiscard]] row_size_t TotalSize() const;
            [[nodiscard]] row_header_size_t GetHeaderSize() const;
            [[nodiscard]] Value FindLargestVariableLengthColumn() const;
            [[nodiscard]] QueryResult AsQueryResult()const;
            [[nodiscard]] bool IsInvalid()const;
            [[nodiscard]] bool HasOlderVersion()const;
        /** @} End of Helper Functions */

        /**
         * @name Join Functions
         * Functions used during join operations
         * @{
         */
            void Join(const Row* row);
            void LeftJoin(const std::vector<const Column*>& innerTableColumns);
            void RightJoin(const std::vector<const Column*>& innerTableColumns);
        /** @} End of Join Functions */

        /**
         * @name Insert - Update Functions
         * Functions to insert or update row blocks
         * @{
         */
            //User primarily by Version Database
            void InsertColumnAtEnd(Block* block);
            void InsertColumnData(Block *block, column_index_t columnIndex);
            //primarily used by the join operation
            Int InsertNewColumn(Block* block);
            Int InsertNewColumnAtBeginning(Block* block);
            void UpdateColumnData(Block *block);

            [[nodiscard]] Errors::RuntimeStatus Update(const std::vector<Value> & updates, int& diff);
            [[nodiscard]] Errors::RuntimeStatus Update(const std::vector<QueryPipeline::Statements::UpdateColumn*> & updates, int& diff);
        /** @} End of Insert - Update Functions */

        /**
         * @name Data Retrieval Functions
         * Functions to retrieve data from the row
         * @{
         */
            [[nodiscard]] Value GetColumnByIndex(Int indexPos) const;

            [[nodiscard]] const std::vector<Block *> &GetData() const;
            [[nodiscard]] std::vector<Block *> &GetData();

            [[nodiscard]] std::vector<column_index_t> GetLargeBlocks()const;
            unsigned char *GetLargeObjectValue(page_id_t pageId, UnsignedInt *objectSize) const;
            [[nodiscard]] Block* GetLargeObject(page_id_t pageId, const Column* column)const;

            [[nodiscard]] Pages::OverflowRow* GetOverflowValue(const Pages::OverflowPointer &objectPointer) const;

            [[nodiscard]] Row GetVisibleVersionForTransaction(const Snapshot& snapshot);
        /** @} End of Data Retrieval Functions */

        /**
         * @name Metadata Functions
         * Functions to set and retrieve metadata information
         * @{
         */
            void SetId(page_id_t pageId, Int indexId);

            void SetCurrentTransactionId(transaction_id_t transactionId);
            void SetDeletedTransactionId(transaction_id_t transactionId);
            void SetOlderVersionPointer(page_id_t pageId, page_offset_t offset);

            void SetNullBitMapValue(bit_map_pos_t position, bool value);
            void SetOverflowBitMapValue(bit_map_pos_t position, bool value);

            [[nodiscard]] const DataTypes::RowIdentifier& GetId() const;

            [[nodiscard]] bool GetNullBitMapValue(bit_map_pos_t position) const;
            [[nodiscard]] bool GetOverflowBitMapValue(bit_map_pos_t position) const;

        /** @} End of Metadata Functions */

        /**
         * @name Serialization Functions
         * Functions to serialize and deserialize rows to and from disk
         * @{
         */
            void Serialize(std::vector<char>* buffer, page_offset_t& pos)const;
            void Serialize(object_t*& buffer, page_offset_t &offSet)const;
            void WriteHeaderToBuffer(object_t*& buffer, page_offset_t& offSet)const;
            void WriteHeaderToBuffer(std::vector<char>& buffer, page_offset_t& offSet)const;

            void Deserialize(const std::vector<char>& buffer, page_offset_t& pos, const Table& table);
            static inline RowVersioningHeader PeakVersionHeaderFromDisk(const object_t* buffer, page_offset_t offSet);
            inline void ReadVersionHeaderFromDisk(const object_t* buffer, page_offset_t &offSet);
            inline void ReadVersionHeaderFromDisk(const std::vector<char>& buffer, page_offset_t &offSet);
            void ReadHeaderFromDisk(const object_t* buffer, page_offset_t &offSet);
            void ReadDataFromDisk(const object_t* buffer, page_offset_t &offSet, const std::vector<Column*>& columns);
        /** @} End of Serialization Functions */
    };
}