#pragma once
#include <CoreEngine/DataStorage/ExtentReservation.h>
#include <CoreEngine/DataStorage/Row/SerializedRow.h>
#include <CoreEngine/DatabaseConstants.h>
#include <CoreEngine/SystemDatabases/CatalogHeaders.h>
#include <CoreEngine/Managers/IdentityManager.h>      // _identities is stored by value and used inline
#include <CoreEngine/Indexing/BTree.h>
#include <CoreEngine/Logger/Logger.h>
#include <CoreEngine/BufferPool/FileManager.h>
#include <CoreEngine/Memory/Allocator.h>
#include <CoreEngine/Memory/PersistentAllocator.h>

namespace Expressions
{
    struct EvaluationContext;
}

namespace Pages{
    class LobDataView;
}

namespace CoreEngine::StorageTypes{
    class IdentityManager;
    struct ChunkInsertState;
    struct InsertPlan;
    struct RowSerializationContext;
    class SerializedRow;
}

namespace QueryPipeline::Statements {
    struct Expression;
}

class Value;

namespace Indexing{
    class BTree;
}

namespace CoreEngine{
    struct DataChunk;
    struct DataVector;
    struct ScanHandle;
    struct ScanState;
    struct SelectionVector;
    struct sysTable;
    class Database;
}

namespace ByteMaps{
    class BitMap;
}

namespace CoreEngine::StorageTypes{
    struct TableHeader{
            DataStructures::StaticArray<page_id_t, 10> nonClusteredIndexPageIds;
            mutable page_id_t _allocationPageId;
            mutable page_id_t _clusteredIndexPageId;

            mutable AllocationCursor _allocationCursor;

            TableHeader()
                :   _allocationPageId(INVALID_PAGE_ID),
                    _clusteredIndexPageId(INVALID_PAGE_ID){}
            ~TableHeader() = default;

            page_id_t GetAllocationPageId() const;
            void SetAllocationPageId(page_id_t allocationPageId) const;

            page_id_t GetClusteredIndexPageId() const;
            void SetClusteredIndexPageId(page_id_t indexPageId) const;
    };
    static_assert(std::is_trivially_copyable_v<TableHeader>);

    class Table final{
        TableHeader _physicalHeader;

        const Schemas::TableSchema* _schema;

        DataStructures::PolymorphicArray<IdentityManager> _identities;
        DataStructures::PolymorphicArray<Indexing::BTree*> _nonClusteredTrees;

        Memory::PersistentAllocator _allocator;

        Database* _db;
        Indexing::BTree* _clusteredTree;

        static bool VectorContainsIndex(const DataStructures::PolymorphicArray<column_index_t>& vector, column_index_t index, int& indexPosition);

        /**
        * @name Index and Pages protected Functions
        * Functions to manage indexes and pages.
        * @{
        */
        [[nodiscard]] Pages::IndexPageView GetIndexFromDisk(page_id_t indexPageId) const;

        [[nodiscard]]
        page_id_t InsertLargeObject(
            const ::Memory::IAllocator* allocator,
            const Value& value
        )const;
        page_id_t StoreLargeObject(
            ExtentReservation& reservation,
            const Value& value
        )const;

        /** @} End of: Class Constructors and Destructors*/

        void InsertExistingRowsToNonClusteredIndexByClusteredIndex(Int indexPos, Int pagesToAllocate);
        void InsertExistingRowToNonClusteredIndexByHeap(Int indexPos, Int pagesToAllocate);
        void RemoveColumnByClusteredIndex(column_index_t index);
        void RemoveColumnByHeap(column_index_t index)const;
        [[nodiscard]]
        static RID InsertToVersionDatabase(
            const ::Memory::IAllocator* allocator,
            const Pages::RawRowReference& rowRef
        );

        /**
        * @name Row Insert Functions
        * @{
        */
        void PrepareChunkSources(
            const ExecutionContext& context,
            const InsertPlan& plan,
            const ChunkInsertState& state
        );

        /** @} End of: Row Insert Functions*/

        [[nodiscard]] IdentityManager* IdentityOf(const column_index_t ordinal){
            const auto* identity = this->Column(ordinal)->_identity;
            assert(identity != nullptr && "Table: column has no identity");
            return &this->_identities[identity->_slot];
        }

        template<DataTypes::IsInteger T>
        [[nodiscard]] T GenerateIdentityValue(const ::Memory::IAllocator* allocator, const column_index_t ordinal){
            auto* manager = this->IdentityOf(ordinal);
            return manager->Generate<T>(allocator);
        }

        template<DataTypes::IsInteger T>
        [[nodiscard]] T ReserveIdentityRange(
            const ::Memory::IAllocator* allocator,
            const column_index_t ordinal,
            T rowCount
        ){
            auto* manager = this->IdentityOf(ordinal);
            return manager->ReserveRange<T>(allocator, rowCount);
        }

        [[nodiscard]] Value GenerateIdentityValue(const ::Memory::IAllocator* allocator, column_index_t ordinal);

        public:
            template<typename ValueProvider>
            void EvaluateRow(RowSerializationContext& context, ValueProvider&& provider);

            [[nodiscard]] Errors::RuntimeStatus PlanRow(const RowSerializationContext& context, UnsignedInt& outRowSize)const;

            void WriteOffRowValues(const RowSerializationContext& context)const;

            [[nodiscard]] SerializedRow WriteRow(const RowSerializationContext& context, UnsignedInt rowSize)const;

            template<typename ValueProvider>
            SerializedRow SerializeRowGeneric(
                Errors::RuntimeStatus& status,
                RowSerializationContext& rowContext,
                ValueProvider&& provider
            );

            SerializedRow SerializeRow(
                Errors::RuntimeStatus& status,
                RowSerializationContext& rowContext,
                const DataStructures::PolymorphicArray<Value>& values
            );
        /**
        * @name Class Constructors and Destructors
        * Functions to create and destroy Table objects.
        * @{
        */
            Table(
                const Schemas::TableSchema* schema,
                const TableHeader& physicalHeader,
                Database *database
            );
            void Destroy()const;

        /** @} End of: Class Constructors and Destructors*/

        /**
        * @name Schema Functions
        * Functions for schema Lookups and Updates.
        * @{
        */

        [[nodiscard]] const Schemas::TableSchema* Schema()const { return this->_schema; }
        [[nodiscard]] const Schemas::ColumnSchema* Column(const column_index_t ordinal) const{
            return &this->_schema->_columns[ordinal];
        }
        [[nodiscard]] std::span<const Schemas::ColumnSchema> Columns() const{
            return {this->_schema->_columns, this->_schema->_columnCount};
        }
        [[nodiscard]] column_number_t ColumnCount()const{
            return this->_schema->_columnCount;
        }
        [[nodiscard]] bool HasIdentity(const column_index_t ordinal)const{
            return this->_schema->_columns[ordinal]._identity != nullptr;
        }

        void PersistIdentities()const;
        /** @} End of: Schema Functions*/

        /**
        * @name Insert Functions
        * Functions to insert rows into the table.
        * @{
        */
            Errors::RuntimeStatus ChunkInsert(
                const ExecutionContext& executionContext,
                const DataChunk* chunk,
                const InsertPlan& plan
            );
            Errors::RuntimeStatus SystemInsertRow(
                const ExecutionContext& executionContext,
                const DataStructures::PolymorphicArray<Value> &inputData
            );
            Errors::RuntimeStatus InsertRowPayload(
                const ExecutionContext& executionContext,
                ExtentReservation& extentReservation,
                SerializedRow& payload
            );
            Errors::RuntimeStatus HeapInsertToNewPage(
                ExtentReservation& extentReservation,
                const SerializedRow& payload
            )const;
            Errors::RuntimeStatus HeapInsert(
                const ExecutionContext& executionContext,
                ExtentReservation& extentReservation,
                const SerializedRow& payload
            )const;
            Errors::RuntimeStatus ClusteredIndexInsert(
                const ExecutionContext& executionContext,
                ExtentReservation& extentReservation,
                SerializedRow& payload
            );
            // Errors::RuntimeStatus NonClusteredIndexInsert(
            //     const Row* row,
            //     Int nonClusteredIndexId,
            //     Int pagesToAllocate,
            //     const DataTypes::RowIdentifier& data
            // );
            Errors::RuntimeStatus NonClusteredIndexInsertExistingRows(
                Int indexPos,
                Int pagesToAllocate
            );

        /** @} End of: Insert Functions*/

        /**
        * @name Metadata Accessor Functions
        * Functions to access table metadata.
        * @{
        */
            [[nodiscard]] Storage::FileKey GetSystemFileKey() const;
            [[nodiscard]] Storage::FileKey GetDataFileKey() const;
            [[nodiscard]] const TableHeader& GetPhysicalHeader() const { return this->_physicalHeader; }
            [[nodiscard]] bool IsClustered()const{
                return this->_schema->_clusteredIndexOrdinalPosition != Schemas::TableSchema::NONE;
            }
            [[nodiscard]] const Schemas::IndexSchema* GetNonClusteredIndexes(const Int indexPos) const{
                return &this->_schema->_indexes[this->IsClustered() + indexPos];
            }
            [[nodiscard]] const Schemas::IndexSchema* GetClusteredIndex() const{
                return &this->_schema->_indexes[this->_schema->_clusteredIndexOrdinalPosition];
            }
            [[nodiscard]] table_id_t GetTableId() const{
                return this->_schema->_id;
            }
            [[nodiscard]] Constants::TableType GetType() const{
                return this->IsClustered()
                   ? Constants::TableType::CLUSTERED
                   : Constants::TableType::HEAP;
            }
            [[nodiscard]] UnsignedSmallInt GetOrdinalPosition() const{
                return this->_schema->_ordinalPosition;
            }


        /** @} End of: Metadata Accessor Functions*/

        /**
        * @name Scan and Seek Functions
        * Functions to access rows using scans and seeks.
        * @{
        */
            void ClusteredIndexSeekRange(
                const ExecutionContext& executionContext,
                DataStructures::PolymorphicArray<RID>* selectedRows,
                const DataTypes::Indexing::Key& minKey,
                const DataTypes::Indexing::Key& maxKey,
                const Expressions::Expression* expression,
                const DataStructures::PolymorphicArray<FilterColumnInfo>& filterColumns,
                UnsignedSmallInt slotIndex,
                UnsignedSmallInt schemaWidth
            );
            void ClusteredIndexSeek(
                const ExecutionContext& executionContext,
                DataStructures::PolymorphicArray<RID>* selectedRows,
                const DataTypes::Indexing::Key& key,
                const Expressions::Expression* expression,
                const DataStructures::PolymorphicArray<FilterColumnInfo>& filterColumns,
                UnsignedSmallInt slotIndex,
                UnsignedSmallInt schemaWidth
            );
            void SystemClusteredIndexSeek(
                const ::Memory::IAllocator* allocator,
                DataStructures::PolymorphicArray<RID>* selectedRows,
                const DataTypes::Indexing::Key& key,
                const Expressions::Expression* expression
            );
            void ClusteredIndexScan(
                const ExecutionContext& executionContext,
                DataStructures::PolymorphicArray<RID>* selectedRows,
                IndexState& state,
                const Expressions::Expression* expression,
                const DataStructures::PolymorphicArray<FilterColumnInfo>& filterColumns,
                UnsignedSmallInt slotIndex,
                UnsignedSmallInt schemaWidth
            );
            void ClusteredIndexScan(
                const ExecutionContext& executionContext,
                DataStructures::PolymorphicArray<RID>* selectedRows,
                const Expressions::Expression* expression,
                const DataStructures::PolymorphicArray<FilterColumnInfo>& filterColumns,
                const Storage::FileKey* fileKeys
            );
            void SystemClusteredIndexScan(
                const ::Memory::IAllocator* allocator,
                DataStructures::PolymorphicArray<RID>* selectedRows,
                const Expressions::Expression* expression
            );
            void NonClusteredIndexScan(
                const ExecutionContext& executionContext,
                DataStructures::PolymorphicArray<RID>* selectedRows,
                Int indexPos,
                IndexState& state,
                const Expressions::Expression* expression
            );
            void HeapScan(
                const ExecutionContext& executionContext,
                DataStructures::PolymorphicArray<RID>* result,
                ScanState& state
            )const;
            void TemporaryDatabaseHeapScan(
                DataStructures::PolymorphicArray<RID>* result,
                ScanState& state,
                Int batchSize
            )const;


        /** @} End of: Scan and Seek Functions*/

        /**
        * @name Update Functions
        * Functions to update rows in the table.
        * @{
        */
            Errors::RuntimeStatus HeapUpdate(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression,
                const DataStructures::PolymorphicArray<Value> &updates
            );
            Errors::RuntimeStatus HeapUpdate(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression,
                const DataStructures::PolymorphicArray<Expressions::Expression*> &updates
            );
            void ClusteredIndexScanUpdate(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression,
                const DataStructures::PolymorphicArray<Value> &updates
            );
            [[nodiscard]] Errors::RuntimeStatus ClusteredIndexScanUpdate(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression,
                const DataStructures::PolymorphicArray<Expressions::Expression*> &updates
            );
            [[nodiscard]] Errors::RuntimeStatus ClusteredIndexSeekUpdate(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression,
                const DataTypes::Indexing::Key* minimumValue,
                const DataTypes::Indexing::Key* maximumValue,
                const DataStructures::PolymorphicArray<Value> &updates
            );
            [[nodiscard]] Errors::RuntimeStatus ClusteredIndexSeekUpdate(
                const ExecutionContext& executionContext,
                const DataTypes::Indexing::Key& key,
                const DataStructures::PolymorphicArray<Value> &updates
            );
            [[nodiscard]] Errors::RuntimeStatus SystemClusteredIndexSeekUpdate(
                const ::Memory::IAllocator* allocator,
                const DataTypes::Indexing::Key& key,
                const DataStructures::PolymorphicArray<Value> &updates
            );
            [[nodiscard]]
            Errors::RuntimeStatus UpdateRowNoLock(
                const Pages::PageView* page,
                const RID* rid,
                const ExecutionContext& context,
                const DataStructures::PolymorphicArray<Value>& updates
            );
            [[nodiscard]]
            Errors::RuntimeStatus UpdateRowNoLock(
                const Pages::PageView* page,
                const RID* row,
                const ExecutionContext& context,
                const DataStructures::PolymorphicArray<Expressions::Expression*>& updates
            );
            [[nodiscard]]
            Errors::RuntimeStatus SystemUpdateRowNoLock(
                const Pages::PageView* page,
                const RID* row,
                const ::Memory::IAllocator* allocator,
                const DataStructures::PolymorphicArray<Value>& updates
            );
        /** @} End of: Update Functions*/

        /**
        * @name Delete Functions
        * Functions to delete rows in the table.
        * @{
        */
            void HeapDelete(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression
            ) const;
            void ClusteredIndexScanDelete(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression,
                IndexState& state
            );
            void ClusteredIndexSeekDelete(
                const ExecutionContext& executionContext,
                const Expressions::Expression* expression,
                IndexState& state
            );

        /** @} End of: Delete Functions*/

        /**
        * @name Validation Functions
        * Function to validate table structure
        * @{
        */
            [[nodiscard]] Errors::RuntimeStatus ValidateTableLayout(const ::Memory::IAllocator* allocator)const;
        /** @} End of: Validation Functions*/

        /**
        * @name Calculation and Utility Functions
        * Mostly general utility functions for the table.
        * @{
        */
            [[nodiscard]] row_size_t MaxInlineRowSize()const;
            [[nodiscard]] row_size_t ClusteredKeySize()const;
            [[nodiscard]] row_size_t WorstCaseRowSize()const;
            [[nodiscard]] bool IsKeyColumn(column_index_t ordinalPosition)const;
            [[nodiscard]] bool CanStoreColumnOffRow(column_index_t ordinalPosition)const;

            [[nodiscard]] const ::Memory::IAllocator* Allocator()const;
        /** @} End of: Calculation and Utility Functions*/

        /**
        * @name Materialization Functions
        * @{
        */
            MaterializedRow MaterializeFromPage(const::Memory::IAllocator* allocator, const RID* row) const;

            template<typename T>
            static void MaterializeColumnFromPage(
                const Storage::FileKey* fileKeys,
                const ::Memory::IAllocator* allocator,
                const DataStructures::PolymorphicArray<RID>& rids,
                DataVector* __restrict__ _vector,
                column_index_t ordinalPosition
            );

            template<typename T>
            static void MaterializeColumn(
                const ExecutionContext& context,
                const SelectionVector* sv,
                DataVector* __restrict__ _vector,
                UnsignedSmallInt slotIndex,
                column_index_t ordinalPosition
            );
        /** @} End of: Materialization Functions*/

        /**
        * @name Page and Index Management Functions
        * Functions that manage indexes and pages
        * @{
        */
            Int CreateNonClusteredIndex(const DataStructures::PolymorphicArray<column_index_t>& columnIndices);
            void UpdateAllocationPageId(page_id_t allocationPageId) const;
            page_id_t GetAllocationPageId()const;

            [[nodiscard]] page_id_t GetClusteredIndexPageId() const;
            void SetClusteredIndexPageId(page_id_t pageId) const;

            [[nodiscard]] page_id_t GetNonClusteredIndexPageId(Int indexPosition) const;
            void SetNonClusteredIndexPageId(page_id_t indexPageId, Int indexPosition);

            Indexing::BTree* GetClusteredIndexedTree();
            Indexing::BTree* GetNonClusteredIndexTree(Int nonClusteredIndexId);
            [[nodiscard]] bool HasNonClusteredIndexes() const{
                return this->_schema->_indexesCount > this->IsClustered();
            }

            DataTypes::Indexing::Key CreateKey(
                const ExecutionContext& executionContext,
                const std::span<const Schemas::IndexKeyColumn>& columnKeys,
                const SerializedRow& payload
            ) const;

            void DeleteLargeObjectFromPage(RID* rowPtr, const HashSet<column_index_t>& updatedColumns);
            void DeleteOverflowedRowsFromPage(RID* rowPtr, const HashSet<column_index_t>& updatedColumns)const;

            ExtentReservation ReserveExtents(
                const ::Memory::IAllocator* allocator,
                Int requiredPages
            )const;

            ExtentReservation LazyReservation(const ::Memory::IAllocator* allocator)const;

        /** @} End of: Page and Index Management Functions*/

            void Truncate();

            [[nodiscard]] key_size_t CalculateIndexKeySize(Int indexPos = -1) const;
            [[nodiscard]] Database* GetDatabase() const;

            int HandleRowOverflow(RID* rowPtr) const;

            [[nodiscard]] bool IsEmpty()const;

            void PopulateColumn(column_index_t index, const Value& defaultValue);
            void PopulateColumnByClusteredIndex(column_index_t index, const Value& defaultValue);
            void PopulateColumnByHeap(column_index_t index, const Value& defaultValue);

            void Rollback(const Snapshot& snapshot, const DataTypes::RowIdentifier& rowId)const;
        /**
        * @name Table Altering Functions
        * Functions that alter the structure of the table.
        * @{
        */
            void ReplaceSchema(const Schemas::TableSchema* newSchema);

        /** @} End of System Catalog Integration Functions */

        /**
        * @name System Catalog Integration Functions
        * Functions to retrieve and update system catalog information related to the table.
        * @{
        */
            void UpdateSystemCatalog(const ::Memory::IAllocator* allocator);
            void UpdateCatalogIdentityColumns(const ::Memory::IAllocator* allocator)const;

        /** @} End of System Catalog Integration Functions */

        void SetPrimaryKeyIndexedColumns(const column_index_t* _array, Int size);
    };
}
