#pragma once
#include <array>
#include <string>
#include <vector>

#include <meta>
#include <optional>

#include <CoreEngine/Errors.h>
#include <CoreEngine/SystemDatabases/CatalogHeaders.h>
#include <CoreEngine/SystemDatabases/CatalogRows.h>
#include <CoreEngine/Contexts/ExecutionContext.h>

namespace QueryPipeline
{
    class CompileContext;
}

namespace CoreEngine {
  struct sysTable;
}

namespace CoreEngine {
  class Database;

    class SystemCatalog {
        SystemCatalog();

        Database* masterDb;

        // Catalog tables by CatalogTables ordinal: fixed for the process lifetime, so no lookup and no lock
        std::array<StorageTypes::Table*, Catalog::SYSTEM_TABLES.size()> _catalogTables{};

        void BindCatalogTables();
        [[nodiscard]] StorageTypes::Table* CatalogTable(const CatalogTables ordinal) const{
            return this->_catalogTables[ordinal];
        }

        [[nodiscard]] static bool CatalogExists(const DataTypes::StringView& path);
        void UseCatalogDatabase(const ::Memory::IAllocator* allocator, const DataTypes::StringView& dbName);
        void CreateCatalogDatabase(const ::Memory::IAllocator* allocator, const DataTypes::StringView& dbName);
        void StoreSystemTablesToCatalog(
            const ExecutionContext& baseContext,
            const DataTypes::StringView& dbNameView,
            const DataTypes::StringView& dbPathView
        )const;

        void ReseedSystemIdentities(const ::Memory::IAllocator* allocator) const;

        static Catalog::DatabaseHeader ToDatabaseHeader(
            const ::Memory::IAllocator* allocator,
            const StorageTypes::RID* rowPtr,
            const StorageTypes::Table* table
        );
        static Catalog::DatabaseHeader ToDatabaseHeader(
            const ::Memory::IAllocator* allocator,
            const StorageTypes::RID* rowPtr,
            const StorageTypes::Table* table,
            DataStructures::PolymorphicArray<Catalog::TableHeader>& dbTables,
            DataStructures::PolymorphicArray<Catalog::SchemaHeader>& schemas
        );
        static Catalog::SchemaHeader ToSchemaHeader(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);
        static Catalog::TableHeader ToTableHeader(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);
        static Catalog::ColumnHeader ToColumnHeader(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);
        static Catalog::IndexHeader ToIndexHeader(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);
        static Catalog::IndexColumnsHeader ToIndexColumnsHeader(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);
        static Catalog::IdentityColumnsHeader ToIdentityColumnsHeader(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);
        static Catalog::ConstraintsHeader ToConstraintsHeader(
            const ::Memory::IAllocator* allocator,
            const StorageTypes::RID* rowPtr,
            const StorageTypes::Table* table,
            DataStructures::PolymorphicArray<Catalog::ConstraintsColumnsHeader>& constraintColumns,
            Catalog::IndexHeader& indexHeader
        );
        static Catalog::ConstraintsColumnsHeader ToConstraintsColumnsHeader(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);
        static Catalog::DefaultValuesHeader ToDefaultValuesHeader(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);
        static Catalog::TableStatistics ToTableStatistics(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);
        static Catalog::ColumnStatistics ToColumnStatistics(
            const ::Memory::IAllocator* allocator,
            const StorageTypes::RID* rowPtr,
            const StorageTypes::Table* table,
            DataType columnType
        );
        static Catalog::ColumnHistograms ToColumnHistograms(
            const ::Memory::IAllocator* allocator,
            const StorageTypes::RID* rowPtr,
            const StorageTypes::Table* table,
            DataType columnType
        );
        static Catalog::IndexStatistics ToIndexStatistics(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);

    public:
        SystemCatalog(const SystemCatalog&) = delete;
        SystemCatalog& operator=(const SystemCatalog&) = delete;
        SystemCatalog(SystemCatalog&&) = delete;
        SystemCatalog& operator=(SystemCatalog&&) = delete;

        static SystemCatalog& Get();

        [[nodiscard]] Database* GetDatabase()const;

        /**
         * @name Generic catalog row access
         * Typed reads and writes of any catalog table through its row struct (CatalogRows.h).
         * Defined in SystemCatalog.Templates.h: include it in the translation units that call them.
         * @{
         */

        // Inserts the row. An Identity member left at 0 is generated; the result carries the new primary key.
        template <Catalog::CatalogRow TRow>
        [[nodiscard]] Errors::RuntimeStatus Insert(const ExecutionContext& context, const TRow& row) const;

        template<Catalog::CatalogRow TRow>
        [[nodiscard]] Errors::RuntimeStatus Insert(const ExecutionContext& context, TRow& row) const;

        // Rows whose primary key starts with keyPrefix, e.g. Select<Catalog::SysColumnRow>(allocator, tableId)
        template <Catalog::CatalogRow TRow, DataTypes::Primitive... TKey> requires (sizeof...(TKey) > 0)
        [[nodiscard]] DataStructures::PolymorphicArray<TRow> Select(
            const ::Memory::IAllocator* allocator,
            const TKey&... keyPrefix
        ) const;

        // The row with exactly this primary key, if any
        template <Catalog::CatalogRow TRow, DataTypes::Primitive... TKey> requires (sizeof...(TKey) > 0)
        [[nodiscard]] std::optional<TRow> SelectOne(
            const ::Memory::IAllocator* allocator,
            const TKey&... key
        ) const;

        // Overwrites the row with row's primary key. With no Fields every non-key column is written,
        // otherwise only the listed members, e.g. Update<^^Catalog::SysIdentityColumnRow::lastValue>(allocator, row)
        template <std::meta::info... Fields, Catalog::CatalogRow TRow>
        [[nodiscard]] Errors::RuntimeStatus Update(const ::Memory::IAllocator* allocator, const TRow& row) const;

        /** @} End of: Generic catalog row access */

        [[nodiscard]]bool Initialize(const ExecutionContext& baseContext);
        void Shutdown();

        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::DatabaseHeader> RetrieveCatalog()const;

        [[nodiscard]] DataStructures::PolymorphicArray<Security::Role*> InsertSystemRoles(const ExecutionContext& baseContext)const;
        [[nodiscard]] Security::User InsertSystemUsers(
            const ExecutionContext& baseContext,
            const DataTypes::String& hashedPassword,
            Int32_t defaultRoleId
        )const;

        [[nodiscard]] Errors::RuntimeStatus InsertDbToMasterDb(
            const ExecutionContext& executionContext,
            const DataTypes::StringView& dbName,
            const DataTypes::StringView& dbPath,
            bool isSystem = false,
            const DataTypes::StringView& user = "system",
            Int32_t version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertSchemaToMasterDb(
            const ExecutionContext& executionContext,
            Int32_t databaseId,
            const DataTypes::StringView& schemaName,
            const DataTypes::StringView& user = "system",
            Int32_t version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertTableToMasterDb(
            const ExecutionContext& executionContext,
            Int32_t databaseId,
            Int32_t schemaId,
            const DataTypes::StringView& tableName,
            Int16_t ordinalPosition,
            bool isSystem = false,
            const DataTypes::StringView& user = "system",
            Int32_t version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertColumnToMasterDb(
            const ExecutionContext& executionContext,
            Int32_t tableId,
            const DataTypes::StringView& columnName,
            DataType columnType,
            Int32_t columnSize,
            Int8_t precision,
            Int8_t scale,
            bool isNullable,
            Int16_t ordinalPosition,
            bool isSystem = false,
            const DataTypes::StringView& user = "system",
            Int32_t version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertIndexToMasterDb(
            const ExecutionContext& executionContext,
            Int32_t tableId,
            const DataTypes::StringView& indexName,
            bool isClustered,
            bool isDisabled = false,
            const DataTypes::StringView& user = "system",
            Int32_t version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertIndexColumnToMasterDb(
            const ExecutionContext& executionContext,
            Int32_t indexId,
            Int32_t columnId,
            const int16_t& ordinalPosition,
            bool isIncluded,
            Int32_t version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertConstraintToMasterDb(
            const ExecutionContext& executionContext,
            Int32_t tableId,
            const DataTypes::StringView& constraintName,
            const Schemas::ConstraintType& constraintType,
            bool isDisabled,
            const Int32_t* constraintIndexId,
            const DataTypes::StringView& user = "system",
            Int32_t version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertConstraintColumnToMasterDb(
            const ExecutionContext& executionContext,
            Int32_t constraintId,
            Int32_t columnId,
            Int16_t ordinalPosition,
            Int32_t version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertIdentityColumnToMasterDb(
            const ExecutionContext& executionContext,
            Int32_t tableId,
            Int32_t columnId,
            Int32_t seedValue,
            Int32_t increment,
            Int64_t lastValue,
            bool isCached,
            Int32_t cacheBlock,
            Int32_t version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertDefaultValuesToMasterDb(
            const ExecutionContext& executionContext,
            Int32_t columnId,
            const Value& value,
            Int32_t version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertTableStatisticsToMasterDb(
            const ExecutionContext& executionContext,
            Int32_t tableId,
            Int64_t rowCount = 0,
            Int32_t rowSize = 0,
            Int32_t pageCount = 0
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertColumnStatisticsToMasterDb(
            const ExecutionContext& executionContext,
            Int32_t columnId,
            Int64_t distinctCount = 0,
            Int64_t nullCount = 0
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertColumnHistogramsToMasterDb(
            const ExecutionContext& executionContext,
            Int32_t columnId,
            const Value& min,
            const Value& max,
            Int32_t rowCount,
            Int64_t distinctCount
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertIndexStatisticsToMasterDb(
            const ExecutionContext& executionContext,
            Int32_t tableId,
            Int32_t indexId,
            Int64_t leafPages = 0,
            Int8_t depth = 0,
            const DataTypes::Decimal& averageFragmentation = DataTypes::Decimal(0)
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertRoleToMasterDb(
            const ExecutionContext& executionContext,
            const DataTypes::StringView& roleName,
            const Security::Permission& permissions,
            bool isSystem = true,
            Int32_t version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertUserToMasterDb(
            const ExecutionContext& executionContext,
            const DataTypes::StringView& username,
            const DataTypes::StringView& passwordHash,
            Int32_t roleId,
            bool isActive = false,
            Int32_t version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] DataStructures::PolymorphicArray<Security::Role> SelectRoles(const ::Memory::IAllocator* allocator)const;
        [[nodiscard]] DataStructures::PolymorphicArray<Security::User> SelectUsers(const ::Memory::IAllocator* allocator)const;
        [[nodiscard]] bool DatabaseExists(const ::Memory::IAllocator* allocator, const DataTypes::StringView& dbName) const;
        [[nodiscard]] Catalog::DatabaseHeader SelectDatabase(const ::Memory::IAllocator* allocator, const DataTypes::StringView& name) const;
        [[nodiscard]] Catalog::DatabaseHeader SelectDatabaseById(const ::Memory::IAllocator* allocator, Int32_t databaseId) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::SchemaHeader>  SelectSchemas(const ::Memory::IAllocator* allocator, Int32_t databaseId) const;
        [[nodiscard]] Dictionary<DataTypes::String, Catalog::SchemaHeader>  SelectSchemasToDictionary(const ::Memory::IAllocator* allocator, Int32_t databaseId) const;
        [[nodiscard]] bool SchemaExists(
            const ::Memory::IAllocator* allocator,
            Int32_t databaseId,
            const DataTypes::StringView& schema,
            Int32_t* schemaId = nullptr
        ) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::TableHeader> SelectTables(const ::Memory::IAllocator* allocator, const DataTypes::StringView& dbName) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::TableHeader> SelectTables(
            const ::Memory::IAllocator* allocator,
            Int32_t databaseId
        ) const;
        [[nodiscard]] Catalog::TableHeader SelectTable(const ::Memory::IAllocator* allocator, Int32_t databaseId, Int32_t tableId) const;
        [[nodiscard]] Catalog::TableHeader SelectTable(
            const ::Memory::IAllocator* allocator,
            const DataTypes::StringView& dbName,
            const DataTypes::StringView& tableName
        ) const;
        [[nodiscard]] Catalog::TableHeader SelectTable(
            const ::Memory::IAllocator* allocator,
            Int32_t databaseId,
            const DataTypes::StringView& tableName,
            const DataTypes::StringView& schema
        ) const;

        [[nodiscard]] Catalog::TableDefinition SelectTableDefinition(
            const ::Memory::IAllocator* allocator,
            const Catalog::TableHeader& tableHeader
        ) const;

        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::ConstraintsHeader> SelectConstraints(const ::Memory::IAllocator* allocator, Int32_t tableId) const;
        [[nodiscard]] Catalog::ColumnHeader SelectColumnById(const ::Memory::IAllocator* allocator, Int32_t tableId, Int32_t columnId) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::ColumnHeader> SelectColumns(const ::Memory::IAllocator* allocator, Int32_t tableId) const;
        [[nodiscard]] Dictionary<DataTypes::String, Catalog::ColumnHeader> SelectColumnsToDictionary(const ::Memory::IAllocator* allocator, Int32_t tableId) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::IndexHeader> SelectIndexes(const ::Memory::IAllocator* allocator, Int32_t tableId) const;
        [[nodiscard]] Catalog::IndexHeader SelectIndexById(const ::Memory::IAllocator* allocator, Int32_t indexId) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::IndexColumnsHeader> SelectIndexColumnsByIndexId(const ::Memory::IAllocator* allocator, Int32_t indexId) const;
        [[nodiscard]] Dictionary<Int32_t, Catalog::IndexColumnsHeader> SelectIndexColumnsByIndexIdToDictionary(const ::Memory::IAllocator* allocator, Int32_t indexId) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::IdentityColumnsHeader> SelectIdentityColumnsByTableId(const ::Memory::IAllocator* allocator, Int32_t tableId) const;
        [[nodiscard]] Dictionary<Int32_t , Catalog::IdentityColumnsHeader> SelectIdentityColumnsByTableIdToDictionary(const ::Memory::IAllocator* allocator, Int32_t tableId) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::ConstraintsColumnsHeader> SelectConstraintColumnsByConstraintId(const ::Memory::IAllocator* allocator, Int32_t constraintId) const;
        [[nodiscard]] Dictionary<Int32_t, Catalog::ConstraintsColumnsHeader> SelectConstraintColumnsByConstraintIdToDictionary(const ::Memory::IAllocator* allocator, Int32_t constraintId) const;
        [[nodiscard]] Catalog::DefaultValuesHeader SelectDefaultValueByColumnId(const ::Memory::IAllocator* allocator, Int32_t columnId) const;
        [[nodiscard]] Catalog::TableStatistics SelectTableStatisticsById(const ::Memory::IAllocator* allocator, Int32_t tableId)const;
        [[nodiscard]] Catalog::ColumnStatistics SelectColumnStatisticsById(
            const ::Memory::IAllocator* allocator,
            Int32_t columnId,
            DataType columnType
        )const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::ColumnHistograms> SelectColumnHistogramsByColumnId(
            const ::Memory::IAllocator* allocator,
            Int32_t tableId,
            Int32_t columnId
        )const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::IndexStatistics> SelectIndexStatisticsByTableId(const ::Memory::IAllocator* allocator, Int32_t tableId)const;

        void UpdateIdentityByColumnId(
            const ::Memory::IAllocator* allocator,
            Int32_t tableId,
            Int32_t columnId,
            Int64_t lastValue
        )const;
        void UpdateTableStatisticsById(
            const ::Memory::IAllocator* allocator,
            Int32_t tableId,
            Int64_t rowCount,
            Int32_t rowSize,
            Int32_t pageCount
        )const;
        void UpdateColumnStatisticsById(
            const ::Memory::IAllocator* allocator,
            Int32_t columnId,
            Int64_t distinctCount,
            Int64_t nullCount,
            const Value& min,
            const Value& max
        )const;
        void UpdateIndexStatisticsById(
            const ::Memory::IAllocator* allocator,
            Int32_t tableId,
            Int32_t indexId,
            Int64_t leafPages,
            Int8_t depth,
            const DataTypes::Decimal& averageFragmentation
        )const;
        [[nodiscard]] Errors::RuntimeStatus UpdateHistogramBucket(
            const ::Memory::IAllocator* allocator,
            Int32_t columnId,
            Int32_t histogramId,
            const Value& min,
            const Value& max,
            Int32_t rowCount,
            const Int64_t& distinctCount
        ) const;
        [[nodiscard]] Errors::RuntimeStatus UpdateColumnById(
            const ::Memory::IAllocator* allocator,
            Int32_t columnId,
            const DataStructures::PolymorphicArray<Value>& updates
        )const;

        [[nodiscard]] Errors::RuntimeStatus UpdateUserById(
            const ExecutionContext& context,
            const DataTypes::StringView& username,
            Int32_t userId,
            Int32_t roleId
        )const;
  };
}