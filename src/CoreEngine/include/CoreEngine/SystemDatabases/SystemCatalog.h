#pragma once
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

        [[nodiscard]] static bool CatalogExists(const DataTypes::StringView& path);
        void UseCatalogDatabase(const ::Memory::IAllocator* allocator, const DataTypes::StringView& dbName);
        void CreateCatalogDatabase(const ::Memory::IAllocator* allocator, const DataTypes::StringView& dbName);
        void StoreSystemTablesToCatalog(
            const ExecutionContext& baseContext,
            const DataTypes::StringView& dbNameView,
            const DataTypes::StringView& dbPathView
        )const;

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
            Int defaultRoleId
        )const;

        [[nodiscard]] Errors::RuntimeStatus InsertDbToMasterDb(
            const ExecutionContext& executionContext,
            const DataTypes::StringView& dbName,
            const DataTypes::StringView& dbPath,
            bool isSystem = false,
            const DataTypes::StringView& user = "system",
            Int version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertSchemaToMasterDb(
            const ExecutionContext& executionContext,
            Int databaseId,
            const DataTypes::StringView& schemaName,
            const DataTypes::StringView& user = "system",
            Int version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertTableToMasterDb(
            const ExecutionContext& executionContext,
            Int databaseId,
            Int schemaId,
            const DataTypes::StringView& tableName,
            SmallInt ordinalPosition,
            bool isSystem = false,
            const DataTypes::StringView& user = "system",
            Int version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertColumnToMasterDb(
            const ExecutionContext& executionContext,
            Int tableId,
            const DataTypes::StringView& columnName,
            DataType columnType,
            Int columnSize,
            TinyInt precision,
            TinyInt scale,
            bool isNullable,
            SmallInt ordinalPosition,
            bool isSystem = false,
            const DataTypes::StringView& user = "system",
            Int version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertIndexToMasterDb(
            const ExecutionContext& executionContext,
            Int tableId,
            const DataTypes::StringView& indexName,
            bool isClustered,
            bool isDisabled = false,
            const DataTypes::StringView& user = "system",
            Int version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertIndexColumnToMasterDb(
            const ExecutionContext& executionContext,
            Int indexId,
            Int columnId,
            const int16_t& ordinalPosition,
            bool isIncluded,
            Int version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertConstraintToMasterDb(
            const ExecutionContext& executionContext,
            Int tableId,
            const DataTypes::StringView& constraintName,
            const Schemas::ConstraintType& constraintType,
            bool isDisabled,
            const Int* constraintIndexId,
            const DataTypes::StringView& user = "system",
            Int version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertConstraintColumnToMasterDb(
            const ExecutionContext& executionContext,
            Int constraintId,
            Int columnId,
            SmallInt ordinalPosition,
            Int version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertIdentityColumnToMasterDb(
            const ExecutionContext& executionContext,
            Int tableId,
            Int columnId,
            Int seedValue,
            Int increment,
            BigInt lastValue,
            bool isCached,
            Int cacheBlock,
            Int version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertDefaultValuesToMasterDb(
            const ExecutionContext& executionContext,
            Int columnId,
            const Value& value,
            Int version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertTableStatisticsToMasterDb(
            const ExecutionContext& executionContext,
            Int tableId,
            BigInt rowCount = 0,
            Int rowSize = 0,
            Int pageCount = 0
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertColumnStatisticsToMasterDb(
            const ExecutionContext& executionContext,
            Int columnId,
            BigInt distinctCount = 0,
            BigInt nullCount = 0
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertColumnHistogramsToMasterDb(
            const ExecutionContext& executionContext,
            Int columnId,
            const Value& min,
            const Value& max,
            Int rowCount,
            BigInt distinctCount
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertIndexStatisticsToMasterDb(
            const ExecutionContext& executionContext,
            Int tableId,
            Int indexId,
            BigInt leafPages = 0,
            TinyInt depth = 0,
            const DataTypes::Decimal& averageFragmentation = DataTypes::Decimal(0)
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertRoleToMasterDb(
            const ExecutionContext& executionContext,
            const DataTypes::StringView& roleName,
            const Security::Permission& permissions,
            bool isSystem = true,
            Int version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertUserToMasterDb(
            const ExecutionContext& executionContext,
            const DataTypes::StringView& username,
            const DataTypes::StringView& passwordHash,
            Int roleId,
            bool isActive = false,
            Int version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] DataStructures::PolymorphicArray<Security::Role> SelectRoles(const ::Memory::IAllocator* allocator)const;
        [[nodiscard]] DataStructures::PolymorphicArray<Security::User> SelectUsers(const ::Memory::IAllocator* allocator)const;
        [[nodiscard]] bool DatabaseExists(const ::Memory::IAllocator* allocator, const DataTypes::StringView& dbName) const;
        [[nodiscard]] Catalog::DatabaseHeader SelectDatabase(const ::Memory::IAllocator* allocator, const DataTypes::StringView& name) const;
        [[nodiscard]] Catalog::DatabaseHeader SelectDatabaseById(const ::Memory::IAllocator* allocator, Int databaseId) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::SchemaHeader>  SelectSchemas(const ::Memory::IAllocator* allocator, Int databaseId) const;
        [[nodiscard]] Dictionary<DataTypes::String, Catalog::SchemaHeader>  SelectSchemasToDictionary(const ::Memory::IAllocator* allocator, Int databaseId) const;
        [[nodiscard]] bool SchemaExists(
            const ::Memory::IAllocator* allocator,
            Int databaseId,
            const DataTypes::StringView& schema,
            Int* schemaId = nullptr
        ) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::TableHeader> SelectTables(const ::Memory::IAllocator* allocator, const DataTypes::StringView& dbName) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::TableHeader> SelectTables(
            const ::Memory::IAllocator* allocator,
            Int databaseId
        ) const;
        [[nodiscard]] Catalog::TableHeader SelectTable(const ::Memory::IAllocator* allocator, Int databaseId, Int tableId) const;
        [[nodiscard]] Catalog::TableHeader SelectTable(
            const ::Memory::IAllocator* allocator,
            const DataTypes::StringView& dbName,
            const DataTypes::StringView& tableName
        ) const;
        [[nodiscard]] Catalog::TableHeader SelectTable(
            const ::Memory::IAllocator* allocator,
            Int databaseId,
            const DataTypes::StringView& tableName,
            const DataTypes::StringView& schema
        ) const;

        [[nodiscard]] Catalog::TableDefinition SelectTableDefinition(
            const ::Memory::IAllocator* allocator,
            const Catalog::TableHeader& tableHeader
        ) const;

        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::ConstraintsHeader> SelectConstraints(const ::Memory::IAllocator* allocator, Int tableId) const;
        [[nodiscard]] Catalog::ColumnHeader SelectColumnById(const ::Memory::IAllocator* allocator, Int tableId, Int columnId) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::ColumnHeader> SelectColumns(const ::Memory::IAllocator* allocator, Int tableId) const;
        [[nodiscard]] Dictionary<DataTypes::String, Catalog::ColumnHeader> SelectColumnsToDictionary(const ::Memory::IAllocator* allocator, Int tableId) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::IndexHeader> SelectIndexes(const ::Memory::IAllocator* allocator, Int tableId) const;
        [[nodiscard]] Catalog::IndexHeader SelectIndexById(const ::Memory::IAllocator* allocator, Int indexId) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::IndexColumnsHeader> SelectIndexColumnsByIndexId(const ::Memory::IAllocator* allocator, Int indexId) const;
        [[nodiscard]] Dictionary<Int, Catalog::IndexColumnsHeader> SelectIndexColumnsByIndexIdToDictionary(const ::Memory::IAllocator* allocator, Int indexId) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::IdentityColumnsHeader> SelectIdentityColumnsByTableId(const ::Memory::IAllocator* allocator, Int tableId) const;
        [[nodiscard]] Dictionary<Int , Catalog::IdentityColumnsHeader> SelectIdentityColumnsByTableIdToDictionary(const ::Memory::IAllocator* allocator, Int tableId) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::ConstraintsColumnsHeader> SelectConstraintColumnsByConstraintId(const ::Memory::IAllocator* allocator, Int constraintId) const;
        [[nodiscard]] Dictionary<Int, Catalog::ConstraintsColumnsHeader> SelectConstraintColumnsByConstraintIdToDictionary(const ::Memory::IAllocator* allocator, Int constraintId) const;
        [[nodiscard]] Catalog::DefaultValuesHeader SelectDefaultValueByColumnId(const ::Memory::IAllocator* allocator, Int columnId) const;
        [[nodiscard]] Catalog::TableStatistics SelectTableStatisticsById(const ::Memory::IAllocator* allocator, Int tableId)const;
        [[nodiscard]] Catalog::ColumnStatistics SelectColumnStatisticsById(
            const ::Memory::IAllocator* allocator,
            Int columnId,
            DataType columnType
        )const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::ColumnHistograms> SelectColumnHistogramsByColumnId(
            const ::Memory::IAllocator* allocator,
            Int tableId,
            Int columnId
        )const;
        [[nodiscard]]DataStructures::PolymorphicArray<Catalog::IndexStatistics> SelectIndexStatisticsByTableId(const ::Memory::IAllocator* allocator, Int tableId)const;

        void UpdateIdentityByColumnId(
            const ::Memory::IAllocator* allocator,
            Int tableId,
            Int columnId,
            BigInt lastValue
        )const;
        void UpdateTableStatisticsById(
            const ::Memory::IAllocator* allocator,
            Int tableId,
            BigInt rowCount,
            Int rowSize,
            Int pageCount
        )const;
        void UpdateColumnStatisticsById(
            const ::Memory::IAllocator* allocator,
            Int columnId,
            BigInt distinctCount,
            BigInt nullCount,
            const Value& min,
            const Value& max
        )const;
        void UpdateIndexStatisticsById(
            const ::Memory::IAllocator* allocator,
            Int tableId,
            Int indexId,
            BigInt leafPages,
            TinyInt depth,
            const DataTypes::Decimal& averageFragmentation
        )const;
        [[nodiscard]] Errors::RuntimeStatus UpdateHistogramBucket(
            const ::Memory::IAllocator* allocator,
            Int columnId,
            Int histogramId,
            const Value& min,
            const Value& max,
            Int rowCount,
            const BigInt& distinctCount
        ) const;
        [[nodiscard]] Errors::RuntimeStatus UpdateColumnById(
            const ::Memory::IAllocator* allocator,
            Int columnId,
            const DataStructures::PolymorphicArray<Value>& updates
        )const;

        [[nodiscard]] Errors::RuntimeStatus UpdateUserById(
            const ExecutionContext& context,
            const DataTypes::StringView& username,
            Int userId,
            Int roleId
        )const;
  };
}