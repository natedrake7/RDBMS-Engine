#pragma once
#include <string>
#include <vector>

#include "../../../QueryPipeline/include/PhysicalPlan.h"
#include "../Contexts/ExecutionContext.h"

namespace QueryPipeline
{
    class CompileContext;
}

namespace Headers {
  struct sysTable;
}

namespace CoreEngine {
  class Database;

    class SystemCatalog {
        SystemCatalog();

        Database* masterDb;
        std::vector<Headers::sysTable> sysTables;

        std::tuple<DataTypes::String, DataTypes::String> ReadConfiguration(const ::Memory::IAllocator* allocator, const DataTypes::StringView& configPath);
        [[nodiscard]] static bool CatalogExists(const DataTypes::StringView& path);
        void UseCatalogDatabase(const ::Memory::IAllocator* allocator, const DataTypes::String& dbName);
        void CreateCatalogDatabase(const ::Memory::IAllocator* allocator, const DataTypes::String& dbName);
        void StoreSystemTablesToCatalog(
            const ExecutionContext& baseContext,
            const DataTypes::StringView& dbNameView,
            const DataTypes::StringView& dbPathView
        )const;

        static Headers::DatabaseHeader ToDatabaseHeader(
            const ::Memory::IAllocator* allocator,
            const StorageTypes::RID* rowPtr,
            const StorageTypes::Table* table
        );
        static Headers::DatabaseHeader ToDatabaseHeader(
            const ::Memory::IAllocator* allocator,
            const StorageTypes::RID* rowPtr,
            const StorageTypes::Table* table,
            DataStructures::PolymorphicArray<Headers::TableHeader>& dbTables,
            DataStructures::PolymorphicArray<Headers::SchemaHeader>& schemas
        );
        static Headers::SchemaHeader ToSchemaHeader(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);
        static Headers::TableHeader ToTableHeader(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);
        static Headers::ColumnHeader ToColumnHeader(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);
        static Headers::IndexHeader ToIndexHeader(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);
        static Headers::IndexColumnsHeader ToIndexColumnsHeader(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);
        static Headers::IdentityColumnsHeader ToIdentityColumnsHeader(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);
        static Headers::ConstraintsHeader ToConstraintsHeader(
            const ::Memory::IAllocator* allocator,
            const StorageTypes::RID* rowPtr,
            const StorageTypes::Table* table,
            DataStructures::PolymorphicArray<Headers::ConstraintsColumnsHeader>& constraintColumns,
            Headers::IndexHeader& indexHeader
        );
        static Headers::ConstraintsColumnsHeader ToConstraintsColumnsHeader(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);
        static Headers::DefaultValuesHeader ToDefaultValuesHeader(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);
        static Headers::TableStatistics ToTableStatistics(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);
        static Headers::ColumnStatistics ToColumnStatistics(
            const ::Memory::IAllocator* allocator,
            const StorageTypes::RID* rowPtr,
            const StorageTypes::Table* table,
            DataType columnType
        );
        static Headers::ColumnHistograms ToColumnHistograms(
            const ::Memory::IAllocator* allocator,
            const StorageTypes::RID* rowPtr,
            const StorageTypes::Table* table,
            DataType columnType
        );
        static Headers::IndexStatistics ToIndexStatistics(const ::Memory::IAllocator* allocator, const StorageTypes::RID* rowPtr, const StorageTypes::Table* table);

    public:
        SystemCatalog(SystemCatalog const&) = delete;
        void operator=(SystemCatalog const&) = delete;
        SystemCatalog(SystemCatalog&&) = delete;
        void operator=(SystemCatalog&&) = delete;

        static SystemCatalog& Get();

        [[nodiscard]] Database* GetDatabase()const;

        [[nodiscard]]bool Initialize(
            const ExecutionContext& baseContext,
            const DataTypes::StringView& configPath
        );
        void Shutdown();

        [[nodiscard]]DataStructures::PolymorphicArray<Headers::DatabaseHeader> RetrieveCatalog()const;

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
            Int ordinalPosition,
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
            const Headers::ConstraintType& constraintType,
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
            Int ordinalPosition,
            Int version = 0,
            bool isDeleted = false
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertIdentityColumnToMasterDb(
            const ExecutionContext& executionContext,
            Int tableId,
            Int columnId,
            Int seedValue,
            Int increment,
            Int lastValue,
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
            const BigInt& rowCount = 0,
            Int rowSize = 0,
            Int pageCount = 0
        ) const;

        [[nodiscard]] Errors::RuntimeStatus InsertColumnStatisticsToMasterDb(
            const ExecutionContext& executionContext,
            Int columnId,
            const BigInt& distinctCount = 0,
            const BigInt& nullCount = 0
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
        [[nodiscard]] Headers::DatabaseHeader SelectDatabase(const ::Memory::IAllocator* allocator, const DataTypes::StringView& name) const;
        [[nodiscard]] Headers::DatabaseHeader SelectDatabaseById(const ::Memory::IAllocator* allocator, Int databaseId) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Headers::SchemaHeader>  SelectSchemas(const ::Memory::IAllocator* allocator, Int databaseId) const;
        [[nodiscard]] Dictionary<DataTypes::String, Headers::SchemaHeader>  SelectSchemasToDictionary(const ::Memory::IAllocator* allocator, Int databaseId) const;
        [[nodiscard]] bool SchemaExists(
            const ::Memory::IAllocator* allocator,
            Int databaseId,
            const DataTypes::StringView& schema,
            Int* schemaId = nullptr
        ) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Headers::TableHeader> SelectTables(const ::Memory::IAllocator* allocator, const DataTypes::StringView& dbName) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Headers::TableHeader> SelectTables(
            const ::Memory::IAllocator* allocator,
            Int databaseId
        ) const;
        [[nodiscard]] Headers::TableHeader SelectTable(
            const ::Memory::IAllocator* allocator,
            const DataTypes::StringView& dbName,
            const DataTypes::StringView& tableName
        ) const;
        [[nodiscard]] Headers::TableHeader SelectTable(
            const ::Memory::IAllocator* allocator,
            Int databaseId,
            const DataTypes::StringView& tableName,
            const DataTypes::StringView& schema
        ) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Headers::ConstraintsHeader> SelectConstraints(const ::Memory::IAllocator* allocator, Int tableId) const;
        [[nodiscard]] Headers::ColumnHeader SelectColumnById(const ::Memory::IAllocator* allocator, Int tableId, Int columnId) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Headers::ColumnHeader> SelectColumns(const ::Memory::IAllocator* allocator, Int tableId) const;
        [[nodiscard]] Dictionary<DataTypes::String, Headers::ColumnHeader> SelectColumnsToDictionary(const ::Memory::IAllocator* allocator, Int tableId) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Headers::IndexHeader> SelectIndexes(const ::Memory::IAllocator* allocator, Int tableId) const;
        [[nodiscard]] Headers::IndexHeader SelectIndexById(const ::Memory::IAllocator* allocator, Int indexId) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Headers::IndexColumnsHeader> SelectIndexColumnsByIndexId(const ::Memory::IAllocator* allocator, Int indexId) const;
        [[nodiscard]] Dictionary<Int, Headers::IndexColumnsHeader> SelectIndexColumnsByIndexIdToDictionary(const ::Memory::IAllocator* allocator, Int indexId) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Headers::IdentityColumnsHeader> SelectIdentityColumnsByTableId(const ::Memory::IAllocator* allocator, Int tableId) const;
        [[nodiscard]] Dictionary<Int , Headers::IdentityColumnsHeader> SelectIdentityColumnsByTableIdToDictionary(const ::Memory::IAllocator* allocator, Int tableId) const;
        [[nodiscard]]DataStructures::PolymorphicArray<Headers::ConstraintsColumnsHeader> SelectConstraintColumnsByConstraintId(const ::Memory::IAllocator* allocator, Int constraintId) const;
        [[nodiscard]] Dictionary<Int, Headers::ConstraintsColumnsHeader> SelectConstraintColumnsByConstraintIdToDictionary(const ::Memory::IAllocator* allocator, Int constraintId) const;
        [[nodiscard]] Headers::DefaultValuesHeader SelectDefaultValueByColumnId(const ::Memory::IAllocator* allocator, Int columnId) const;
        [[nodiscard]] Headers::TableStatistics SelectTableStatisticsById(const ::Memory::IAllocator* allocator, Int tableId)const;
        [[nodiscard]] Headers::ColumnStatistics SelectColumnStatisticsById(
            const ::Memory::IAllocator* allocator,
            Int columnId,
            DataType columnType
        )const;
        [[nodiscard]]DataStructures::PolymorphicArray<Headers::ColumnHistograms> SelectColumnHistogramsByColumnId(
            const ::Memory::IAllocator* allocator,
            Int tableId,
            Int columnId
        )const;
        [[nodiscard]]DataStructures::PolymorphicArray<Headers::IndexStatistics> SelectIndexStatisticsByTableId(const ::Memory::IAllocator* allocator, Int tableId)const;

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