#pragma once
#include <string>
#include <vector>

#include "../../../QueryPipeline/include/PhysicalPlan.h"
#include "../Contexts/ExecutionContext.h"

namespace Headers {
  struct sysTable;
}

namespace DatabaseEngine {
  class Database;

  class SystemCatalog {
    SystemCatalog();
    ~SystemCatalog();

    Database* masterDb;

    std::string sysDbName;
    std::string sysDbPath;

    std::vector<Headers::sysTable> sysTables;
    ExecutionContext baseExecutionContext;

    void ReadConfiguration(std::string_view configPath);
    [[nodiscard]] bool CatalogExists()const;
    void UseCatalogDatabase();
    void CreateCatalogDatabase();
    void StoreSystemTablesToCatalog()const;

    static Headers::DatabaseHeader ToDatabaseHeader(const Pages::RowReference& rowPtr);
    static Headers::DatabaseHeader ToDatabaseHeader(
        const Pages::RowReference& rowPtr,
        std::vector<Headers::TableHeader>& dbTables,
        std::vector<Headers::SchemaHeader>& schemas
    );
    static Headers::SchemaHeader ToSchemaHeader(const Pages::RowReference& rowPtr);
    static Headers::TableHeader ToTableHeader(const Pages::RowReference& rowPtr);
    static Headers::ColumnHeader ToColumnHeader(const Pages::RowReference& rowPtr);
    static Headers::IndexHeader ToIndexHeader(const Pages::RowReference& rowPtr);
    static Headers::IndexColumnsHeader ToIndexColumnsHeader(const Pages::RowReference& rowPtr);
    static Headers::IdentityColumnsHeader ToIdentityColumnsHeader(const Pages::RowReference& rowPtr);
    static Headers::ConstraintsHeader ToConstraintsHeader(
        const Pages::RowReference& rowPtr,
        std::vector<Headers::ConstraintsColumnsHeader>& constraintColumns,
        Headers::IndexHeader& indexHeader
    );
    static Headers::ConstraintsColumnsHeader ToConstraintsColumnsHeader(const Pages::RowReference& rowPtr);
    static Headers::DefaultValuesHeader ToDefaultValuesHeader(const Pages::RowReference& rowPtr);
    static Headers::TableStatistics ToTableStatistics(const Pages::RowReference& rowPtr);
    static Headers::ColumnStatistics ToColumnStatistics(const Pages::RowReference& rowPtr, DataType columnType);
    static Headers::ColumnHistograms ToColumnHistograms(const Pages::RowReference& rowPtr, DataType columnType);
    static Headers::IndexStatistics ToIndexStatistics(const Pages::RowReference& rowPtr);

    public:
      SystemCatalog(SystemCatalog const&) = delete;
      void operator=(SystemCatalog const&) = delete;
      SystemCatalog(SystemCatalog&&) = delete;
      void operator=(SystemCatalog&&) = delete;

      static SystemCatalog& Get();

      Database* GetDatabase()const;

      [[nodiscard]]bool Initialize(std::string_view configPath);
      void Shutdown();

      std::vector<Headers::DatabaseHeader> RetrieveCatalog()const;

      std::vector<Security::Role*> InsertSystemRoles()const;
      Security::User* InsertSystemUsers(
        const std::string& hashedPassword,
        Int defaultRoleId
      )const;

      [[nodiscard]] Errors::RuntimeStatus InsertDbToMasterDb(
          const ExecutionContext& executionContext,
          const std::string& dbName,
          const std::string& dbPath,
          bool isSystem = false,
          const std::string& user = "system",
          Int version = 0,
          bool isDeleted = false
      ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertSchemaToMasterDb(
        const ExecutionContext& executionContext,
        Int databaseId,
        const std::string& schemaName,
        const std::string& user = "system",
        Int version = 0,
        bool isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertTableToMasterDb(
        const ExecutionContext& executionContext,
        Int databaseId,
        Int schemaId,
        const std::string& tableName,
        SmallInt ordinalPosition,
        bool isSystem = false,
        const std::string& user = "system",
        Int version = 0,
        bool isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertColumnToMasterDb(
        const ExecutionContext& executionContext,
        Int tableId,
        const std::string& columnName,
        DataType columnType,
        Int columnSize,
        TinyInt precision,
        TinyInt scale,
        bool isNullable,
        Int ordinalPosition,
        bool isSystem = false,
        const std::string& user = "system",
        Int version = 0,
        bool isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertIndexToMasterDb(
        const ExecutionContext& executionContext,
        Int tableId,
        const std::string &indexName,
        bool isClustered,
        bool isDisabled = false,
        const std::string& user = "system",
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
        const std::string& constraintName,
        const Headers::ConstraintType& constraintType,
        bool isDisabled,
        const Int* constraintIndexId,
        const std::string& user = "system",
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
      const std::string& roleName,
      const Security::Permission& permissions,
      bool isSystem = true,
      Int version = 0,
      bool isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertUserToMasterDb(
      const ExecutionContext& executionContext,
      const std::string& username,
      const std::string& passwordHash,
      Int roleId,
      bool isActive = false,
      Int version = 0,
      bool isDeleted = false
    ) const;

    [[nodiscard]] std::vector<Security::Role> SelectRoles()const;
    [[nodiscard]] std::vector<Security::User> SelectUsers()const;
    [[nodiscard]] bool DatabaseExists(const std::string& dbName) const;
    [[nodiscard]] Headers::DatabaseHeader SelectDatabase(const std::string& name) const;
    [[nodiscard]] Headers::DatabaseHeader SelectDatabaseById(Int databaseId) const;
    [[nodiscard]] std::vector<Headers::SchemaHeader>  SelectSchemas(Int databaseId) const;
    [[nodiscard]] Dictionary<std::string, Headers::SchemaHeader>  SelectSchemasToDictionary(Int databaseId) const;
    [[nodiscard]] bool SchemaExists(
      Int databaseId,
      const std::string& schema,
      int* schemaId = nullptr
    ) const;
    [[nodiscard]] std::vector<Headers::TableHeader> SelectTables(const std::string& dbName) const;
    [[nodiscard]] std::vector<Headers::TableHeader> SelectTables(Int databaseId) const;
    [[nodiscard]] Headers::TableHeader SelectTable(const std::string& dbName, const std::string& tableName) const;
    [[nodiscard]] Headers::TableHeader SelectTable(Int databaseId, const std::string &tableName, const std::string& schema) const;
    [[nodiscard]] std::vector<Headers::ConstraintsHeader> SelectConstraints(Int tableId) const;
    [[nodiscard]] Headers::ColumnHeader SelectColumnById(Int tableId, Int columnId) const;
    [[nodiscard]] std::vector<Headers::ColumnHeader> SelectColumns(Int tableId) const;
    [[nodiscard]] Dictionary<std::string, Headers::ColumnHeader> SelectColumnsToDictionary(Int tableId) const;
    [[nodiscard]] std::vector<Headers::IndexHeader> SelectIndexes(Int tableId) const;
    [[nodiscard]] Headers::IndexHeader SelectIndexById(Int indexId) const;
    [[nodiscard]] std::vector<Headers::IndexColumnsHeader> SelectIndexColumnsByIndexId(Int indexId) const;
    [[nodiscard]] Dictionary<Int, Headers::IndexColumnsHeader> SelectIndexColumnsByIndexIdToDictionary(Int indexId) const;
    [[nodiscard]] std::vector<Headers::IdentityColumnsHeader> SelectIdentityColumnsByTableId(Int tableId) const;
    [[nodiscard]] Dictionary<Int , Headers::IdentityColumnsHeader> SelectIdentityColumnsByTableIdToDictionary(Int tableId) const;
    [[nodiscard]] std::vector<Headers::ConstraintsColumnsHeader> SelectConstraintColumnsByConstraintId(Int constraintId) const;
    [[nodiscard]] Dictionary<Int, Headers::ConstraintsColumnsHeader> SelectConstraintColumnsByConstraintIdToDictionary(Int constraintId) const;
    [[nodiscard]] Headers::DefaultValuesHeader SelectDefaultValueByColumnId(Int columnId) const;
    [[nodiscard]] Headers::TableStatistics SelectTableStatisticsById(Int tableId)const;
    [[nodiscard]] Headers::ColumnStatistics SelectColumnStatisticsById(
      Int columnId,
      DataType columnType
    )const;
    [[nodiscard]] std::vector<Headers::ColumnHistograms> SelectColumnHistogramsByColumnId(
      Int tableId,
      Int columnId
    )const;
    [[nodiscard]] std::vector<Headers::IndexStatistics> SelectIndexStatisticsByTableId(Int tableId)const;

    void UpdateIdentityByColumnId(Int tableId, Int columnId, const BigInt& lastValue)const;
    void UpdateTableStatisticsById(
      Int tableId,
      const BigInt& rowCount,
      Int rowSize,
      Int pageCount
    )const;
    void UpdateColumnStatisticsById(
      Int columnId,
      const BigInt& distinctCount,
      const BigInt& nullCount,
      const Value& min,
      const Value& max
    )const;
    void UpdateIndexStatisticsById(
      Int tableId,
      Int indexId,
      const BigInt& leafPages,
      const TinyInt& depth,
      const DataTypes::Decimal& averageFragmentation
    )const;
    [[nodiscard]] Errors::RuntimeStatus UpdateHistogramBucket(
      Int columnId,
      Int histogramId,
      const Value& min,
      const Value& max,
      Int rowCount,
      const BigInt& distinctCount
    ) const;
    [[nodiscard]] Errors::RuntimeStatus UpdateColumnById(Int columnId, const std::vector<Value>& updates)const;

    [[nodiscard]] Errors::RuntimeStatus UpdateUserById(
      const std::string& username,
      Int userId,
      Int roleId
    )const;

  };
}