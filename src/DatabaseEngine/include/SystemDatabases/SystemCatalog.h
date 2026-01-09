#pragma once
#include <string>
#include <vector>

#include "../../../QueryPipeline/include/PhysicalPlan.h"

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
    ExecutionProperties baseProperties;

    void ReadConfiguration(const std::string& configPath);
    [[nodiscard]] bool CatalogExists()const;
    void UseCatalogDatabase();
    void CreateCatalogDatabase();
    void StoreSystemTablesToCatalog()const;

    static Headers::DatabaseHeader ToDatabaseHeader(const Pointer<StorageTypes::Row>& row);
    static Headers::DatabaseHeader ToDatabaseHeader(
      const Pointer<StorageTypes::Row>& row,
      std::vector<Headers::TableHeader>& dbTables,
      std::vector<Headers::SchemaHeader>& schemas
    );
    static Headers::SchemaHeader ToSchemaHeader(const Pointer<StorageTypes::Row>& row);
    static Headers::TableHeader ToTableHeader(const Pointer<StorageTypes::Row>& row);
    static Headers::ColumnHeader ToColumnHeader(const Pointer<StorageTypes::Row>& row);
    static Headers::IndexHeader ToIndexHeader(const Pointer<StorageTypes::Row>& row);
    static Headers::IndexColumnsHeader ToIndexColumnsHeader(const Pointer<StorageTypes::Row>& row);
    static Headers::IdentityColumnsHeader ToIdentityColumnsHeader(const Pointer<StorageTypes::Row>& row);
    static Headers::ConstraintsHeader ToConstraintsHeader(
      const Pointer<StorageTypes::Row>& row,
      std::vector<Headers::ConstraintsColumnsHeader>& constraintColumns,
      Headers::IndexHeader& indexHeader
    );
    static Headers::ConstraintsColumnsHeader ToConstraintsColumnsHeader(const Pointer<StorageTypes::Row>& row);
    static Headers::DefaultValuesHeader ToDefaultValuesHeader(const Pointer<StorageTypes::Row>& row);
    static Headers::TableStatistics ToTableStatistics(const Pointer<StorageTypes::Row>& row);
    static Headers::ColumnStatistics ToColumnStatistics(const Pointer<StorageTypes::Row>& row, const DataType& columnType);
    static Headers::ColumnHistograms ToColumnHistograms(const Pointer<StorageTypes::Row>& row, const DataType& columnType);
    static Headers::IndexStatistics ToIndexStatistics(const Pointer<StorageTypes::Row>& row);

    public:
      SystemCatalog(SystemCatalog const&) = delete;
      void operator=(SystemCatalog const&) = delete;
      SystemCatalog(SystemCatalog&&) = delete;
      void operator=(SystemCatalog&&) = delete;

      static SystemCatalog& Get();

      Database* GetDatabase()const;

      [[nodiscard]]bool Initialize(const std::string& configPath);
      void Shutdown();

      std::vector<Headers::DatabaseHeader> RetrieveCatalog()const;

      std::vector<Security::Role*> InsertSystemRoles()const;
      Security::User* InsertSystemUsers(
        const std::string& hashedPassword,
        const Int& defaultRoleId
      )const;

      [[nodiscard]] Errors::RuntimeStatus InsertDbToMasterDb(
          const ExecutionProperties& properties,
          const std::string& dbName,
          const std::string& dbPath,
          const bool& isSystem = false,
          const std::string& user = "system",
          const int& version = 0,
          const bool& isDeleted = false
      ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertSchemaToMasterDb(
        const ExecutionProperties& properties,
        const Int& databaseId,
        const std::string& schemaName,
        const std::string& user = "system",
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertTableToMasterDb(
        const ExecutionProperties& properties,
        const Int & databaseId,
        const Int & schemaId,
        const std::string& tableName,
        const int16_t& ordinalPosition,
        const bool& isSystem = false,
        const std::string& user = "system",
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertColumnToMasterDb(
        const ExecutionProperties& properties,
        const Int & tableId,
        const std::string& columnName,
        const DataType& columnType,
        const int& columnSize,
        const TinyInt& precision,
        const TinyInt& scale,
        const bool& isNullable,
        const int& ordinalPosition,
        const bool& isSystem = false,
        const std::string& user = "system",
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertIndexToMasterDb(
        const ExecutionProperties& properties,
        const Int & tableId,
        const std::string &indexName,
        const bool &isClustered,
        const bool &isDisabled = false,
        const std::string& user = "system",
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertIndexColumnToMasterDb(
      const ExecutionProperties& properties,
        const Int& indexId,
        const Int& columnId,
        const int16_t& ordinalPosition,
        const bool& isIncluded,
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertConstraintToMasterDb(
      const ExecutionProperties& properties,
        const Int& tableId,
        const string& constraintName,
        const Headers::ConstraintType& constraintType,
        const bool& isDisabled,
        const Int* constraintIndexId,
        const std::string& user = "system",
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertConstraintColumnToMasterDb(
      const ExecutionProperties& properties,
        const Int& constraintId,
        const Int& columnId,
        const Int& ordinalPosition,
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertIdentityColumnToMasterDb(
      const ExecutionProperties& properties,
        const Int& tableId,
        const Int& columnId,
        const Int& seedValue,
        const Int& increment,
        const Int& lastValue,
        const bool& isCached,
        const Int& cacheBlock,
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertDefaultValuesToMasterDb(
      const ExecutionProperties& properties,
        const Int& columnId,
        const Value& value,
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertTableStatisticsToMasterDb(
      const ExecutionProperties& properties,
      const Int& tableId,
      const BigInt& rowCount = 0,
      const Int& rowSize = 0,
      const Int& pageCount = 0
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertColumnStatisticsToMasterDb(
      const ExecutionProperties& properties,
      const Int& columnId,
      const BigInt& distinctCount = 0,
      const BigInt& nullCount = 0
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertColumnHistogramsToMasterDb(
      const Int& columnId,
      const Value& min,
      const Value& max,
      const Int& rowCount,
      const BigInt& distinctCount
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertIndexStatisticsToMasterDb(
      const ExecutionProperties& properties,
      const Int& tableId,
      const Int& indexId,
      const BigInt& leafPages = 0,
      const TinyInt& depth = 0,
      const DataTypes::Decimal& averageFragmentation = DataTypes::Decimal(0)
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertRoleToMasterDb(
      const ExecutionProperties& properties,
      const std::string& roleName,
      const Security::Permission& permissions,
      const bool& isSystem = true,
      const int& version = 0,
      const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertUserToMasterDb(
      const ExecutionProperties& properties,
      const std::string& username,
      const std::string& passwordHash,
      const Int& roleId,
      const bool& isActive = false,
      const int& version = 0,
      const bool& isDeleted = false
    ) const;

    [[nodiscard]] std::vector<Security::Role> SelectRoles()const;
    [[nodiscard]] std::vector<Security::User> SelectUsers()const;
    [[nodiscard]] bool DatabaseExists(const string& dbName) const;
    [[nodiscard]] Headers::DatabaseHeader SelectDatabase(const std::string& name) const;
    [[nodiscard]] Headers::DatabaseHeader SelectDatabaseById(const Int& databaseId) const;
    [[nodiscard]] std::vector<Headers::SchemaHeader>  SelectSchemas(const Int& databaseId) const;
    [[nodiscard]] Dictionary<std::string, Headers::SchemaHeader>  SelectSchemasToDictionary(const Int& databaseId) const;
    [[nodiscard]] bool SchemaExists(
      const Int &databaseId,
      const std::string& schema,
      int* schemaId = nullptr
    ) const;
    [[nodiscard]] std::vector<Headers::TableHeader> SelectTables(const string& dbName) const;
    [[nodiscard]] std::vector<Headers::TableHeader> SelectTables(const Int & databaseId) const;
    [[nodiscard]] Headers::TableHeader SelectTable(const string& dbName, const string& tableName) const;
    [[nodiscard]] Headers::TableHeader SelectTable(const Int &databaseId, const string &tableName, const std::string& schema) const;
    [[nodiscard]] std::vector<Headers::ConstraintsHeader> SelectConstraints(const Int& tableId) const;
    [[nodiscard]] Headers::ColumnHeader SelectColumnById(const Int& tableId, const Int& columnId) const;
    [[nodiscard]] std::vector<Headers::ColumnHeader> SelectColumns(const Int& tableId) const;
    [[nodiscard]] Dictionary<string, Headers::ColumnHeader> SelectColumnsToDictionary(const Int& tableId) const;
    [[nodiscard]] std::vector<Headers::IndexHeader> SelectIndexes(const Int& tableId) const;
    [[nodiscard]] Headers::IndexHeader SelectIndexById(const Int& indexId) const;
    [[nodiscard]] std::vector<Headers::IndexColumnsHeader> SelectIndexColumnsByIndexId(const Int& indexId) const;
    [[nodiscard]] Dictionary<Int, Headers::IndexColumnsHeader> SelectIndexColumnsByIndexIdToDictionary(const Int& indexId) const;
    [[nodiscard]] std::vector<Headers::IdentityColumnsHeader> SelectIdentityColumnsByTableId(const Int& tableId) const;
    [[nodiscard]] Dictionary<Int , Headers::IdentityColumnsHeader> SelectIdentityColumnsByTableIdToDictionary(const Int& tableId) const;
    [[nodiscard]] std::vector<Headers::ConstraintsColumnsHeader> SelectConstraintColumnsByConstraintId(const Int& constraintId) const;
    [[nodiscard]] Dictionary<Int, Headers::ConstraintsColumnsHeader> SelectConstraintColumnsByConstraintIdToDictionary(const Int& constraintId) const;
    [[nodiscard]] Headers::DefaultValuesHeader SelectDefaultValueByColumnId(const Int& columnId) const;
    [[nodiscard]] Headers::TableStatistics SelectTableStatisticsById(const Int& tableId)const;
    [[nodiscard]] Headers::ColumnStatistics SelectColumnStatisticsById(
      const Int& columnId,
      const DataType& columnType
    )const;
    [[nodiscard]] std::vector<Headers::ColumnHistograms> SelectColumnHistogramsByColumnId(
      const Int& tableId,
      const Int& columnId
    )const;
    [[nodiscard]] std::vector<Headers::IndexStatistics> SelectIndexStatisticsByTableId(const Int& tableId)const;

    void UpdateIdentityByColumnId(const Int & tableId, const Int& columnId, const BigInt& lastValue)const;
    void UpdateTableStatisticsById(
      const Int& tableId,
      const BigInt& rowCount,
      const Int& rowSize,
      const Int& pageCount
    )const;
    void UpdateColumnStatisticsById(
      const Int& columnId,
      const BigInt& distinctCount,
      const BigInt& nullCount,
      const Value& min,
      const Value& max
    )const;
    void UpdateIndexStatisticsById(
      const Int& tableId,
      const Int& indexId,
      const BigInt& leafPages,
      const TinyInt& depth,
      const DataTypes::Decimal& averageFragmentation
    )const;
    [[nodiscard]] Errors::RuntimeStatus UpdateHistogramBucket(
      const Int& columnId,
      const Int& histogramId,
      const Value& min,
      const Value& max,
      const Int& rowCount,
      const BigInt& distinctCount
    ) const;
    [[nodiscard]]Errors::RuntimeStatus UpdateColumnById(const Int& columnId, const std::vector<Value>& updates)const;

    [[nodiscard]] Errors::RuntimeStatus UpdateUserById(
      const std::string& username,
      const Int &userId,
      const Int &roleId
    )const;

  };
}