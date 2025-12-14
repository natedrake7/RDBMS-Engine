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

    SystemCatalog(SystemCatalog const&) = delete;
    void operator=(SystemCatalog const&) = delete;
    SystemCatalog(SystemCatalog&&) = delete;
    void operator=(SystemCatalog&&) = delete;

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

    static Headers::DatabaseHeader ToDatabaseHeader(const DatabaseEngine::StorageTypes::Row* row);
    static Headers::DatabaseHeader ToDatabaseHeader(
      const DatabaseEngine::StorageTypes::Row* row,
      std::vector<Headers::TableHeader>& dbTables,
      std::vector<Headers::SchemaHeader>& schemas
    );
    static Headers::SchemaHeader ToSchemaHeader(const DatabaseEngine::StorageTypes::Row* row);
    static Headers::TableHeader ToTableHeader(const DatabaseEngine::StorageTypes::Row* row);
    static Headers::ColumnHeader ToColumnHeader(const DatabaseEngine::StorageTypes::Row* row);
    static Headers::IndexHeader ToIndexHeader(const DatabaseEngine::StorageTypes::Row* row);
    static Headers::IndexColumnsHeader ToIndexColumnsHeader(const DatabaseEngine::StorageTypes::Row* row);
    static Headers::IdentityColumnsHeader ToIdentityColumnsHeader(const DatabaseEngine::StorageTypes::Row* row);
    static Headers::ConstraintsHeader ToConstraintsHeader(
      const DatabaseEngine::StorageTypes::Row* row,
      std::vector<Headers::ConstraintsColumnsHeader>& constraintColumns,
      Headers::IndexHeader& indexHeader
    );
    static Headers::ConstraintsColumnsHeader ToConstraintsColumnsHeader(const DatabaseEngine::StorageTypes::Row* row);
    static Headers::DefaultValuesHeader ToDefaultValuesHeader(const DatabaseEngine::StorageTypes::Row* row);
    static Headers::TableStatistics ToTableStatistics(const DatabaseEngine::StorageTypes::Row* row);
    static Headers::ColumnStatistics ToColumnStatistics(const DatabaseEngine::StorageTypes::Row* row, const DataType& columnType);
    static Headers::ColumnHistograms ToColumnHistograms(const DatabaseEngine::StorageTypes::Row* row, const DataType& columnType);

    public:
      static SystemCatalog& Get();

      Database* GetDatabase()const;

      [[nodiscard]]bool Initialize(const std::string& configPath);
      void Shutdown();

      std::vector<Headers::DatabaseHeader> RetrieveCatalog()const;

      std::vector<Security::Role*> InsertSystemRoles()const;
      Security::User* InsertSystemUsers(
        const std::string& hashedPassword,
        const int32_t& defaultRoleId
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
        const int32_t& databaseId,
        const std::string& schemaName,
        const std::string& user = "system",
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertTableToMasterDb(
        const ExecutionProperties& properties,
        const int32_t & databaseId,
        const int32_t & schemaId,
        const std::string& tableName,
        const int16_t& ordinalPosition,
        const bool& isSystem = false,
        const std::string& user = "system",
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertColumnToMasterDb(
        const ExecutionProperties& properties,
        const int32_t & tableId,
        const std::string& columnName,
        const DataType& columnType,
        const int& columnSize,
        const int8_t& precision,
        const int8_t& scale,
        const bool& isNullable,
        const int& ordinalPosition,
        const bool& isSystem = false,
        const std::string& user = "system",
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertIndexToMasterDb(
        const ExecutionProperties& properties,
        const int32_t & tableId,
        const std::string &indexName,
        const bool &isClustered,
        const bool &isDisabled = false,
        const std::string& user = "system",
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertIndexColumnToMasterDb(
      const ExecutionProperties& properties,
        const int32_t& indexId,
        const int32_t& columnId,
        const int16_t& ordinalPosition,
        const bool& isIncluded,
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertConstraintToMasterDb(
      const ExecutionProperties& properties,
        const int32_t& tableId,
        const string& constraintName,
        const Headers::ConstraintType& constraintType,
        const bool& isDisabled,
        const int32_t* constraintIndexId,
        const std::string& user = "system",
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertConstraintColumnToMasterDb(
      const ExecutionProperties& properties,
        const int32_t& constraintId,
        const int32_t& columnId,
        const int32_t& ordinalPosition,
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertIdentityColumnToMasterDb(
      const ExecutionProperties& properties,
        const int32_t& tableId,
        const int32_t& columnId,
        const int32_t& seedValue,
        const int32_t& increment,
        const int32_t& lastValue,
        const bool& isCached,
        const int32_t& cacheBlock,
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertDefaultValuesToMasterDb(
      const ExecutionProperties& properties,
        const int32_t& columnId,
        const Value& value,
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertTableStatisticsToMasterDb(
      const ExecutionProperties& properties,
      const int32_t& tableId,
      const int64_t& rowCount = 0,
      const int32_t& rowSize = 0,
      const int32_t& pageCount = 0
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertColumnStatisticsToMasterDb(
      const ExecutionProperties& properties,
      const int32_t& columnId,
      const int64_t& distinctCount = 0,
      const int64_t& nullCount = 0
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertColumnHistogramsToMasterDb(
      const ExecutionProperties& properties,
      const int32_t& columnId,
      const Value& min,
      const Value& max,
      const int64_t& distinctCount = 0
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertIndexStatisticsToMasterDb(
      const ExecutionProperties& properties,
      const int32_t& indexId,
      const int64_t& leafPages = 0,
      const int8_t& depth = 0,
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
      const int32_t& roleId,
      const bool& isActive = false,
      const int& version = 0,
      const bool& isDeleted = false
    ) const;

    [[nodiscard]] std::vector<Security::Role> SelectRoles()const;
    [[nodiscard]] std::vector<Security::User> SelectUsers()const;
    [[nodiscard]] bool DatabaseExists(const string& dbName) const;
    [[nodiscard]] Headers::DatabaseHeader SelectDatabase(const std::string& name) const;
    [[nodiscard]] Headers::DatabaseHeader SelectDatabaseById(const int32_t& databaseId) const;
    [[nodiscard]] std::vector<Headers::SchemaHeader>  SelectSchemas(const int32_t& databaseId) const;
    [[nodiscard]] Dictionary<std::string, Headers::SchemaHeader>  SelectSchemasToDictionary(const int32_t& databaseId) const;
    [[nodiscard]] bool SchemaExists(
      const int32_t &databaseId,
      const std::string& schema,
      int* schemaId = nullptr
    ) const;
    [[nodiscard]] std::vector<Headers::TableHeader> SelectTables(const string& dbName) const;
    [[nodiscard]] std::vector<Headers::TableHeader> SelectTables(const int32_t & databaseId) const;
    [[nodiscard]] Headers::TableHeader SelectTable(const string& dbName, const string& tableName) const;
    [[nodiscard]] Headers::TableHeader SelectTable(const int32_t &databaseId, const string &tableName, const std::string& schema) const;
    [[nodiscard]] std::vector<Headers::ConstraintsHeader> SelectConstraints(const int32_t& tableId) const;
    [[nodiscard]] std::vector<Headers::ColumnHeader> SelectColumns(const int32_t& tableId) const;
    [[nodiscard]] Dictionary<string, Headers::ColumnHeader> SelectColumnsToDictionary(const int32_t& tableId) const;
    [[nodiscard]] std::vector<Headers::IndexHeader> SelectIndexes(const int32_t& tableId) const;
    [[nodiscard]] Headers::IndexHeader SelectIndexById(const int32_t& indexId) const;
    [[nodiscard]] std::vector<Headers::IndexColumnsHeader> SelectIndexColumnsByIndexId(const int32_t& indexId) const;
    [[nodiscard]] Dictionary<int32_t, Headers::IndexColumnsHeader> SelectIndexColumnsByIndexIdToDictionary(const int32_t& indexId) const;
    [[nodiscard]] std::vector<Headers::IdentityColumnsHeader> SelectIdentityColumnsByTableId(const int32_t& tableId) const;
    [[nodiscard]] Dictionary<int32_t , Headers::IdentityColumnsHeader> SelectIdentityColumnsByTableIdToDictionary(const int32_t& tableId) const;
    [[nodiscard]] std::vector<Headers::ConstraintsColumnsHeader> SelectConstraintColumnsByConstraintId(const int32_t& constraintId) const;
    [[nodiscard]] Dictionary<int32_t, Headers::ConstraintsColumnsHeader> SelectConstraintColumnsByConstraintIdToDictionary(const int32_t& constraintId) const;
    [[nodiscard]] Headers::DefaultValuesHeader SelectDefaultValueByColumnId(const int32_t& columnId) const;
    [[nodiscard]] Headers::TableStatistics SelectTableStatisticsById(const int32_t& tableId)const;
    [[nodiscard]] Headers::ColumnStatistics SelectColumnStatisticsById(
      const int32_t& columnId,
      const DataType& columnType
    )const;
    [[nodiscard]] std::vector<Headers::ColumnHistograms> SelectColumnHistogramsByColumnId(
      const int32_t& columnId,
      const DataType& columnType
    )const;

    void UpdateIdentityByColumnId(const int32_t & tableId, const int32_t& columnId, const int64_t& lastValue)const;
    void UpdateTableStatisticsById(
      const int32_t& tableId,
      const int64_t& rowCount,
      const int32_t& rowSize,
      const int32_t& pageCount
    )const;
    void UpdateColumnStatisticsById(
      const int32_t& columnId,
      const int64_t& distinctCount,
      const int64_t& nullCount,
      const Value& min,
      const Value& max
    )const;
    [[nodiscard]]Errors::RuntimeStatus UpdateColumnById(const int32_t& columnId, const std::vector<Value>& updates)const;

    [[nodiscard]] Errors::RuntimeStatus UpdateUserById(
      const std::string& username,
      const int32_t &userId,
      const int32_t &roleId
    )const;

  };
}