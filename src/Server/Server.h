#pragma once
#include "../Systemic/DataTypes/Headers/Headers.h"
#include "../Systemic/Errors/Errors.h"
#include "../Database/Database.h"
#include <string>
#include <vector>

using namespace std;

namespace DatabaseEngine {
  class Database;

  namespace StorageTypes {
    class Row;
  }
}

namespace Server {
  enum MasterDbTables: uint8_t {
    SYSDATABASES = 0,
    SYSSCHEMAS = 1,
    SYSTABLES = 2,
    SYSCOLUMNS = 3,
    SYSINDEXES = 4,
    SYSIDENTITYCOLUMNS = 5,
    SYSINDEXCOLUMNS = 6,
    SYSCONSTRAINTS = 7,
    SYSCONSTRAINTCOLUMNS = 8,
    SYSDEFAULTVALUES = 9,
    SYSTABLESTATS = 10,
    SYSCOLUMNSTATS = 11
  };

  class ServerInstance {
    string sysDbName;
    string sysDbPath;
    vector<Headers::sysTable> sysTables;
    DatabaseEngine::Database* masterDb;

    Dictionary<int32_t, DatabaseEngine::Database*> databases;

    ServerInstance();
    ~ServerInstance();

    void ReadConfiguration(const std::string& configPath);
    void CreateSystemDatabase();
    [[nodiscard]] bool CheckIfMasterDbExists()const;
    

  public:
    static ServerInstance& Get();

    //MasterDB Insert Functions
    void Initialize(const string& configPath);
    Errors::ResultStatus  InsertDbToMasterDb(
      const string& dbName,
      const string& dbPath,
      const bool& isSystem = false,
      const string& user = "system",
      const int& version = 0,
      const bool& isDeleted = false) const;

    Errors::ResultStatus  InsertSchemaToMasterDb(
      const int32_t& databaseId,
      const string& schemaName,
      const string& user = "system",
      const int& version = 0,
      const bool& isDeleted = false) const;

    Errors::ResultStatus  InsertTableToMasterDb(
      const int32_t & databaseId,
      const int32_t & schemaId,
      const string& tableName,
      const int16_t& ordinalPosition,
      const bool& isSystem = false,
      const string& user = "system",
      const int& version = 0,
      const bool& isDeleted = false) const;

    Errors::ResultStatus  InsertColumnToMasterDb(
      const int32_t & tableId,
      const string& columnName,
      const DataType& columnType,
      const int& columnSize,
      const int8_t& precision,
      const int8_t& scale,
      const bool& isNullable,
      const int& ordinalPosition,
      const bool& isSystem = false,
      const string& user = "system",
      const int& version = 0,
      const bool& isDeleted = false) const;

    Errors::ResultStatus  InsertIndexToMasterDb(
      const int32_t & tableId,
      const string &indexName,
      const bool &isClustered,
      const bool &isDisabled = false,
      const string& user = "system",
      const int& version = 0,
      const bool& isDeleted = false) const;

    Errors::ResultStatus InsertIndexColumnToMasterDb(
        const int32_t& indexId,
        const int32_t& columnId,
        const int16_t& ordinalPosition,
        const bool& isIncluded,
        const int& version = 0,
        const bool& isDeleted = false) const;

    Errors::ResultStatus InsertConstraintToMasterDb(
        const int32_t& tableId,
        const std::string& constraintName,
        const Headers::ConstraintType& constraintType,
        const bool& isDisabled,
        const int32_t* constraintIndexId,
        const std::string& user = "system",
        const int& version = 0,
        const bool& isDeleted = false) const;

    Errors::ResultStatus InsertConstraintColumnToMasterDb(
        const int32_t& constraintId,
        const int32_t& columnId,
        const int32_t& ordinalPosition,
        const int& version = 0,
        const bool& isDeleted = false) const;

    Errors::ResultStatus InsertIdentityColumnToMasterDb(
        const int32_t& tableId,
        const int32_t& columnId,
        const int32_t& seedValue,
        const int32_t& increment,
        const int32_t& lastValue,
        const bool& isCached,
        const int32_t& cacheBlock,
        const int& version = 0,
        const bool& isDeleted = false) const;

    Errors::ResultStatus InsertDefaultValuesToMasterDb(
        const int32_t& columnId,
        const Value& value,
        const int& version = 0,
        const bool& isDeleted = false) const;

    Errors::ResultStatus InsertTableStatisticsToMasterDb(
      const int32_t& tableId,
      const int64_t& rowCount = 0,
      const int32_t& rowSize = 0,
      const int& version = 0,
      const bool& isDeleted = false
    ) const;

    Errors::ResultStatus InsertColumnStatisticsToMasterDb(
      const int32_t& columnId,
      const int64_t& distinctCount = 0,
      const int64_t& nullCount = 0,
      const int& version = 0,
      const bool& isDeleted = false
    ) const;

    //MasterDB Select Functions
    [[nodiscard]] vector<Headers::DatabaseHeader> GetCatalog()const;
    [[nodiscard]] bool DatabaseExists(const string& dbName) const;
    [[nodiscard]] Headers::DatabaseHeader SelectDatabase(const std::string& name) const;
    [[nodiscard]] Headers::DatabaseHeader SelectDatabaseById(const int32_t& databaseId) const;
    [[nodiscard]] vector<Headers::SchemaHeader>  SelectSchemas(const int32_t& databaseId) const;
    [[nodiscard]] Dictionary<std::string, Headers::SchemaHeader>  SelectSchemasToDictionary(const int32_t& databaseId) const;
    [[nodiscard]] bool SchemaExists(const int32_t &databaseId, const std::string& schema) const;
    [[nodiscard]] vector<Headers::TableHeader> SelectTables(const string& dbName) const;
    [[nodiscard]] vector<Headers::TableHeader> SelectTables(const int32_t & databaseId) const;
    [[nodiscard]] Headers::TableHeader SelectTable(const string& dbName, const string& tableName) const;
    [[nodiscard]] Headers::TableHeader SelectTable(const int32_t &databaseId, const string &tableName, const std::string& schema) const;
    [[nodiscard]] vector<Headers::ConstraintsHeader> SelectConstraints(const int32_t& tableId) const;
    [[nodiscard]] vector<Headers::ColumnHeader> SelectColumns(const int32_t& tableId) const;
    [[nodiscard]] Dictionary<string, Headers::ColumnHeader> SelectColumnsToDictionary(const int32_t& tableId) const;
    [[nodiscard]] vector<Headers::IndexHeader> SelectIndexes(const int32_t& tableId) const;
    [[nodiscard]] Headers::IndexHeader SelectIndexById(const int32_t& indexId) const;
    [[nodiscard]] vector<Headers::IndexColumnsHeader> SelectIndexColumnsByIndexId(const int32_t& indexId) const;
    [[nodiscard]] Dictionary<int32_t, Headers::IndexColumnsHeader> SelectIndexColumnsByIndexIdToDictionary(const int32_t& indexId) const;
    [[nodiscard]] vector<Headers::IdentityColumnsHeader> SelectIdentityColumnsByTableId(const int32_t& tableId) const;
    [[nodiscard]] Dictionary<int32_t , Headers::IdentityColumnsHeader> SelectIdentityColumnsByTableIdToDictionary(const int32_t& tableId) const;
    [[nodiscard]] vector<Headers::ConstraintsColumnsHeader> SelectConstraintColumnsByConstraintId(const int32_t& constraintId) const;
    [[nodiscard]] Dictionary<int32_t, Headers::ConstraintsColumnsHeader> SelectConstraintColumnsByConstraintIdToDictionary(const int32_t& constraintId) const;
    [[nodiscard]] Headers::DefaultValuesHeader SelectDefaultValueByColumnId(const int32_t& columnId) const;
    [[nodiscard]] Headers::TableStatistics SelectTableStatisticsById(const int32_t& tableId)const;
    [[nodiscard]] Headers::ColumnStatistics SelectColumnStatisticsById(
      const int32_t& columnId,
      const DataType& columnType
    )const;
    void UpdateIdentityByColumnId(const int32_t & tableId, const int32_t& columnId, const int64_t& lastValue)const;
    void UpdateTableStatisticsById(
      const int32_t& tableId,
      const int64_t& rowCount,
      const int32_t& rowSize
    )const;
    void UpdateColumnStatisticsById(
      const int32_t& columnId,
      const int64_t& distinctCount,
      const int64_t& nullCount,
      const Value& min,
      const Value& max
    )const;
    void UpdateColumnById(const int32_t& columnId, const std::vector<Value>& updates)const;
    [[nodiscard]] DatabaseEngine::Database* GetMasterDb()const;

    //Cursor Functions
    // QueryPipeline::Cursor* CreateCursor(QueryPipeline::PhysicalPlan::PhysicalOperator* plan);
    // QueryPipeline::Cursor* GetCursor(const QueryPipeline::PipelineConstants::cursor_id_t& cursorId)const;
    // void DeleteCursor(const QueryPipeline::PipelineConstants::cursor_id_t& cursorId)const;

    void Shutdown();
    [[nodiscard]] DatabaseEngine::Database* UseDatabase(const int32_t & databaseId, const bool& isServerInitialization = false);
    void UseMasterDb();
    
  };
}