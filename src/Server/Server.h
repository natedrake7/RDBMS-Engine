#pragma once
#include "../Systemic/DataTypes/Headers/Headers.h"
#include "../Systemic/Errors/Errors.h"
#include "../Database/Database.h"
#include "../Systemic/Security/Security.h"
#include "RoleManager/RoleManager.h"
#include "SessionManager/SessionManager.h"
#include "UserManager/UserManager.h"

#include <string>
#include <vector>

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
    SYSCOLUMNSTATS = 11,
    SYSROLES = 12,
    SYSUSERS = 13,
  };

  class ServerInstance {
    std::string sysDbName;
    std::string sysDbPath;
    vector<Headers::sysTable> sysTables;
    DatabaseEngine::Database* masterDb;

    Dictionary<int32_t, DatabaseEngine::Database*> databases;

    Sessions::SessionManager sessionManager;
    Security::RoleManager roleManager;
    Security::UserManager userManager;

    ServerInstance();
    ~ServerInstance();

    void ReadConfiguration(const std::string& configPath);
    void CreateSystemDatabase();
    [[nodiscard]] bool CheckIfMasterDbExists()const;
    [[nodiscard]] std::vector<Security::Role> SelectRoles()const;
    [[nodiscard]] std::vector<Security::User> SelectUsers()const;

    void InsertSystemRoles();
    void InsertSystemUsers();

  public:
    [[nodiscard]] static ServerInstance& Get();
    void Initialize(const std::string& configPath);

    //Security Functions
    bool GrantRole(const std::string& username, const Security::Role* role)const;
    bool UserExists(const std::string& userName)const;
    bool CreateUser(const std::string& userName, const std::string& password, const std::string& roleName);
    [[nodiscard]] const Security::User* Authenticate(const std::string& username, const std::string& password);

    bool RoleExists(const std::string& role)const;
    const Security::Role* GetRole(const std::string& roleName)const;

    //Session Functions
    [[nodiscard]] const Network::Session* CreateSession(const Security::User* user);
    [[nodiscard]] const Network::Session* GetSession(const DataTypes::Guid& key);
    [[nodiscard]] bool CloseSession(const DataTypes::Guid& key);
    [[nodiscard]] bool UpdateSession(const DataTypes::Guid& key, const int32_t& databaseId);

    //MasterDB Insert Functions
    [[nodiscard]] Errors::ResultStatus InsertDbToMasterDb(
      const std::string& dbName,
      const std::string& dbPath,
      const bool& isSystem = false,
      const std::string& user = "system",
      const int& version = 0,
      const bool& isDeleted = false) const;

    [[nodiscard]] Errors::ResultStatus InsertSchemaToMasterDb(
      const int32_t& databaseId,
      const std::string& schemaName,
      const std::string& user = "system",
      const int& version = 0,
      const bool& isDeleted = false) const;

    [[nodiscard]] Errors::ResultStatus InsertTableToMasterDb(
      const int32_t & databaseId,
      const int32_t & schemaId,
      const std::string& tableName,
      const int16_t& ordinalPosition,
      const bool& isSystem = false,
      const std::string& user = "system",
      const int& version = 0,
      const bool& isDeleted = false) const;

    [[nodiscard]] Errors::ResultStatus InsertColumnToMasterDb(
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
      const bool& isDeleted = false) const;

    [[nodiscard]] Errors::ResultStatus InsertIndexToMasterDb(
      const int32_t & tableId,
      const std::string &indexName,
      const bool &isClustered,
      const bool &isDisabled = false,
      const std::string& user = "system",
      const int& version = 0,
      const bool& isDeleted = false) const;

    [[nodiscard]] Errors::ResultStatus InsertIndexColumnToMasterDb(
        const int32_t& indexId,
        const int32_t& columnId,
        const int16_t& ordinalPosition,
        const bool& isIncluded,
        const int& version = 0,
        const bool& isDeleted = false) const;

    [[nodiscard]] Errors::ResultStatus InsertConstraintToMasterDb(
        const int32_t& tableId,
        const string& constraintName,
        const Headers::ConstraintType& constraintType,
        const bool& isDisabled,
        const int32_t* constraintIndexId,
        const std::string& user = "system",
        const int& version = 0,
        const bool& isDeleted = false) const;

    [[nodiscard]] Errors::ResultStatus InsertConstraintColumnToMasterDb(
        const int32_t& constraintId,
        const int32_t& columnId,
        const int32_t& ordinalPosition,
        const int& version = 0,
        const bool& isDeleted = false) const;

    [[nodiscard]] Errors::ResultStatus InsertIdentityColumnToMasterDb(
        const int32_t& tableId,
        const int32_t& columnId,
        const int32_t& seedValue,
        const int32_t& increment,
        const int32_t& lastValue,
        const bool& isCached,
        const int32_t& cacheBlock,
        const int& version = 0,
        const bool& isDeleted = false) const;

    [[nodiscard]] Errors::ResultStatus InsertDefaultValuesToMasterDb(
        const int32_t& columnId,
        const Value& value,
        const int& version = 0,
        const bool& isDeleted = false) const;

    [[nodiscard]] Errors::ResultStatus InsertTableStatisticsToMasterDb(
      const int32_t& tableId,
      const int64_t& rowCount = 0,
      const int32_t& rowSize = 0,
      const int& version = 0,
      const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::ResultStatus InsertColumnStatisticsToMasterDb(
      const int32_t& columnId,
      const int64_t& distinctCount = 0,
      const int64_t& nullCount = 0,
      const int& version = 0,
      const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::ResultStatus InsertRoleToMasterDb(
      const std::string& roleName,
      const Security::Permission& permissions,
      const bool& isSystem = true,
      const int& version = 0,
      const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::ResultStatus InsertUserToMasterDb(
      const std::string& username,
      const std::string& passwordHash,
      const int32_t& roleId,
      const bool& isActive = false,
      const int& version = 0,
      const bool& isDeleted = false
    ) const;

    //MasterDB Select Functions
    [[nodiscard]] std::vector<Headers::DatabaseHeader> GetCatalog()const;
    [[nodiscard]] bool DatabaseExists(const string& dbName) const;
    [[nodiscard]] Headers::DatabaseHeader SelectDatabase(const std::string& name) const;
    [[nodiscard]] Headers::DatabaseHeader SelectDatabaseById(const int32_t& databaseId) const;
    [[nodiscard]] std::vector<Headers::SchemaHeader>  SelectSchemas(const int32_t& databaseId) const;
    [[nodiscard]] Dictionary<std::string, Headers::SchemaHeader>  SelectSchemasToDictionary(const int32_t& databaseId) const;
    [[nodiscard]] bool SchemaExists(const int32_t &databaseId, const std::string& schema) const;
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
      const Constants::DataType& columnType
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