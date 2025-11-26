#pragma once
#include "../Systemic/DataTypes/Headers/Headers.h"
#include "../Systemic/Errors/Errors.h"
#include "../Database/Database.h"
#include "../Database/VersionDatabase/VersionDatabase.h"
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
    SysDatabases = 0,
    SysSchemas = 1,
    SysTables = 2,
    SysColumns = 3,
    SysIndexes = 4,
    SysIdentityColumns = 5,
    SysIndexColumns = 6,
    SysConstraints = 7,
    SysConstraintColumns = 8,
    SysDefaultValues = 9,
    SysTableStats = 10,
    SysColumnStats = 11,
    SysRoles = 12,
    SysUsers = 13,
  };

  class ServerInstance {
    std::string sysDbName;
    std::string sysDbPath;

    std::string versionDbName;
    std::string versionDbPath;

    std::vector<Headers::sysTable> sysTables;
    DatabaseEngine::Database* masterDb;
    DatabaseEngine::VersionDatabase *versionDb;

    Dictionary<int32_t, DatabaseEngine::Database*> databases;

    Sessions::SessionManager sessionManager;

    Security::RoleManager roleManager;
    Security::UserManager userManager;

    QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties properties;

    ServerInstance();
    ~ServerInstance();

    void ReadConfiguration(const std::string& configPath);
    void CreateSystemDatabase();
    void CreateVersionDatabase();
    [[nodiscard]] bool CheckIfMasterDbExists()const;
    [[nodiscard]] bool CheckIfVersionDbExists()const;
    [[nodiscard]] std::vector<Security::Role> SelectRoles()const;
    [[nodiscard]] std::vector<Security::User> SelectUsers()const;

    void InsertSystemRoles(const transaction_id_t& transactionId);
    void InsertSystemUsers(const transaction_id_t& transactionId);

  public:
    [[nodiscard]] static ServerInstance& Get();
    void Initialize(const std::string& configPath);

    //Security Functions
    [[nodiscard]]Errors::RuntimeStatus GrantRole(const DataTypes::Guid& currentSessionId, const std::string& username, const Security::Role* role)const;
    bool UserExists(const std::string& userName)const;
    bool CreateUser(const transaction_id_t& transactionId, const std::string& userName, const std::string& password, const std::string& roleName);
    [[nodiscard]] const Security::User* Authenticate(const std::string& username, const std::string& password)const;

    bool RoleExists(const std::string& role)const;
    const Security::Role* GetRole(const std::string& roleName)const;

    //Session Functions
    [[nodiscard]] const Network::Session* CreateSession(const Security::User* user);
    [[nodiscard]] const Network::Session* GetSession(const DataTypes::Guid& key)const;
    [[nodiscard]] bool CloseSession(const DataTypes::Guid& key);
    [[nodiscard]] bool UpdateSession(const DataTypes::Guid& key, const int32_t& databaseId)const;
    [[nodiscard]] QueryPipeline::Cursor* CreateCursor(
      const DataTypes::Guid &id,
      const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
      QueryPipeline::PhysicalPlan::PhysicalOperator *physicalPlan
    )const;
    [[nodiscard]] bool CloseCursor(const DataTypes::Guid &id)const;

    //MasterDB Insert Functions
    [[nodiscard]] Errors::RuntimeStatus InsertDbToMasterDb(
      const Constants::transaction_id_t& transactionId,
      const std::string& dbName,
      const std::string& dbPath,
      const bool& isSystem = false,
      const std::string& user = "system",
      const int& version = 0,
      const bool& isDeleted = false
  ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertSchemaToMasterDb(
      const Constants::transaction_id_t& transactionId,
      const int32_t& databaseId,
      const std::string& schemaName,
      const std::string& user = "system",
      const int& version = 0,
      const bool& isDeleted = false
  ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertTableToMasterDb(
      const Constants::transaction_id_t& transactionId,
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
      const Constants::transaction_id_t& transactionId,
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
      const Constants::transaction_id_t& transactionId,
      const int32_t & tableId,
      const std::string &indexName,
      const bool &isClustered,
      const bool &isDisabled = false,
      const std::string& user = "system",
      const int& version = 0,
      const bool& isDeleted = false
  ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertIndexColumnToMasterDb(
      const Constants::transaction_id_t& transactionId,
        const int32_t& indexId,
        const int32_t& columnId,
        const int16_t& ordinalPosition,
        const bool& isIncluded,
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertConstraintToMasterDb(
      const Constants::transaction_id_t& transactionId,
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
      const Constants::transaction_id_t& transactionId,
        const int32_t& constraintId,
        const int32_t& columnId,
        const int32_t& ordinalPosition,
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertIdentityColumnToMasterDb(
      const Constants::transaction_id_t& transactionId,
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
      const Constants::transaction_id_t& transactionId,
        const int32_t& columnId,
        const Value& value,
        const int& version = 0,
        const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertTableStatisticsToMasterDb(
      const Constants::transaction_id_t& transactionId,
      const int32_t& tableId,
      const int64_t& rowCount = 0,
      const int32_t& rowSize = 0,
      const int& version = 0,
      const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertColumnStatisticsToMasterDb(
      const Constants::transaction_id_t& transactionId,
      const int32_t& columnId,
      const int64_t& distinctCount = 0,
      const int64_t& nullCount = 0,
      const int& version = 0,
      const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertRoleToMasterDb(
      const transaction_id_t& transactionId,
      const std::string& roleName,
      const Security::Permission& permissions,
      const bool& isSystem = true,
      const int& version = 0,
      const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus InsertUserToMasterDb(
      const transaction_id_t& transactionId,
      const std::string& username,
      const std::string& passwordHash,
      const int32_t& roleId,
      const bool& isActive = false,
      const int& version = 0,
      const bool& isDeleted = false
    ) const;

    [[nodiscard]] Errors::RuntimeStatus UpdateUserById(
      const DataTypes::Guid& callerSessionId,
      const int32_t& userId,
      const int32_t& roleId
    )const;

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
    [[nodiscard]]Errors::RuntimeStatus UpdateColumnById(const int32_t& columnId, const std::vector<Value>& updates)const;
    [[nodiscard]] DatabaseEngine::Database* GetMasterDb()const;

    //Cursor Functions
    // QueryPipeline::Cursor* CreateCursor(QueryPipeline::PhysicalPlan::PhysicalOperator* plan);
    // QueryPipeline::Cursor* GetCursor(const QueryPipeline::PipelineConstants::cursor_id_t& cursorId)const;
    // void DeleteCursor(const QueryPipeline::PipelineConstants::cursor_id_t& cursorId)const;

    void Shutdown();
    [[nodiscard]] DatabaseEngine::Database* UseDatabase(const int32_t & databaseId, const bool& isServerInitialization = false);
    void UseMasterDb();

    DatabaseEngine::VersionDatabase* GetVersionDatabase()const;
    
  };
}