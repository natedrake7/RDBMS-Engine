#pragma once
#include "../AdditionalLibraries/AdditionalDataTypes/Headers/Headers.h"
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
    SYSINDEXES = 4
  };

  class ServerInstance {
    string sysDbName;
    string sysDbPath;
    vector<Headers::sysTable> sysTables;
    DatabaseEngine::Database* masterDb;

    Dictionary<string, DatabaseEngine::Database*> databases;

    ServerInstance();
    ~ServerInstance();

    void ReadConfiguration(const string& configPath);
    void CreateSystemDatabase();
    [[nodiscard]] bool CheckIfMasterDbExists()const;
    

  public:
    static ServerInstance& Get() {
      static ServerInstance instance;

      return instance;
    }

    void Initialize(const string& configPath);
    AdditionalDataTypes::ResultStatus  InsertDbToMasterDb(
      const string& dbName,
      const string& dbPath,
      const bool& isSystem = false,
      const string& user = "system") const;
    AdditionalDataTypes::ResultStatus  InsertTableToMasterDb(
      const int32_t & databaseId,
      const int32_t & schemaId,
      const string& tableName,
      const table_id_t& tablePosition,
      const bool& isSystem = false,
      const string& user = "system") const;
    AdditionalDataTypes::ResultStatus  InsertColumnToMasterDb(
      const int32_t & tableId,
      const string& columnName,
      const string& columnType,
      const int& columnSize,
      const bool& isNullable,
      const int& tablePosition,
      const bool& isSystem = false,
      const string& user = "system") const;
    AdditionalDataTypes::ResultStatus  InsertIndexToMasterDb(
      const int32_t & tableId,
      const string &indexName,
      const string &columns,
      const bool &isClustered,
      const int32_t& seed,
      const int32_t& incrementFactor,
      const int32_t& lastValue,
      const string& user = "system") const;
    AdditionalDataTypes::ResultStatus  InsertSchemaToMasterDb(
      const int32_t& databaseId,
      const string& schemaName,
      const string& user = "system") const;

    [[nodiscard]] vector<Headers::DatabaseHeader> GetCatalog()const;
    [[nodiscard]] bool DatabaseExists(const string& dbName) const;
    [[nodiscard]] Headers::DatabaseHeader SelectDatabase(const std::string& name) const;
    [[nodiscard]] vector<Headers::SchemaHeader>  SelectSchemas(const int32_t& databaseId) const;
    [[nodiscard]] bool SchemaExists(const string &dbName, const std::string& schema) const;
    [[nodiscard]] vector<Headers::TableHeader> SelectTables(const string& dbName) const;
    [[nodiscard]] Headers::TableHeader SelectTable(const string& dbName, const string& tableName) const;
    [[nodiscard]] Headers::TableHeader SelectTable(const string &dbName, const string &tableName, const std::string& schema) const;
    [[nodiscard]] vector<Headers::ColumnHeader> SelectColumns(const int32_t& tableId) const;
    [[nodiscard]] Dictionary<string, Headers::ColumnHeader> SelectColumnsToDictionary(const int32_t& tableId) const;
    [[nodiscard]] vector<Headers::IndexHeader> SelectIndexes(const int32_t& tableId) const;
    [[nodiscard]] DatabaseEngine::Database* GetMasterDb()const;

    void Shutdown()const;
    [[nodiscard]] DatabaseEngine::Database* UseDatabase(const string& dbName, const bool& isServerInitialization = false);
    void UseMasterDb();
    
  };
}