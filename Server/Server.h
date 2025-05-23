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
    void InsertDbToMasterDb(const string& dbName, const string& dbPath, const bool& isSystem = false, const string& user = "system") const;
    void InsertTableToMasterDb(
      const string& dbName,
      const string& tableName,
      const table_id_t& tableId,
      const string& schemaName = "dbo",
      const bool& isSystem = false,
      const string& user = "system") const;
    void InsertColumnToMasterDb(
      const string& dbName,
      const string& tableName,
      const string& columnName,
      const string& columnType,
      const int& columnSize,
      const bool& isNullable,
      const int& tablePosition,
      const bool& isSystem = false,
      const string& user = "system") const;
    void InsertIndexToMasterDb(
      const string& dbName,
      const string& tableName,
      const string& indexName,
      const string& columns,
      const bool& isClustered,
      const string& user = "system") const;
    void InsertSchemaToMasterDb(const string& dbName, const string& schemaName, const string& user = "system") const;

    void SelectDb(const string& dbName) const;
    [[nodiscard]] bool DatabaseExists(const string& dbName) const;
    [[nodiscard]] Headers::DatabaseHeader SelectDatabases(const string& dbName) const;
    [[nodiscard]] vector<DatabaseEngine::StorageTypes::Row> SelectSchemas(const string& dbName) const;
    [[nodiscard]] bool SchemaExists(const string &dbName, const std::string& schema) const;
    [[nodiscard]] vector<Headers::TableHeader> SelectTables(const string& dbName) const;
    [[nodiscard]] Headers::TableHeader SelectTable(const string& dbName, const string& tableName) const;
    [[nodiscard]] bool TableExists(const string &dbName, const string &tableName, const std::string& schema) const;
    [[nodiscard]] vector<Headers::ColumnHeader> SelectColumns(const string& dbName, const string& tableName) const;
    [[nodiscard]] Dictionary<string, Headers::ColumnHeader> SelectColumnsToDictionary(const string& dbName, const string& tableName) const;
    [[nodiscard]] vector<Headers::IndexHeader> SelectIndexes(const string& dbName, const string& tableName) const;
    [[nodiscard]] DatabaseEngine::Database* GetMasterDb()const;

    void Shutdown()const;
    [[nodiscard]] DatabaseEngine::Database* UseDatabase(const string& dbName, const bool& isServerInitialization = false);
    void UseMasterDb();
    
  };
}