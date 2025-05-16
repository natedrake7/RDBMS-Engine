#pragma once
#include "../Database/Database.h"
#include <string>
#include <vector>

using namespace std;



namespace Server {
  typedef struct sysColumn {
    string name;
    string type;
    int size = 0;
  }sysColumn;

  typedef struct sysTable {
    string name;
    vector<sysColumn> columns;
    vector<string> primaryKey;
  }sysTable;

  typedef struct DatabaseHeader {
    string name;
    string filepath;
    bool isSystem;
    DataTypes::DateTime createdAt;
    DataTypes::DateTime lastModified;
    string lastModifiedBy;
  }DatabaseHeader;

  typedef struct TableHeader {
    string dbName;
    string name;
    string schemaName;
    bool isSystem;
    DataTypes::DateTime createdAt;
    DataTypes::DateTime lastModified;
    string lastModifiedBy;
  }TableHeader;

  typedef struct ColumnHeader {
    string dbName;
    string tableName;
    string name;
    string dataType;
    int16_t recordSize;
    bool isNullable;
    int16_t tablePosition;
    DataTypes::DateTime createdAt;
    DataTypes::DateTime lastModified;
    string lastModifiedBy;
  }TableHeColumnHeader;

  class ServerInstance {
    string sysDbName;
    string sysDbPath;
    vector<sysTable> sysTables;

    DatabaseEngine::Database* masterDb;

    ServerInstance();
    ~ServerInstance() = default;

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
    void InsertTableToMasterDb(const string& dbName, const string& tableName, const string& schemaName = "dbo", const bool& isSystem = false, const string& user = "system") const;
    void InsertColumnToMasterDb(
      const string& dbName,
      const string& tableName,
      const string& columnName,
      const string& columnType,
      const int& columnSize,
      const bool& isNullable,
      const int& tablePosition,
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
    [[nodiscard]] DatabaseHeader SelectDatabases(const string& dbName) const;
    [[nodiscard]] vector<DatabaseEngine::StorageTypes::Row> SelectSchemas(const string& dbName) const;
    [[nodiscard]] vector<TableHeader> SelectTables(const string& dbName) const;
    [[nodiscard]] vector<ColumnHeader> SelectColumns(const string& dbName, const string& tableName) const;
    [[nodiscard]] vector<DatabaseEngine::StorageTypes::Row> SelectIndexes(const string& dbName, const string& tableName) const;
    [[nodiscard]] DatabaseEngine::Database* GetMasterDb();

    void Shutdown()const;
  };
}
