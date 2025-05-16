#pragma once
#include "../Database/Database.h"


#include <string>
#include <vector>

using namespace std;

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

namespace Server {
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
    void InsertDbToMasterDb(const string& dbName, const string& dbPath, const string& user = "system") const;
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
    [[nodiscard]] DatabaseEngine::Database* GetMasterDb();

    void Shutdown()const;
  };
}
