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

    ServerInstance() = default;
    ~ServerInstance() = default;

    void ReadConfiguration(const string& configPath);
    void CreateSystemDatabase();

  public:
    static ServerInstance& Get() {
      static ServerInstance instance;

      return instance;
    }

    void Initialize(const string& configPath);
    void InsertDbToMasterDb(const vector<Field>& fields) const;
    void InsertTableToMasterDb(const vector<Field>& fields) const;
    [[nodiscard]] DatabaseEngine::Database* GetMasterDb();

    void Shutdown()const;
  };
}
