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

    ServerInstance() = default;
    ~ServerInstance() = default;

    void ReadConfiguration(const string& configPath);
    [[nodiscard]] DatabaseEngine::Database* CreateSystemDatabase()const;

  public:
    static ServerInstance& Get() {
      static ServerInstance instance;

      return instance;
    }

    [[nodiscard]] DatabaseEngine::Database* Initialize(const string& configPath);
  };
}
