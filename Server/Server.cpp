#include "Server.h"
#include <fstream>
#include <nlohmann/json.hpp>
#include "../Database/Table/Table.h"

using json = nlohmann::json;

void from_json(const json& j, sysColumn& c) {
  j.at("name").get_to(c.name);
  j.at("type").get_to(c.type);

  if (j.contains("size"))
    j.at("size").get_to(c.size);
}

void from_json(const json& j, sysTable& t) {
  j.at("name").get_to(t.name);
  j.at("columns").get_to(t.columns);
  j.at("primaryKey").get_to(t.primaryKey);
}

namespace Server {
  void ServerInstance::ReadConfiguration(const string &configPath){
    std::ifstream file(configPath);

    if (!file.is_open())
      throw runtime_error("System Tables file: " + configPath + "could not be opened");

    json jsonFile;

    try {
      file >> jsonFile;
    }
    catch (exception &e)
    {
      throw runtime_error(e.what());
    }

    this->sysDbName = jsonFile.at("dbName");
    this->sysDbPath = jsonFile.at("dbPath");

    jsonFile.at("tables").get_to(this->sysTables);  

  }

  DatabaseEngine::Database* ServerInstance::Initialize(const string &configPath){
    this->ReadConfiguration(configPath);

    return this->CreateSystemDatabase();
  }

  [[nodiscard]] DatabaseEngine::Database* ServerInstance::CreateSystemDatabase()const{
      using namespace DatabaseEngine;
      using namespace DatabaseEngine::StorageTypes;

     Database* db = nullptr;
      
     CreateDatabase(this->sysDbName);

     UseDatabase(this->sysDbName, &db);

      if (db == nullptr)
        throw runtime_error("Failed to create" + this->sysDbName + " database");
      
      for (const auto& table: this->sysTables) {
        vector<Column *> columns;
        vector<column_index_t> primaryKey;
        
        for (int i = 0;i < table.columns.size(); i++) {
          const auto& column = table.columns[i];

          block_size_t columnSize = 0; 
          if (!ColumnTypeSizes.TryGetValue(column.type, columnSize))
            throw runtime_error("Column type " + column.type + " does not exist");

          columns.push_back(new Column(column.name, column.type, columnSize, false));

          for (const auto& key: table.primaryKey) {
            if (column.name != key)
              continue;

            primaryKey.push_back(i);
          }
        }

        if (primaryKey.empty())
          throw runtime_error("All tables in masterDb must have a primary key");
          
        db->CreateTable(table.name, columns, &primaryKey);
      }

     return db;
  }
}

