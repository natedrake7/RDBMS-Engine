#include "Server.h"
#include <fstream>
#include <nlohmann/json.hpp>
#include "../Database/Table/Table.h"
#include "../Database/Database.h"
#include "../Database/Storage/StorageManager/StorageManager.h"

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

  void ServerInstance::Initialize(const string &configPath){
    this->ReadConfiguration(configPath);

    this->CreateSystemDatabase();

    const vector<Field> dbFields = {
      Field(this->sysDbName, 0),
      Field(this->sysDbPath, 1)
    };
    
    this->InsertDbToMasterDb(dbFields);

    DatabaseEngine::StorageTypes::Table* sysTables = this->masterDb->OpenTable("sys_tables");
    DatabaseEngine::StorageTypes::Table* sysColumns = this->masterDb->OpenTable("sys_columns");
    
    vector<vector<Field>> tableFields(this->sysTables.size());

    vector<vector<Field>> tableColumns;

    for (int i = 0; i < this->sysTables.size(); i++) {
      int counter = 0;
      
      const auto& table = this->sysTables[i];

      tableFields[i].emplace_back(this->sysDbName, counter++);
      tableFields[i].emplace_back(table.name, counter++);
      tableFields[i].emplace_back("0", counter++);

      for (int j = 0; j < table.columns.size(); j++) {
        const auto& column = table.columns[j];
        tableColumns.emplace_back();

        
        int columnCounter = 0;

        tableColumns.back().emplace_back(this->sysDbName, columnCounter++);
        tableColumns.back().emplace_back(table.name, columnCounter++);
        tableColumns.back().emplace_back(column.name, columnCounter++);

        block_size_t columnSize;
        ColumnTypeSizes.TryGetValue(column.type, columnSize);

        if (columnSize == 0)
          columnSize = column.size;
        
        tableColumns.back().emplace_back(column.type, columnCounter++);
        tableColumns.back().emplace_back(to_string(columnSize), columnCounter++);
        tableColumns.back().emplace_back("0", columnCounter++);
        tableColumns.back().emplace_back(to_string(j), columnCounter++);
      }
    }

    sysTables->InsertRows(tableFields);
    sysColumns->InsertRows(tableColumns);

  }

  DatabaseEngine::Database * ServerInstance::GetMasterDb(){ return this->masterDb; }

  void ServerInstance::Shutdown() const{
    delete this->masterDb;
  }

  void ServerInstance::InsertDbToMasterDb(const vector<Field> &fields) const{
      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable("sys_databases");

      table->InsertRows({fields});
  }

  void ServerInstance::InsertTableToMasterDb(const vector<Field> &fields) const{
    DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable("sys_tables");

    table->InsertRows({fields});
  }

  void ServerInstance::CreateSystemDatabase(){
    using namespace DatabaseEngine;
    using namespace DatabaseEngine::StorageTypes;

   CreateDatabase(this->sysDbName);

   UseDatabase(this->sysDbName, &this->masterDb);

    if (this->masterDb == nullptr)
      throw runtime_error("Failed to create" + this->sysDbName + " database");
    
    for (const auto& table: this->sysTables) {
      vector<Column *> columns;
      vector<column_index_t> primaryKey;
      
      for (int i = 0;i < table.columns.size(); i++) {
        const auto& column = table.columns[i];

        block_size_t columnSize = 0; 
        if (!ColumnTypeSizes.TryGetValue(column.type, columnSize))
          throw runtime_error("Column type " + column.type + " does not exist");

        if (columnSize == 0) 
          columnSize = column.size;

        columns.push_back(new Column(column.name, column.type, columnSize, false));

        for (const auto& key: table.primaryKey) {
          if (column.name != key)
            continue;

          primaryKey.push_back(i);
        }
      }

      if (primaryKey.empty())
        throw runtime_error("All tables in masterDb must have a primary key");
        
      this->masterDb->CreateTable(table.name, columns, &primaryKey);
    }
    
    Storage::StorageManager::Get().BindDatabase(this->masterDb);
  }
}

