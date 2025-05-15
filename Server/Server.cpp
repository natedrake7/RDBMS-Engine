#include "Server.h"
#include <fstream>
#include <nlohmann/json.hpp>
#include "../Database/Row/Row.h"
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
   ServerInstance::ServerInstance(){
     this->masterDb = nullptr;
  }

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

     if (this->CheckIfMasterDbExists()) {
       DatabaseEngine::UseDatabase(this->sysDbName, &this->masterDb);
       Storage::StorageManager::Get().BindDatabase(this->masterDb);
       return;
     }
     
    this->CreateSystemDatabase();
    Storage::StorageManager::Get().BindDatabase(this->masterDb);

    DatabaseEngine::StorageTypes::Table* sysDatabases = this->masterDb->OpenTable("sys_databases");
     
     const vector<Field> dbFields = {
       Field(this->sysDbName, 0),
       Field(this->sysDbPath, 1),
     };

     sysDatabases->InsertRows({dbFields});
     
    DatabaseEngine::StorageTypes::Table* sysTables = this->masterDb->OpenTable("sys_tables");
    DatabaseEngine::StorageTypes::Table* sysColumns = this->masterDb->OpenTable("sys_columns");
    DatabaseEngine::StorageTypes::Table* sysIndexes = this->masterDb->OpenTable("sys_indexes");
     
    vector<vector<Field>> tableFields(this->sysTables.size());
    vector<vector<Field>> tableColumns;
    vector<vector<Field>> tableIndexes;     

    for (int i = 0; i < this->sysTables.size(); i++) {
      int counter = 0;
      
      const auto& table = this->sysTables[i];

      tableFields[i].emplace_back(this->sysDbName, counter++);
      tableFields[i].emplace_back(table.name, counter++);
      tableFields[i].emplace_back("1", counter++);

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

      string concatenatedColumns;
      string _columns;
      for (const auto & j : table.primaryKey) {
        concatenatedColumns += j;
        _columns +="_" + j;  
      }

      int indexCounter = 0;
      tableIndexes.emplace_back();

      tableIndexes.back().emplace_back(this->sysDbName, indexCounter++);
      tableIndexes.back().emplace_back(table.name, indexCounter++);
      tableIndexes.back().emplace_back(to_string(i), indexCounter++);
      tableIndexes.back().emplace_back("PK" + _columns , indexCounter++);
      tableIndexes.back().emplace_back(concatenatedColumns , indexCounter++);
      tableIndexes.back().emplace_back("1" , indexCounter++);
    }

    sysTables->InsertRows(tableFields);
    sysColumns->InsertRows(tableColumns);
    sysIndexes->InsertRows(tableIndexes);
  }

  DatabaseEngine::Database * ServerInstance::GetMasterDb(){ return this->masterDb; }

  void ServerInstance::Shutdown() const{
    delete this->masterDb;
  }

  void ServerInstance::InsertDbToMasterDb(const string& dbName, const string& dbPath) const{
      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable("sys_databases");

    const vector<Field> fields = {
      Field(dbName, 0),
      Field(dbPath, 1),
    };
    
    table->InsertRows({fields});
  }

  void ServerInstance::InsertTableToMasterDb(const string& dbName, const string& tableName) const{

    DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable("sys_tables");

    const vector<Field> fields = {
      Field(dbName, 0),
      Field(tableName, 1),
      Field("0", 2),
    };
    
    table->InsertRows({fields});
  }

  void ServerInstance::InsertColumnToMasterDb(const string &dbName, const string &tableName, const string &columnName, const string &columnType, const int &columnSize, const int &tablePosition) const{
    DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable("sys_columns");

    const vector<Field> fields = {
      Field(dbName, 0),
      Field(tableName, 1),
      Field(columnName, 2),
      Field(columnType, 3),
      Field(to_string(columnSize), 4),
      Field("0", 5),
      Field(to_string(tablePosition), 6),
    };

    table->InsertRows({fields});
  }

  void ServerInstance::SelectDb(const string &dbName) const{
    DatabaseEngine::StorageTypes::Table* sysDatabases = this->masterDb->OpenTable("sys_databases");

    vector<DatabaseEngine::StorageTypes::Row> selectedDatabases;
    const vector<Field> conditions = {
      Field(dbName, 0),
    };
    
    sysDatabases->Select(selectedDatabases, {0, 1}, &conditions);

    if (selectedDatabases.empty())
      return;

    vector<DatabaseEngine::StorageTypes::Row> selectedTables;
    DatabaseEngine::StorageTypes::Table* sysTables = this->masterDb->OpenTable("sys_tables");

    sysTables->Select(selectedTables, {0, 1, 2}, &conditions);

    if (selectedTables.empty())
      return;

    DatabaseEngine::StorageTypes::Table* sysColumns = this->masterDb->OpenTable("sys_columns");
    vector<vector<DatabaseEngine::StorageTypes::Row>> selectedColumns(selectedTables.size());

    for (int i = 0; i < selectedTables.size(); i++)
      sysColumns->Select(selectedColumns[i], {0, 1, 2, 3, 4, 5, 6}, &conditions);

     DatabaseEngine::StorageTypes::Table* sysIndexes = this->masterDb->OpenTable("sys_indexes");
     vector<vector<DatabaseEngine::StorageTypes::Row>> selectedIndexes(selectedTables.size());
     
     for (int i = 0; i < selectedTables.size(); i++)
       sysIndexes->Select(selectedIndexes[i], {0, 1, 2, 3, 4, 5}, &conditions);
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
  }

  bool ServerInstance::CheckIfMasterDbExists() const{ return std::filesystem::exists(this->sysDbPath); }
}

