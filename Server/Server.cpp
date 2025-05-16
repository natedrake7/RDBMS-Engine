#include "Server.h"
#include <fstream>
#include <nlohmann/json.hpp>
#include "../Database/Row/Row.h"
#include "../Database/Table/Table.h"
#include "../Database/Database.h"
#include "../Database/Block/Block.h"
#include "../Database/Storage/StorageManager/StorageManager.h"

using json = nlohmann::json;

namespace Server {
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

   ServerInstance::ServerInstance(){
     this->masterDb = nullptr;
  }

  void ServerInstance::ReadConfiguration(const string &configPath){
    std::ifstream file(configPath);

    if (!file.is_open())
      throw runtime_error("System Tables file: " + configPath + " could not be opened");

    json jsonFile;

    try {
      file >> jsonFile;
    }
    catch (exception &e)
    {
      throw runtime_error(e.what());
    }

    this->sysDbName = jsonFile.at("db_name");
    this->sysDbPath = jsonFile.at("db_path");

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

    this->InsertDbToMasterDb(this->sysDbName, this->sysDbPath);

     for (const auto& table: this->sysTables) {
      this->InsertTableToMasterDb(this->sysDbName, table.name, "dbo", true, "system");

       int columnPos = 0;
       for (const auto& column: table.columns) {

         block_size_t columnSize;
         ColumnTypeSizes.TryGetValue(column.type, columnSize);

         if (columnSize == 0)
           columnSize = column.size;

         this->InsertColumnToMasterDb(
            this->sysDbName,
            table.name,
            column.name,
            column.type,
            columnSize,
            false,
            columnPos);

          columnPos++;
       }

       string concatenatedColumns;
       string _columns;
       for (const auto & j : table.primaryKey) {
         concatenatedColumns += j;
         _columns +="_" + j;
       }

       this->InsertIndexToMasterDb(this->sysDbName, table.name, "PK" + _columns, concatenatedColumns, true);
     }

     this->InsertSchemaToMasterDb(this->sysDbName, "dbo");
  }

  DatabaseEngine::Database * ServerInstance::GetMasterDb(){ return this->masterDb; }

  void ServerInstance::Shutdown() const{
    delete this->masterDb;
  }

  void ServerInstance::InsertDbToMasterDb(const string& dbName, const string& dbPath, const bool& isSystem, const string& user) const{
      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable("sys_databases");

      const auto currentDate = DataTypes::DateTime::Now().ToString();

      const vector<Field> fields = {
        Field(dbName, 0),
        Field(dbPath, 1),
        Field(isSystem ? "1" : "0", 2),
        Field(currentDate, 3),
        Field(currentDate, 4),
        Field(user, 5),
    };
    
    table->InsertRows({fields});
  }

  void ServerInstance::InsertTableToMasterDb(
    const string& dbName,
    const string& tableName,
    const string& schemaName,
    const bool& isSystem,
    const string& user) const{
      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable("sys_tables");
      const auto currentDate = DataTypes::DateTime::Now().ToString();

      const vector<Field> fields = {
        Field(dbName, 0),
        Field(tableName, 1),
        Field(schemaName, 2),
        Field(isSystem ? "1" : "0", 3),
        Field(currentDate, 4),
        Field(currentDate, 5),
        Field(user, 6),
      };
    
    table->InsertRows({fields});
  }

  void ServerInstance::InsertColumnToMasterDb(
    const string &dbName,
    const string &tableName,
    const string &columnName,
    const string &columnType,
    const int &columnSize,
    const bool& isNullable,
    const int &tablePosition,
    const string& user) const{
      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable("sys_columns");
      const auto currentDate = DataTypes::DateTime::Now().ToString();

    const vector<Field> fields = {
      Field(dbName, 0),
      Field(tableName, 1),
      Field(columnName, 2),
      Field(columnType, 3),
      Field(to_string(columnSize), 4),
      Field(isNullable ? "1" : "0", 5),
      Field(to_string(tablePosition), 6),
      Field(currentDate, 7),
      Field(currentDate, 8),
      Field(user, 9),
    };

    table->InsertRows({fields});
  }

  void ServerInstance::InsertIndexToMasterDb(
    const string &dbName,
    const string &tableName,
    const string &indexName,
    const string &columns,
    const bool &isClustered,
    const string &user) const{
     DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable("sys_indexes");
     const auto currentDate = DataTypes::DateTime::Now().ToString();

     const vector<Field> fields = {
       Field(dbName, 0),
       Field(tableName, 1),
       Field(indexName, 2),
       Field(columns, 3),
       Field(isClustered ? "1" : "0", 4),
       Field(currentDate, 5),
       Field(currentDate, 6),
       Field(user, 7),
     };

     table->InsertRows({fields});
  }
  void ServerInstance::InsertSchemaToMasterDb(const string &dbName, const string &schemaName, const string &user) const{
     DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable("sys_schemas");
     const auto currentDate = DataTypes::DateTime::Now().ToString();

     const vector<Field> fields = {
       Field(dbName, 0),
       Field(schemaName, 1),
       Field(currentDate, 2),
       Field(currentDate, 3),
       Field(user, 4),
     };

     table->InsertRows({fields});
  }

  void ServerInstance::SelectDb(const string &dbName) const{
    using namespace DatabaseEngine::StorageTypes;

    Table* sysDatabases = this->masterDb->OpenTable("sys_databases");

    vector<Row> selectedDatabases;
    const vector<Field> conditions = {
      Field(dbName, 0),
    };

    sysDatabases->Select(selectedDatabases, {0, 1, 2, 3, 4, 5}, &conditions);

    if (selectedDatabases.empty())
      return;

    vector<Row> selectedTables;
    Table* sysTables = this->masterDb->OpenTable("sys_tables");

    sysTables->Select(selectedTables, {0, 1, 2, 3, 4, 5, 6}, &conditions);

    if (selectedTables.empty())
      return;

    Table* sysColumns = this->masterDb->OpenTable("sys_columns");
    vector<vector<Row>> selectedColumns(selectedTables.size());

    for (int i = 0; i < selectedTables.size(); i++)
      sysColumns->Select(selectedColumns[i], {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, &conditions);

     Table* sysIndexes = this->masterDb->OpenTable("sys_indexes");
     vector<vector<Row>> selectedIndexes(selectedTables.size());
     
     for (int i = 0; i < selectedTables.size(); i++)
       sysIndexes->Select(selectedIndexes[i], {0, 1, 2, 3, 4, 5, 6, 7}, &conditions);

     Table* sysSchemas = this->masterDb->OpenTable("sys_schemas");
     vector<Row> selectedSchemas;

     sysSchemas->Select(selectedSchemas, {0, 1, 2, 3, 4}, &conditions);
  }

  bool ServerInstance::DatabaseExists(const string &dbName) const{
     using namespace DatabaseEngine::StorageTypes;

     Table* sysDatabases = this->masterDb->OpenTable("sys_databases");

     vector<Row> selectedDatabases;
     const vector<Field> conditions = {
       Field(dbName, 0),
     };

     sysDatabases->Select(selectedDatabases, {0}, &conditions);

     return !selectedDatabases.empty();
  }

  DatabaseHeader ServerInstance::SelectDatabases(const string &dbName) const{
     using namespace DatabaseEngine::StorageTypes;

     Table* sysDatabases = this->masterDb->OpenTable("sys_databases");

     vector<Row> selectedDatabases;
     const vector<Field> conditions = {
       Field(dbName, 0),
     };

     sysDatabases->Select(selectedDatabases, {0, 1, 2, 3, 4, 5}, &conditions);

     DatabaseHeader header;

     if (selectedDatabases.empty())
       return header;

     const auto& database = selectedDatabases.front();

     const auto& data = database.GetData();

     header.name = dbName;

     header.filepath.resize(data[1]->GetBlockSize());
     memcpy(header.filepath.data(), data[1]->GetBlockData(), data[1]->GetBlockSize());
     memcpy(&header.isSystem, data[2]->GetBlockData(), sizeof(bool));

     time_t createdAt;
     memcpy(&createdAt, data[3]->GetBlockData(), data[3]->GetBlockSize());
     header.createdAt = DataTypes::DateTime(createdAt);

     time_t lastModified;
     memcpy(&lastModified, data[4]->GetBlockData(), data[4]->GetBlockSize());
     header.lastModified = DataTypes::DateTime(lastModified);


     header.lastModifiedBy.resize(data[5]->GetBlockSize());
     memcpy(header.lastModifiedBy.data(), data[5]->GetBlockData(), data[5]->GetBlockSize());

     return header;
  }

  vector<DatabaseEngine::StorageTypes::Row> ServerInstance::SelectSchemas(const string &dbName) const{
     using namespace DatabaseEngine::StorageTypes;

     const vector<Field> conditions = {
       Field(dbName, 0),
     };

     Table* sysSchemas = this->masterDb->OpenTable("sys_schemas");
     vector<Row> selectedSchemas;

     sysSchemas->Select(selectedSchemas, {0, 1, 2, 3, 4}, &conditions);

     return selectedSchemas;
  }

  vector<DatabaseEngine::StorageTypes::Row> ServerInstance::SelectTables(const string &dbName) const{
     using namespace DatabaseEngine::StorageTypes;

     const vector<Field> conditions = {
       Field(dbName, 0)
     };

     vector<Row> selectedTables;
     Table* sysTables = this->masterDb->OpenTable("sys_tables");

     sysTables->Select(selectedTables, {0, 1, 2, 3, 4, 5, 6}, &conditions);

     return selectedTables;
  }

  vector<DatabaseEngine::StorageTypes::Row> ServerInstance::SelectColumns(const string &dbName, const string &tableName) const{
     using namespace DatabaseEngine::StorageTypes;

     const vector<Field> conditions = {
       Field(dbName, 0),
       Field(tableName, 1),
     };

     vector<Row> selectedColumns;
     Table* sysColumns = this->masterDb->OpenTable("sys_columns");

     sysColumns->Select(selectedColumns, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, &conditions);

     return selectedColumns;
  }

  vector<DatabaseEngine::StorageTypes::Row> ServerInstance::SelectIndexes(const string &dbName, const string &tableName) const{
     using namespace DatabaseEngine::StorageTypes;

     const vector<Field> conditions = {
       Field(dbName, 0),
       Field(tableName, 1),
     };

     Table* sysIndexes = this->masterDb->OpenTable("sys_indexes");
     vector<Row> selectedIndexes;

      sysIndexes->Select(selectedIndexes, {0, 1, 2, 3, 4, 5, 6, 7}, &conditions);

     return selectedIndexes;
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

