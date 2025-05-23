#include "Server.h"

#include "../AdditionalLibraries/SafeConverter/SafeConverter.h"

#include <fstream>
#include <nlohmann/json.hpp>
#include "../Database/Row/Row.h"
#include "../Database/Table/Table.h"
#include "../Database/Database.h"
#include "../Database/Block/Block.h"
#include "../Database/Storage/StorageManager/StorageManager.h"
#include "../AdditionalLibraries/StringFunctions/StringFunctions.h"
#include "../Database/AdditionalFunctions/SortingFunctions.h"

#include <iostream>

using json = nlohmann::json;

namespace Headers {
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
}

namespace Server {
   ServerInstance::ServerInstance(){
     this->masterDb = nullptr;
  }

  ServerInstance::~ServerInstance() = default;

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
       this->UseMasterDb();
       std::cout << this->sysDbName << " initialized successfully" << std::endl;
       return;
     }
     
    this->CreateSystemDatabase();
    this->InsertDbToMasterDb(this->sysDbName, this->sysDbPath, true);

    Dictionary<string, column_index_t> columnNameToIndex;

    for (int i = 0;i < this->sysTables.size(); i++) {
      const auto& table = this->sysTables[i];
      
      this->InsertTableToMasterDb(
        this->sysDbName,
        table.name,
        i,
        "dbo",
        true,
        "system");

      int columnPos = 0;
      for (auto& column: table.columns) {

        block_size_t columnSize;

        ColumnTypeSizes.TryGetValue(AdditionalLibraries::NormalizeString(column.type), columnSize);

        if (columnSize == 0)
          columnSize = column.size;

        this->InsertColumnToMasterDb(
           this->sysDbName,
           table.name,
           column.name,
           column.type,
           columnSize,
           false,
           columnPos,
           true);

        columnNameToIndex.Add(column.name, columnPos);

        columnPos++;
      }

      string concatenatedColumns;
      string _columns;

      for (int j = 0; j < table.primaryKey.size(); j++) {
        const auto& key = columnNameToIndex.Get(table.primaryKey[j]);

        concatenatedColumns +=  j > 0  ? "," + to_string(key) : to_string(key);
        _columns +="_" + table.primaryKey[j];
      }
       
      this->InsertIndexToMasterDb(this->sysDbName, table.name, "PK" + _columns, concatenatedColumns, true); 
    }

    this->InsertSchemaToMasterDb(this->sysDbName, "dbo");
    std::cout << this->sysDbName << " initialized successfully" << std::endl;
  }

  DatabaseEngine::Database * ServerInstance::GetMasterDb()const{ return this->masterDb; }

  void ServerInstance::Shutdown() const{
    delete this->masterDb;

     for (const auto& [name, database]: this->databases)
       delete database;
  }

  DatabaseEngine::Database* ServerInstance::UseDatabase(const string &dbName, const bool& isServerInitialization){
     if (dbName == this->sysDbName && this->masterDb != nullptr)
       return this->masterDb;

     DatabaseEngine::Database *db = nullptr;
     if (this->databases.TryGetValue(dbName, db))
       return db;

     db = new DatabaseEngine::Database(dbName, isServerInitialization);

     if (dbName != this->sysDbName)
        this->databases.Add(dbName, db);
     
     return db;
  }

  void ServerInstance::UseMasterDb(){
       this->masterDb = new DatabaseEngine::Database(this->sysDbName, this->sysTables);
  }

  void ServerInstance::InsertDbToMasterDb(const string& dbName, const string& dbPath, const bool& isSystem, const string& user) const{
      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable("dbo", "sys_databases");

      const auto currentDate = DataTypes::DateTime::Now();

      const vector<Field> fields = {
        Field(dbName, 0),
        Field(dbPath, 1),
        Field(isSystem, 2),
        Field(currentDate, 3),
        Field(currentDate, 4),
        Field(user, 5),
    };
    
    table->InsertRows({fields});
  }

  void ServerInstance::InsertTableToMasterDb(
    const string& dbName,
    const string& tableName,
    const table_id_t& tableId,
    const string& schemaName,
    const bool& isSystem,
    const string& user) const{
      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable("dbo", "sys_tables");
      const auto currentDate = DataTypes::DateTime::Now();

      const vector<Field> fields = {
        Field(dbName, 0),
        Field(tableName, 1),
        Field(tableId, 2),
        Field(schemaName, 3),
        Field(isSystem, 4),
        Field(currentDate, 5),
        Field(currentDate, 6),
        Field(user, 7),
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
    const bool& isSystem,
    const string& user) const{
      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable("dbo", "sys_columns");
      const auto currentDate = DataTypes::DateTime::Now();

      const vector<Field> fields = {
        Field(dbName, 0),
        Field(tableName, 1),
        Field(columnName, 2),
        Field(columnType, 3),
        Field(columnSize, 4),
        Field(isNullable, 5),
        Field(tablePosition, 6),
        Field(isSystem, 7),
        Field(currentDate, 8),
        Field(currentDate, 9),
        Field(user, 10),
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
     DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable("dbo", "sys_indexes");
     const auto currentDate = DataTypes::DateTime::Now();

     const vector<Field> fields = {
       Field(dbName, 0),
       Field(tableName, 1),
       Field(indexName, 2),
       Field(columns, 3),
       Field(isClustered, 4),
       Field(currentDate, 5),
       Field(currentDate, 6),
       Field(user, 7),
     };

     table->InsertRows({fields});
  }
  void ServerInstance::InsertSchemaToMasterDb(const string &dbName, const string &schemaName, const string &user) const{
     DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable("dbo", "sys_schemas");
     const auto currentDate = DataTypes::DateTime::Now();

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

    Table* sysDatabases = this->masterDb->OpenTable("dbo", "sys_databases");

    vector<Row> selectedDatabases;
    const vector<Field> conditions = {
      Field(dbName, 0),
    };

    sysDatabases->Select(selectedDatabases, {0, 1, 2, 3, 4, 5}, &conditions);

    if (selectedDatabases.empty())
      return;

    vector<Row> selectedTables;
    Table* sysTables = this->masterDb->OpenTable("dbo", "sys_tables");

    sysTables->Select(selectedTables, {0, 1, 2, 3, 4, 5, 6}, &conditions);

    if (selectedTables.empty())
      return;

    Table* sysColumns = this->masterDb->OpenTable("dbo", "sys_columns");
    vector<vector<Row>> selectedColumns(selectedTables.size());

    for (int i = 0; i < selectedTables.size(); i++)
      sysColumns->Select(selectedColumns[i], {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, &conditions);

     Table* sysIndexes = this->masterDb->OpenTable("dbo", "sys_indexes");
     vector<vector<Row>> selectedIndexes(selectedTables.size());
     
     for (int i = 0; i < selectedTables.size(); i++)
       sysIndexes->Select(selectedIndexes[i], {0, 1, 2, 3, 4, 5, 6, 7}, &conditions);

     Table* sysSchemas = this->masterDb->OpenTable("dbo", "sys_schemas");
     vector<Row> selectedSchemas;

     sysSchemas->Select(selectedSchemas, {0, 1, 2, 3, 4}, &conditions);
  }

  bool ServerInstance::DatabaseExists(const string &dbName) const{
     using namespace DatabaseEngine::StorageTypes;

     Table* sysDatabases = this->masterDb->OpenTable("dbo", "sys_databases");

     vector<Row> selectedDatabases;
     const vector<Field> conditions = {
       Field(dbName, 0),
     };

     sysDatabases->Select(selectedDatabases, {0}, &conditions);

     return !selectedDatabases.empty();
  }

  Headers::DatabaseHeader ServerInstance::SelectDatabases(const string &dbName) const{
     using namespace DatabaseEngine::StorageTypes;

     Table* sysDatabases = this->masterDb->OpenTable("dbo", 0);

     vector<Row> selectedDatabases;
     const vector<Field> conditions = {
       Field(dbName, 0),
     };

     sysDatabases->Select(selectedDatabases, {}, &conditions);

     Headers::DatabaseHeader header;

     if (selectedDatabases.empty())
       return {};

     const auto& database = selectedDatabases.front();

     const auto& data = database.GetData();

      return {
        .name = dbName,
        .filepath = data[1]->GetString(),
        .isSystem = data[2]->GetBool(),
        .createdAt = data[3]->GetDateTime(),
        .lastModified = data[4]->GetDateTime(),
        .lastModifiedBy = data[5]->GetString()
      };
  }

  vector<DatabaseEngine::StorageTypes::Row> ServerInstance::SelectSchemas(const string &dbName) const{
     using namespace DatabaseEngine::StorageTypes;

     const vector<Field> conditions = {
       Field(dbName, 0),
     };

     Table* sysSchemas = this->masterDb->OpenTable("dbo", "sys_schemas");
     vector<Row> selectedSchemas;

     sysSchemas->Select(selectedSchemas, {}, &conditions);

     return selectedSchemas;
  }

  bool ServerInstance::SchemaExists(const string &dbName, const std::string &schema) const{
    using namespace DatabaseEngine::StorageTypes;

    const vector<Field> conditions = {
      Field(dbName, 0),
      Field(schema, 1)
    };

    Table* sysSchemas = this->masterDb->OpenTable("dbo", "sys_schemas");
    vector<Row> selectedSchemas;

    Indexing::Key key;
    key.InsertKey(Indexing::Key(dbName.data(), dbName.size(), ColumnType::String));
    key.InsertKey(Indexing::Key(schema.data(), schema.size(), ColumnType::String));

    sysSchemas->ClusteredIndexSeek(&selectedSchemas, &key, &key, {});

    return !selectedSchemas.empty();
  }

  vector<Headers::TableHeader> ServerInstance::SelectTables(const string &dbName) const{
    using namespace DatabaseEngine::StorageTypes;

    const vector<Field> conditions = {
      Field(dbName, 0)
    };

    vector<Row> selectedTables;
    Table* sysTables = this->masterDb->OpenTable("dbo", "sys_tables");

    sysTables->Select(selectedTables, {}, &conditions);

    if (selectedTables.empty())
      return {};
      
    vector<Headers::TableHeader> selectedTableHeaders;
    selectedTableHeaders.reserve(selectedTables.size());

    for (const auto& table : selectedTables) {
      const auto& data = table.GetData();
      
      selectedTableHeaders.emplace_back(
          Headers::TableHeader{
              data[0]->GetString(),
              data[1]->GetString(),
            data[2]->GetSmallInt(),
              data[3]->GetString(),
              data[4]->GetBool(),
              data[5]->GetDateTime(),
              data[6]->GetDateTime(),
              data[7]->GetString()
          }
      );
    }

      ranges::sort(selectedTableHeaders,
      [](const Headers::TableHeader& a, const Headers::TableHeader& b) {
          return a.id < b.id;
      }
    );

    return selectedTableHeaders;
  }

  Headers::TableHeader ServerInstance::SelectTable(const string &dbName, const string &tableName) const{
    using namespace DatabaseEngine::StorageTypes;

    const vector<Field> conditions = {
      Field(dbName, 0)
    };

    vector<Row> selectedTables;
    Table* sysTables = this->masterDb->OpenTable("dbo", "sys_tables");

    Indexing::Key key;
    key.InsertKey(Indexing::Key(dbName.data(), dbName.size(), ColumnType::String));
    key.InsertKey(Indexing::Key(tableName.data(), tableName.size(), ColumnType::String));

    sysTables->ClusteredIndexSeek(&selectedTables, &key, &key, {});

    sysTables->Select(selectedTables, {}, &conditions);

    if (selectedTables.empty())
      return {};

    const auto& data = selectedTables[0].GetData();

    return  Headers::TableHeader{
      data[0]->GetString(),
      data[1]->GetString(),
      data[2]->GetSmallInt(),
      data[3]->GetString(),
      data[4]->GetBool(),
      data[5]->GetDateTime(),
      data[6]->GetDateTime(),
      data[7]->GetString()
    };
  }

  bool ServerInstance::TableExists(const string &dbName, const string &tableName, const std::string& schema) const{
    using namespace DatabaseEngine::StorageTypes;

    vector<Row> selectedTables;
    Table* sysTables = this->masterDb->OpenTable("dbo", "sys_tables");

    Indexing::Key key;

    key.InsertKey(Indexing::Key(dbName.data(), dbName.size(), ColumnType::String));
    key.InsertKey(Indexing::Key(tableName.data(), tableName.size(), ColumnType::String));
    key.InsertKey(Indexing::Key(schema.data(), schema.size(), ColumnType::String));

    sysTables->ClusteredIndexSeek(&selectedTables, &key, &key, {});

    // sysTables->Select(selectedTables, {}, &conditions);

    return !selectedTables.empty();
  }

  vector<Headers::ColumnHeader> ServerInstance::SelectColumns(const string &dbName, const string &tableName) const{
    using namespace DatabaseEngine::StorageTypes;

    const vector<Field> conditions = {
      Field(dbName, 0),
      Field(tableName, 1),
    };

    vector<Row> selectedColumns;
    Table* sysColumns = this->masterDb->OpenTable("dbo", "sys_columns");

    sysColumns->Select(selectedColumns, {}, &conditions);

    if (selectedColumns.empty())
      return {};

    vector<Headers::ColumnHeader> selectedColumnHeaders;
    selectedColumnHeaders.reserve(selectedColumns.size());
    
    for (const auto& column : selectedColumns) {
      const auto& data = column.GetData();

      selectedColumnHeaders.emplace_back(
        Headers::ColumnHeader{
          data[0]->GetString(),
          data[1]->GetString(),
          data[2]->GetString(),
          data[3]->GetString(),
          data[4]->GetSmallInt(),
          data[5]->GetBool(),
          data[6]->GetSmallInt(),
          data[7]->GetBool(),
          data[8]->GetDateTime(),
          data[9]->GetDateTime(),
          data[10]->GetString()
        }
      );
    }

    ranges::sort(selectedColumnHeaders,
        [](const Headers::ColumnHeader& a, const Headers::ColumnHeader& b) {
            return a.tablePosition < b.tablePosition;
        }
    );

     return selectedColumnHeaders;
  }

  Dictionary<string, Headers::ColumnHeader> ServerInstance::SelectColumnsToDictionary(const string &dbName, const string &tableName) const{
    const auto columns = this->SelectColumns(dbName, tableName);

    Dictionary<string, Headers::ColumnHeader> selectedColumns;

    for (const auto& column : columns)
      selectedColumns.Add(column.name, column);

    return selectedColumns;
  }

  vector<Headers::IndexHeader> ServerInstance::SelectIndexes(const string &dbName, const string &tableName) const{
     using namespace DatabaseEngine::StorageTypes;

     const vector<Field> conditions = {
       Field(dbName, 0),
       Field(tableName, 1),
     };

     Table* sysIndexes = this->masterDb->OpenTable("dbo", "sys_indexes");
     vector<Row> selectedIndexes;

    sysIndexes->Select(selectedIndexes, {}, &conditions);

    vector<Headers::IndexHeader> selectedIndexHeaders;

    for (const auto& index : selectedIndexes) {
      const auto& data = index.GetData();

      std::vector<column_index_t> columns;

      constexpr auto delimiter = ",";

      const char* token = strtok(data[3]->GetString().data(), delimiter);

      while (token != nullptr) {
        columns.emplace_back(SafeConverter<column_index_t>::SafeStoi(token));
        token = strtok(nullptr, delimiter);
      }

      selectedIndexHeaders.emplace_back(
        Headers::IndexHeader{
          data[0]->GetString(),
          data[1]->GetString(),
          data[2]->GetString(),
          std::move(columns),
          data[4]->GetBool(),
          data[5]->GetDateTime(),
          data[6]->GetDateTime(),
          data[7]->GetString()
        });
    }

    //get the clustered first
    ranges::sort(selectedIndexHeaders,
    [](const Headers::IndexHeader& a, const Headers::IndexHeader& b) {
        return a.isClustered > b.isClustered;
    });

     return selectedIndexHeaders;
  }

  void ServerInstance::CreateSystemDatabase(){
    using namespace DatabaseEngine;
    using namespace DatabaseEngine::StorageTypes;

    CreateDatabase(this->sysDbName);

    this->masterDb = this->UseDatabase(this->sysDbName, true);

    if (this->masterDb == nullptr)
      throw runtime_error("Failed to create" + this->sysDbName + " database");

    for (int i = 0;i < this->sysTables.size(); i++) {
      const auto& table = this->sysTables[i];
      
      vector<Column *> columns;
      vector<column_index_t> primaryKey;
      
      for (int j = 0;j < table.columns.size(); j++) {
        const auto& column = table.columns[j];

        block_size_t columnSize = 0;

        const auto normalizedColumnType = AdditionalLibraries::NormalizeString(column.type);
        
        if (!ColumnTypeSizes.TryGetValue(normalizedColumnType, columnSize))
          throw runtime_error("Column type " + column.type + " does not exist");

        if (columnSize == 0) 
          columnSize = column.size;

        const auto columnType = ColumnTypesDictionary.Get(normalizedColumnType);

        columns.push_back(new Column(column.name, columnType, columnSize, j, false));

        for (const auto& key: table.primaryKey) {
          if (column.name != key)
            continue;

          primaryKey.push_back(j);
        }
      }

      if (primaryKey.empty())
        throw runtime_error("All tables in masterDb must have a primary key");
        
      this->masterDb->CreateTable(table.name, "dbo", i, columns, &primaryKey); 
    }
  }

  bool ServerInstance::CheckIfMasterDbExists() const{ return std::filesystem::exists(this->sysDbPath); }
}

