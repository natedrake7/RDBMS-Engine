#include "Server.h"

#include "../AdditionalLibraries/SafeConverter/SafeConverter.h"

#include <fstream>
#include <nlohmann/json.hpp>
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

      //get last value etc for master db tables
//      this->SelectIndexes()

       std::cout << this->sysDbName << " initialized successfully" << std::endl;
       return;
     }

    this->CreateSystemDatabase();
    const auto dbInsertResult = this->InsertDbToMasterDb(this->sysDbName, this->sysDbPath, true);

    const auto schemaInsertResult = this->InsertSchemaToMasterDb(static_cast<int32_t>(dbInsertResult.primaryKeyVal), "dbo");


    Dictionary<string, column_index_t> columnNameToIndex;

    for (int i = 0;i < this->sysTables.size(); i++) {
      const auto& table = this->sysTables[i];

      const auto tableResult = this->InsertTableToMasterDb(
        static_cast<int32_t>(dbInsertResult.primaryKeyVal),
        static_cast<int32_t>(schemaInsertResult.primaryKeyVal),
        table.name,
        i,
        true);

      int columnPos = 0;
      for (auto& column: table.columns) {

        block_size_t columnSize;

        ColumnTypeSizes.TryGetValue(AdditionalLibraries::NormalizeString(column.type), columnSize);

        if (columnSize == 0)
          columnSize = column.size;

        this->InsertColumnToMasterDb(
           static_cast<int32_t>(tableResult.primaryKeyVal),
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

      //TODO keep the last value keys
      this->InsertIndexToMasterDb(
      static_cast<int32_t>(tableResult.primaryKeyVal),
      "PK" + _columns,
      concatenatedColumns,
      true,
      1,
      1,
      1);
    }

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

  AdditionalDataTypes::ResultStatus ServerInstance::InsertDbToMasterDb(const string& dbName, const string& dbPath, const bool& isSystem, const string& user) const{
      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SYSDATABASES);

      const auto currentDate = DataTypes::DateTime::Now();

      const vector<Field> fields = {
        Field(dbName, 1),
        Field(dbPath, 2),
        Field(isSystem, 3),
        Field(currentDate, 4),
        Field(currentDate, 5),
        Field(user, 6),
    };
    
    return table->InsertRow(fields);
  }

  AdditionalDataTypes::ResultStatus ServerInstance::InsertTableToMasterDb(
    const int32_t & databaseId,
    const int32_t & schemaId,
    const string& tableName,
    const table_id_t& tablePosition,
    const bool& isSystem,
    const string& user) const{

      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SYSTABLES);
      const auto currentDate = DataTypes::DateTime::Now();

      const vector<Field> fields = {
        Field(databaseId, 1),
        Field(schemaId, 2),
        Field(tableName, 3),
        Field(tablePosition, 4),
        Field(isSystem, 5),
        Field(currentDate, 6),
        Field(currentDate, 7),
        Field(user, 8),
      };
    
    return table->InsertRow(fields);
  }

  AdditionalDataTypes::ResultStatus  ServerInstance::InsertColumnToMasterDb(
    const int32_t & tableId,
    const string &columnName,
    const string &columnType,
    const int &columnSize,
    const bool& isNullable,
    const int &tablePosition,
    const bool& isSystem,
    const string& user) const{
      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SYSCOLUMNS);
      const auto currentDate = DataTypes::DateTime::Now();

      const vector<Field> fields = {
        Field(tableId, 1),
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

    return table->InsertRow(fields);
  }

  AdditionalDataTypes::ResultStatus  ServerInstance::InsertIndexToMasterDb(
    const int32_t & tableId,
    const string &indexName,
    const string &columns,
    const bool &isClustered,
    const int32_t& seed,
    const int32_t& incrementFactor,
    const int32_t& lastValue,
    const string &user) const{
     DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SYSINDEXES);
     const auto currentDate = DataTypes::DateTime::Now();

     const vector<Field> fields = {
       Field(tableId, 1),
       Field(indexName, 2),
       Field(columns, 3),
       Field(isClustered, 4),
       Field(seed, 5),
       Field(incrementFactor, 6),
       Field(lastValue, 7),
       Field(currentDate, 8),
       Field(currentDate, 9),
       Field(user, 10),
     };

     return table->InsertRow(fields);
  }

  AdditionalDataTypes::ResultStatus  ServerInstance::InsertSchemaToMasterDb(
    const int32_t &databaseId,
    const string &schemaName,
    const string &user) const{
     DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SYSSCHEMAS);
     const auto currentDate = DataTypes::DateTime::Now();

     const vector<Field> fields = {
        Field(databaseId, 1),
        Field(schemaName, 2),
        Field(currentDate, 3),
        Field(currentDate, 4),
        Field(user, 5),
     };

     return table->InsertRow(fields);
  }

  bool ServerInstance::DatabaseExists(const string &dbName) const{
     using namespace DatabaseEngine::StorageTypes;

     Table* sysDatabases = this->masterDb->OpenTable(MasterDbTables::SYSDATABASES);
     vector<Row> selectedDatabases;

    Indexing::Key key;
    key.InsertKey(Indexing::Key(dbName.data(), dbName.size(), ColumnType::String));

     sysDatabases->ClusteredIndexSeek(&selectedDatabases, &key, &key);

     return !selectedDatabases.empty();
  }

  vector<Headers::DatabaseHeader> ServerInstance::GetCatalog() const{
     using namespace DatabaseEngine::StorageTypes;

     Table* sysDatabases = this->masterDb->OpenTable(MasterDbTables::SYSDATABASES);

     vector<Row> selectedDatabases;

     sysDatabases->ClusteredIndexScan(&selectedDatabases);

     vector<Headers::DatabaseHeader> databasesHeaders;

     if (selectedDatabases.empty())
       return {};

    for (const auto& row : selectedDatabases) {

      const auto& data = row.GetData();

      const auto databaseId = data[0]->GetInt();

      const auto& dbName = data[1]->GetString();

      auto schemas = this->SelectSchemas(databaseId);

      auto dbTables = this->SelectTables(dbName);

      for (auto& table : dbTables) {
          table.columns = this->SelectColumns(table.id);
          table.indexes = this->SelectIndexes(table.id);
      }

      databasesHeaders.emplace_back(Headers::DatabaseHeader{
        .id = databaseId,
        .name = dbName,
        .filepath = data[2]->GetString(),
        .isSystem = data[3]->GetBool(),
        .createdAt = data[4]->GetDateTime(),
        .lastModified = data[5]->GetDateTime(),
        .lastModifiedBy = data[6]->GetString(),
        .tables = std::move(dbTables),
        .schemas = std::move(schemas)
      });
    }

    return databasesHeaders;
  }

  Headers::DatabaseHeader ServerInstance::SelectDatabase(const std::string &name) const{
    using namespace DatabaseEngine::StorageTypes;

    auto expression = Expressions::Expression::Predicate(1, "=", Field(name, 1));

    Table* sysDatabases = this->masterDb->OpenTable(MasterDbTables::SYSDATABASES);
    vector<Row> selectedDatabases;

    sysDatabases->ClusteredIndexScan(&selectedDatabases, &expression);

    if (selectedDatabases.empty())
      return {};

    const auto& data = selectedDatabases[0].GetData();
    
    return Headers::DatabaseHeader{
      .id = data[0]->GetInt(),
      .name = data[1]->GetString(),
      .filepath = data[2]->GetString(),
      .isSystem = data[3]->GetBool(),
      .createdAt = data[4]->GetDateTime(),
      .lastModified = data[5]->GetDateTime(),
      .lastModifiedBy = data[6]->GetString()
    };
  }

  vector<Headers::SchemaHeader> ServerInstance::SelectSchemas(const int32_t& databaseId) const{
     using namespace DatabaseEngine::StorageTypes;

     Table* sysSchemas = this->masterDb->OpenTable(MasterDbTables::SYSSCHEMAS);
     vector<Row> selectedSchemas;

    auto expression = Expressions::Expression::Predicate(1, "=", Field(databaseId, 1));

    sysSchemas->ClusteredIndexScan(&selectedSchemas, &expression);

    if (selectedSchemas.empty())
      return {};
    
    vector<Headers::SchemaHeader> schemas;

    for (const auto& row : selectedSchemas) {
      const auto& data = row.GetData();

      schemas.emplace_back(Headers::SchemaHeader{
        data[0]->GetInt(),
        data[1]->GetInt(),
        data[2]->GetString(),
        data[3]->GetDateTime(),
  data[4]->GetDateTime(),
        data[5]->GetString()
      });
    }

     return schemas;
  }

  bool ServerInstance::SchemaExists(const string &dbName, const std::string &schema) const{
    using namespace DatabaseEngine::StorageTypes;

    auto databaseHeader = this->SelectDatabase(dbName);

    auto expression = Expressions::Expression::Predicate(1, "=", Field(databaseHeader.id, 1));

    Table* sysSchemas = this->masterDb->OpenTable(MasterDbTables::SYSSCHEMAS);
    vector<Row> selectedSchemas;

    sysSchemas->ClusteredIndexScan(&selectedSchemas, &expression);

    return !selectedSchemas.empty();
  }

  vector<Headers::TableHeader> ServerInstance::SelectTables(const string &dbName) const{
    using namespace DatabaseEngine::StorageTypes;

    auto databaseHeader = this->SelectDatabase(dbName);

    auto expression = Expressions::Expression::Predicate(1, "=", Field(databaseHeader.id, 1));

    vector<Row> selectedTables;

    Table* sysTablesPtr = this->masterDb->OpenTable(MasterDbTables::SYSTABLES);

    sysTablesPtr->ClusteredIndexScan(&selectedTables, &expression);

    if (selectedTables.empty())
      return {};
      
    vector<Headers::TableHeader> selectedTableHeaders;
    selectedTableHeaders.reserve(selectedTables.size());

    for (const auto& table : selectedTables) {
      const auto& data = table.GetData();
      
      selectedTableHeaders.emplace_back(
          Headers::TableHeader{
              data[0]->GetInt(),
              data[1]->GetInt(),
              data[2]->GetInt(),
              data[3]->GetString(),
              data[4]->GetSmallInt(),
                data[5]->GetBool(),
              data[6]->GetDateTime(),
              data[7]->GetDateTime(),
              data[8]->GetString()
          }
      );
    }

      ranges::sort(selectedTableHeaders,
      [](const Headers::TableHeader& a, const Headers::TableHeader& b) {
          return a.tablePosition < b.tablePosition;
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
    Table* table = this->masterDb->OpenTable(MasterDbTables::SYSTABLES);

    Indexing::Key key;
    key.InsertKey(Indexing::Key(dbName.data(), dbName.size(), ColumnType::String));
    key.InsertKey(Indexing::Key(tableName.data(), tableName.size(), ColumnType::String));

    table->ClusteredIndexSeek(&selectedTables, &key, &key);

    if (selectedTables.empty())
      return {};

    const auto& data = selectedTables[0].GetData();

    return  Headers::TableHeader{
          data[0]->GetInt(),
          data[1]->GetInt(),
          data[2]->GetInt(),
          data[3]->GetString(),
          data[4]->GetSmallInt(),
            data[5]->GetBool(),
          data[6]->GetDateTime(),
          data[7]->GetDateTime(),
          data[8]->GetString()
    };
  }

  Headers::TableHeader ServerInstance::SelectTable(const string &dbName, const string &tableName, const std::string& schema) const{
    using namespace DatabaseEngine::StorageTypes;

    const auto dbHeader = this->SelectDatabase(dbName);

    if(dbHeader.id  == 0)
      return {};

    vector<Row> selectedTables;
    Table* sysTablesPtr = this->masterDb->OpenTable(MasterDbTables::SYSTABLES);

    auto leftExpr = Expressions::Expression::Predicate(1, "=", Field(dbHeader.id, 1));
    auto rightExpr = Expressions::Expression::Predicate(1, "=", Field(tableName, 2));
    auto expr = Expressions::Expression::Logical(Expressions::ExpressionType::And, &leftExpr, &rightExpr);

    sysTablesPtr->ClusteredIndexScan(&selectedTables, &expr);

    if (selectedTables.empty())
      return {};

    const auto& data = selectedTables[0].GetData();

    return  Headers::TableHeader{
          data[0]->GetInt(),
          data[1]->GetInt(),
          data[2]->GetInt(),
          data[3]->GetString(),
          data[4]->GetSmallInt(),
            data[5]->GetBool(),
          data[6]->GetDateTime(),
          data[7]->GetDateTime(),
          data[8]->GetString()
    };
  }

  vector<Headers::ColumnHeader> ServerInstance::SelectColumns(const int32_t& tableId) const{
    using namespace DatabaseEngine::StorageTypes;

    vector<Row> selectedColumns;
    Table* sysColumns = this->masterDb->OpenTable(MasterDbTables::SYSCOLUMNS);


    auto expression = Expressions::Expression::Predicate(1, "=", Field(tableId, 1));

    sysColumns->ClusteredIndexScan(&selectedColumns, &expression);

    if (selectedColumns.empty())
      return {};

    vector<Headers::ColumnHeader> selectedColumnHeaders;
    selectedColumnHeaders.reserve(selectedColumns.size());
    
    for (const auto& column : selectedColumns) {
      const auto& data = column.GetData();

      selectedColumnHeaders.emplace_back(
        Headers::ColumnHeader{
          data[0]->GetInt(),
          data[1]->GetInt(),
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

  Dictionary<string, Headers::ColumnHeader> ServerInstance::SelectColumnsToDictionary(const int32_t& tableId) const{
      const auto columns = this->SelectColumns(tableId);

      Dictionary<string, Headers::ColumnHeader> selectedColumns;

      for (const auto& column : columns)
        selectedColumns.Add(column.name, column);

      return selectedColumns;
    }

    vector<Headers::IndexHeader> ServerInstance::SelectIndexes(const int32_t& tableId) const{
      using namespace DatabaseEngine::StorageTypes;

      Table* sysIndexes = this->masterDb->OpenTable(MasterDbTables::SYSINDEXES);
      vector<Row> selectedIndexes;

      auto expression = Expressions::Expression::Predicate(1, "=", Field(tableId, 1));

      sysIndexes->ClusteredIndexScan(&selectedIndexes, &expression);

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
          data[0]->GetInt(),
          data[1]->GetInt(),
          data[2]->GetString(),
          std::move(columns),
          data[4]->GetBool(),
          data[5]->GetInt(),
          data[6]->GetInt(),
          data[7]->GetInt(),
          data[8]->GetDateTime(),
          data[9]->GetDateTime(),
          data[10]->GetString()
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

      Headers::Index index(primaryKey, 1, 1);

      this->masterDb->CreateTable(table.name, "dbo", i, columns, &index);
    }
  }

  bool ServerInstance::CheckIfMasterDbExists() const{ return std::filesystem::exists(this->sysDbPath); }
}

