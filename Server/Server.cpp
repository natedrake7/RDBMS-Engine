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
    if (j.contains("default"))
      j.at("default").get_to(c._default);
    if (j.contains("nullable"))
      j.at("nullable").get_to(c.nullable);
  }

  void from_json(const json& j, sysTable& t) {
    j.at("name").get_to(t.name);
    j.at("id").get_to(t.id);
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
    const auto dbInsertResult = this->InsertDbToMasterDb(this->sysDbName, this->sysDbPath, true);

    const auto schemaInsertResult = this->InsertSchemaToMasterDb(static_cast<int32_t>(dbInsertResult.primaryKeyVal), "dbo");


    Dictionary<string, column_index_t> columnNameToIndex;

    for (int i = 0;i < this->sysTables.size(); i++) {
      const auto& table = this->sysTables[i];

      const auto tableResult =
      this->InsertTableToMasterDb(
        static_cast<int32_t>(dbInsertResult.primaryKeyVal),
        static_cast<int32_t>(schemaInsertResult.primaryKeyVal),
        table.name,
        static_cast<int16_t>(i),
        true);

      int columnPos = 0;

      Dictionary<std::string, int32_t> columnIdsDict;

      for (auto& column: table.columns) {

        block_size_t columnSize;
        ColumnTypeSizes.TryGetValue(AdditionalLibraries::NormalizeString(column.type), columnSize);

        if (columnSize == 0)
          columnSize = column.size;

        ColumnType type;
        ColumnTypesDictionary.TryGetValue(AdditionalLibraries::NormalizeString(column.type), type);

        const auto columnResult =
          this->InsertColumnToMasterDb(
           static_cast<int32_t>(tableResult.primaryKeyVal),
           column.name,
           type,
           columnSize,
           column.nullable,
           columnPos,
           true);

        columnNameToIndex.Add(column.name, columnPos);
        columnIdsDict.Add(column.name, static_cast<int32_t>(columnResult.primaryKeyVal));

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
      const auto indexResult =
        this->InsertIndexToMasterDb(
        static_cast<int32_t>(tableResult.primaryKeyVal),
        "PK" + _columns,
        true);

      auto indexKey = static_cast<int32_t>(indexResult.primaryKeyVal);

      const auto constraintResult =
          this->InsertConstraintToMasterDb(
          static_cast<int32_t>(tableResult.primaryKeyVal),
          "PK" + _columns,
          Headers::ConstraintType::PrimaryKey,
          false,
          &indexKey);

      for(int j = 0;j < table.primaryKey.size(); j++){
        const auto& columnIndex = columnNameToIndex.Get(table.primaryKey[j]);

        this->InsertIndexColumnToMasterDb(
          static_cast<int32_t>(indexResult.primaryKeyVal),
          columnIdsDict.Get(table.primaryKey[j]),
          static_cast<int16_t>(j),
          true);

        this->InsertConstraintColumnToMasterDb(
          static_cast<int32_t>(constraintResult.primaryKeyVal),
          columnIdsDict.Get(table.primaryKey[j]),
          static_cast<int16_t>(j)
        );

        if(table.primaryKey.size() == 1){
          this->InsertIdentityColumnToMasterDb(
            static_cast<int32_t>(tableResult.primaryKeyVal),
            columnIdsDict.Get(table.primaryKey[j]),
            1,
            1,
            1,
            true,
            10000
          );
        }
      }
    }

    this->masterDb->GetColumnsHeaders();
    // this->masterDb->GetIdentityColumns();

    std::cout << this->sysDbName << " initialized successfully" << std::endl;
  }

  DatabaseEngine::Database * ServerInstance::GetMasterDb()const{ return this->masterDb; }

  void ServerInstance::Shutdown(){
    for (const auto &database: this->databases | views::values){
      database->UpdateMasterDatabase();
      delete database;
    }

    this->masterDb->UpdateMasterDatabase();
    delete this->masterDb;
  }

  DatabaseEngine::Database* ServerInstance::UseDatabase(const int32_t & databaseId, const bool& isServerInitialization){
    DatabaseEngine::Database *db = nullptr;

    if (this->databases.TryGetValue(databaseId, db))
      return db;

    const auto dbHeader = this->SelectDatabaseById(databaseId);

    db = new DatabaseEngine::Database(dbHeader.name, isServerInitialization);

    // db->GetIdentityColumns();

    //master db id
    if (databaseId != 1)
      this->databases.Add(databaseId, db);

    return db;
  }


  void ServerInstance::UseMasterDb(){
    this->masterDb = new DatabaseEngine::Database(this->sysDbName, this->sysTables);
    this->masterDb->GetColumnsHeaders();
    this->masterDb->GetIdentityColumns();
  }

  AdditionalDataTypes::ResultStatus ServerInstance::InsertDbToMasterDb(
    const string& dbName,
    const string& dbPath,
    const bool& isSystem,
    const string& user,
    const int& version,
    const bool& isDeleted) const{
      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SYSDATABASES);

      const auto currentDate = DataTypes::DateTime::Now();

      const vector<Field> fields = {
        Field(dbName, 1),
        Field(dbPath, 2),
        Field(isSystem, 3),
        Field(currentDate, 4),
        Field(currentDate, 5),
        Field(user, 6),
        Field(version, 7),
        Field(isDeleted, 8),
        Field(nullptr, 9),
    };

    const auto result = table->InsertRow(fields);

    cout << "Inserted database: "<< dbName << " to master db" << endl;
    
    return result;
  }

  AdditionalDataTypes::ResultStatus  ServerInstance::InsertSchemaToMasterDb(
    const int32_t &databaseId,
    const string &schemaName,
    const string &user,
    const int& version,
    const bool& isDeleted) const{
     DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SYSSCHEMAS);
     const auto currentDate = DataTypes::DateTime::Now();

     const vector<Field> fields = {
        Field(databaseId, 1),
        Field(schemaName, 2),
        Field(currentDate, 3),
        Field(currentDate, 4),
        Field(user, 5),
        Field(version, 6),
        Field(isDeleted, 7),
        Field(nullptr, 8),
     };

    const auto result = table->InsertRow(fields);

    cout << "Inserted schema: "<< schemaName << " to master db" << endl;
    
    return result;
  }

  AdditionalDataTypes::ResultStatus  ServerInstance::InsertTableToMasterDb(
    const int32_t & databaseId,
    const int32_t & schemaId,
    const string& tableName,
    const int16_t& ordinalPosition,
    const bool& isSystem,
    const string& user,
    const int& version,
    const bool& isDeleted) const{

      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SYSTABLES);
      const auto currentDate = DataTypes::DateTime::Now();

      const vector<Field> fields = {
        Field(databaseId, 1),
        Field(schemaId, 2),
        Field(tableName, 3),
        Field(ordinalPosition, 4),
        Field(isSystem, 5),
        Field(currentDate, 6),
        Field(currentDate, 7),
        Field(user, 8),
        Field(version, 9),
        Field(isDeleted, 10),
        Field(nullptr, 11),
      };

      const auto result = table->InsertRow(fields);

      cout << "Inserted table: "<< tableName << " to master db" << endl;
      
      return result;
  }

  AdditionalDataTypes::ResultStatus  ServerInstance::InsertColumnToMasterDb(
    const int32_t & tableId,
    const string &columnName,
    const ColumnType &columnType,
    const int &columnSize,
    const bool& isNullable,
    const int &ordinalPosition,
    const bool& isSystem,
    const string& user,
    const int& version,
    const bool& isDeleted) const{
      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SYSCOLUMNS);
      const auto currentDate = DataTypes::DateTime::Now();

      const vector<Field> fields = {
        Field(tableId, 1),
        Field(columnName, 2),
        Field(static_cast<int8_t>(columnType), 3),
        Field(columnSize, 4),
        Field(isNullable, 5),
        Field(ordinalPosition, 6),
        Field(isSystem, 7),
        Field(currentDate, 8),
        Field(currentDate, 9),
        Field(user, 10),
        Field(version, 11),
        Field(isDeleted, 12),
        Field(nullptr, 13),
      };

      const auto result = table->InsertRow(fields);

      cout << "Inserted column: "<< columnName << " to master db" << endl;
        
      return result;
  }

  AdditionalDataTypes::ResultStatus  ServerInstance::InsertIndexToMasterDb(
    const int32_t & tableId,
    const string &indexName,
    const bool &isClustered,
    const bool &isDisabled,
    const string &user,
    const int& version,
    const bool& isDeleted) const{
     DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SYSINDEXES);
     const auto currentDate = DataTypes::DateTime::Now();

     const vector<Field> fields = {
       Field(tableId, 1),
       Field(indexName, 2),
       Field(isClustered, 3),
       Field(isDisabled, 4),
       Field(currentDate, 5),
       Field(currentDate, 6),
       Field(user, 7),
       Field(version, 8),
       Field(isDeleted, 9),
      Field(nullptr, 10),
     };

      const auto result = table->InsertRow(fields);

      cout << "Inserted index: "<< indexName << " to master db" << endl;
          
      return result;
  }

  AdditionalDataTypes::ResultStatus ServerInstance::InsertIndexColumnToMasterDb(
    const int32_t & indexId,
    const int32_t & columnId,
    const int16_t & ordinalPosition,
    const bool & isIncluded,
    const int& version,
    const bool& isDeleted) const{
    DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SYSINDEXCOLUMNS);
    const auto currentDate = DataTypes::DateTime::Now();

    const vector<Field> fields = {
      Field(indexId, 0),
      Field(columnId, 1),
      Field(ordinalPosition, 2),
      Field(isIncluded, 3),
      Field(version, 4),
      Field(isDeleted, 5),
      Field(nullptr, 6),
    };

    const auto result = table->InsertRow(fields);

    cout << "Inserted index column to master db" << endl;
          
    return result;
  }

  AdditionalDataTypes::ResultStatus ServerInstance::InsertIdentityColumnToMasterDb(
      const int32_t & tableId,
      const int32_t & columnId,
      const int32_t & seedValue,
      const int32_t & increment,
      const int32_t & lastValue,
      const bool & isCached,
      const int32_t & cacheBlock,
      const int& version,
      const bool& isDeleted) const{

      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SYSIDENTITYCOLUMNS);
      const auto currentDate = DataTypes::DateTime::Now();

      const vector<Field> fields = {
        Field(tableId, 0),
        Field(columnId, 1),
        Field(seedValue, 2),
        Field(increment, 3),
        Field(lastValue, 4),
        Field(isCached, 5),
        Field(cacheBlock, 6),
        Field(version, 7),
        Field(isDeleted, 8),
        Field(nullptr, 9),
      };

    const auto result = table->InsertRow(fields);

    cout << "Inserted identity column to master db" << endl;
          
    return result;
  }

  AdditionalDataTypes::ResultStatus ServerInstance::InsertConstraintToMasterDb(
      const int32_t & tableId,
      const string & constraintName,
      const Headers::ConstraintType & constraintType,
      const bool & isDisabled,
      const int32_t *constraintIndexId,
      const string & user,
      const int& version,
      const bool& isDeleted) const{

      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SYSCONSTRAINTS);
      const auto currentDate = DataTypes::DateTime::Now();

      vector<Field> fields = {
          Field(tableId, 1),
          Field(constraintName, 2),
          Field(static_cast<int8_t>(constraintType), 3),
          Field(isDisabled, 4),
          Field(nullptr, 5),
          Field(currentDate, 6),
          Field(currentDate, 7),
          Field(user, 8),
          Field(version, 9),
          Field(isDeleted, 10),
          Field(nullptr, 11),
      };

      if(constraintIndexId != nullptr)
          fields.at(4).SetData(*constraintIndexId);

      const auto result = table->InsertRow(fields);

      cout << "Inserted constraint: "<< constraintName <<" to master db" << endl;
            
      return result;
  }

  AdditionalDataTypes::ResultStatus ServerInstance::InsertConstraintColumnToMasterDb(
    const int32_t & constraintId,
    const int32_t & columnId,
    const int32_t & ordinalPosition,
    const int& version,
    const bool& isDeleted) const{

    DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SYSCONSTRAINTCOLUMNS);
    const auto currentDate = DataTypes::DateTime::Now();

    const vector<Field> fields = {
        Field(constraintId, 0),
        Field(columnId, 1),
        Field(ordinalPosition, 2),
        Field(version, 3),
        Field(isDeleted, 4),
        Field(nullptr, 5),
    };

    const auto result = table->InsertRow(fields);

    cout << "Inserted constraint column to master db" << endl;
            
    return result;
  }

  bool ServerInstance::DatabaseExists(const string &dbName) const{
      using namespace DatabaseEngine::StorageTypes;

      Table* sysDatabases = this->masterDb->OpenTable(MasterDbTables::SYSDATABASES);
      vector<Row> selectedDatabases;

      Indexing::Key key;
      key.InsertKey(Indexing::Key(dbName.data(), dbName.size(), ColumnType::String));

      auto expression = Expressions::Expression::Predicate(1, Expressions::ExpressionOperator::Equal, Field(dbName, 1));

      sysDatabases->ClusteredIndexScan(&selectedDatabases, &expression);

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
          table.constraints = this->SelectConstraints(table.id);
          table.identity = this->SelectIdentityColumnsByTableId(table.id);
      }

      databasesHeaders.emplace_back(Headers::DatabaseHeader{
        .id = databaseId,
        .name = dbName,
        .filepath = data[2]->GetString(),
        .isSystem = data[3]->GetBool(),
        .additionalInfo = {
          .createdAt = data[4]->GetDateTime(),
          .lastModified = data[5]->GetDateTime(),
          .lastModifiedBy = data[6]->GetString(),
          .version = data[7]->GetInt(),
          .isDeleted = data[8]->GetBool(),
          .deletedAt = data[9]->GetBlockData() == nullptr
                    ? DataTypes::DateTime()
                    : data[9]->GetDateTime(),
        },
        .tables = std::move(dbTables),
        .schemas = std::move(schemas)
      });
    }

    return databasesHeaders;
  }

  Headers::DatabaseHeader ServerInstance::SelectDatabase(const std::string &name) const{
    using namespace DatabaseEngine::StorageTypes;

    auto expression = Expressions::Expression::Predicate(1, Expressions::ExpressionOperator::Equal, Field(name, 1));

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
    };
  }

  Headers::DatabaseHeader ServerInstance::SelectDatabaseById(const int32_t & databaseId) const{
    using namespace DatabaseEngine::StorageTypes;

    Table* sysDatabases = this->masterDb->OpenTable(MasterDbTables::SYSDATABASES);
    vector<Row> selectedDatabases;

    Indexing::Key key;
    key.InsertKey(Indexing::Key(&databaseId, sizeof(databaseId), ColumnType::Int));

    sysDatabases->ClusteredIndexSeek(&selectedDatabases, &key, &key);

    if (selectedDatabases.empty())
      return {};

    const auto& data = selectedDatabases[0].GetData();

    return Headers::DatabaseHeader{
      .id = data[0]->GetInt(),
      .name = data[1]->GetString(),
      .filepath = data[2]->GetString(),
      .isSystem = data[3]->GetBool(),
    };
  }

  vector<Headers::SchemaHeader> ServerInstance::SelectSchemas(const int32_t& databaseId) const{
     using namespace DatabaseEngine::StorageTypes;

     Table* sysSchemas = this->masterDb->OpenTable(MasterDbTables::SYSSCHEMAS);
     vector<Row> selectedSchemas;

    auto expression = Expressions::Expression::Predicate(1, Expressions::ExpressionOperator::Equal, Field(databaseId, 1));

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

  bool ServerInstance::SchemaExists(const int32_t &databaseId, const std::string &schema) const{
    using namespace DatabaseEngine::StorageTypes;

    auto *leftExpr =
                new Expressions::Expression{
                    .type = Expressions::ExpressionType::Predicate,
                    .left = nullptr,
                    .right = nullptr,
                    .operation = Expressions::ExpressionOperator::Equal,
                    .value = Field(databaseId, 1),
                    .columnIndex = 1
                };

    auto *rightExpr =
              new Expressions::Expression{
                    .type = Expressions::ExpressionType::Predicate,
                    .left = nullptr,
                    .right = nullptr,
                    .operation = Expressions::ExpressionOperator::Equal,
                    .value = Field(schema, 2),
                    .columnIndex = 2
                };

    auto expr = Expressions::Expression::Logical(Expressions::ExpressionType::And, leftExpr, rightExpr);

    Table* sysSchemas = this->masterDb->OpenTable(MasterDbTables::SYSSCHEMAS);
    vector<Row> selectedSchemas;

    sysSchemas->ClusteredIndexScan(&selectedSchemas, &expr);

    return !selectedSchemas.empty();
  }

  vector<Headers::TableHeader> ServerInstance::SelectTables(const string &dbName) const{
    using namespace DatabaseEngine::StorageTypes;

    auto databaseHeader = this->SelectDatabase(dbName);

    auto expression = Expressions::Expression::Predicate(1, Expressions::ExpressionOperator::Equal, Field(databaseHeader.id, 1));

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
          return a.ordinalPosition < b.ordinalPosition;
      }
    );

    return selectedTableHeaders;
  }

  vector<Headers::TableHeader> ServerInstance::SelectTables(const int32_t & databaseId) const{
    using namespace DatabaseEngine::StorageTypes;

    auto expression = Expressions::Expression::Predicate(1, Expressions::ExpressionOperator::Equal, Field(databaseId, 1));

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
        return a.ordinalPosition < b.ordinalPosition;
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

  Headers::TableHeader ServerInstance::SelectTable(const int32_t &databaseId, const string &tableName, const std::string& schema) const{
    using namespace DatabaseEngine::StorageTypes;

    vector<Row> selectedTables;
    Table* sysTablesPtr = this->masterDb->OpenTable(MasterDbTables::SYSTABLES);

    auto *leftExpr =
                new Expressions::Expression{
                    .type = Expressions::ExpressionType::Predicate,
                    .left = nullptr,
                    .right = nullptr,
                    .operation = Expressions::ExpressionOperator::Equal,
                    .value = Field(databaseId, 1),
                    .columnIndex = 1
                };

    auto *rightExpr =
              new Expressions::Expression{
                    .type = Expressions::ExpressionType::Predicate,
                    .left = nullptr,
                    .right = nullptr,
                    .operation = Expressions::ExpressionOperator::Equal,
                    .value = Field(tableName, 3),
                    .columnIndex = 3
                };

    auto expr = Expressions::Expression::Logical(Expressions::ExpressionType::And, leftExpr, rightExpr);

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


    auto expression = Expressions::Expression::Predicate(1, Expressions::ExpressionOperator::Equal, Field(tableId, 1));

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
          static_cast<uint8_t>(data[3]->GetTinyInt()),
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
            return a.ordinalPosition < b.ordinalPosition;
        }
    );

     return selectedColumnHeaders;
  }

  vector<Headers::ConstraintsHeader> ServerInstance::SelectConstraints(const int32_t & tableId) const{
    using namespace DatabaseEngine::StorageTypes;

    vector<Row> selectedConstraints;
    Table* constraintsTable = this->masterDb->OpenTable(MasterDbTables::SYSCONSTRAINTS);

    auto expression = Expressions::Expression::Predicate(1, Expressions::ExpressionOperator::Equal, Field(tableId, 1));

    constraintsTable->ClusteredIndexScan(&selectedConstraints, &expression);

    if (selectedConstraints.empty())
      return {};

    vector<Headers::ConstraintsHeader> selectedConstraintsHeader;
    selectedConstraintsHeader.reserve(selectedConstraints.size());

    for (const auto& column : selectedConstraints) {
      const auto& data = column.GetData();

      auto constraintColumns = this->SelectConstraintColumnsByConstraintId(data[0]->GetInt());

      const auto indexId =(data[5]->GetBlockData() == nullptr)
              ? -1
              : data[5]->GetInt();

      Headers::IndexHeader index;
      if(indexId != -1)
        index = this->SelectIndexById(indexId);

      selectedConstraintsHeader.emplace_back(
        Headers::ConstraintsHeader{
          .constraintId = data[0]->GetInt(),
          .tableId = data[1]->GetInt(),
          .name = data[2]->GetString(),
          .type = static_cast<Headers::ConstraintType>(data[3]->GetTinyInt()),
          .isDisabled = data[4]->GetBool(),
          .indexId = indexId,
          .index = std::move(index),
          .columns = std::move(constraintColumns),
          .additionalInfo{
              .createdAt = data[6]->GetDateTime(),
              .lastModified = data[7]->GetDateTime(),
              .lastModifiedBy = data[8]->GetString(),
              .version = data[9]->GetInt(),
              .isDeleted = data[10]->GetBool(),
              .deletedAt = data[11]->GetBlockData() == nullptr
                    ? DataTypes::DateTime()
                    : data[11]->GetDateTime() //might crash, is nullable
          },
        }
      );
    }

    ranges::sort(selectedConstraintsHeader,
    [](const Headers::ConstraintsHeader& a, const Headers::ConstraintsHeader& b) {
        return a.constraintId < b.constraintId;
    }
    );

    return selectedConstraintsHeader;
  }

  vector<Headers::ConstraintsColumnsHeader> ServerInstance::SelectConstraintColumnsByConstraintId(const int32_t & constraintId) const{
    using namespace DatabaseEngine::StorageTypes;

    Table* sysIndexes = this->masterDb->OpenTable(MasterDbTables::SYSCONSTRAINTCOLUMNS);
    vector<Row> rows;

    Indexing::Key key;
    key.InsertKey(Indexing::Key(&constraintId, sizeof(constraintId), ColumnType::Int));

    sysIndexes->ClusteredIndexSeek(&rows, &key, &key);

    if(rows.empty())
      return {};

    vector<Headers::ConstraintsColumnsHeader> constraintColumns;

    for(const auto& row : rows){
      const auto& data = row.GetData();

      constraintColumns.emplace_back(
          Headers::ConstraintsColumnsHeader{
            .constraintId = data[0]->GetInt(),
            .columnId = data[1]->GetInt(),
            .ordinalPosition = data[2]->GetInt(),
            .additionalInfo{
              .version = data[3]->GetInt(),
              .isDeleted = data[4]->GetBool(),
              .deletedAt = data[5]->GetBlockData() == nullptr
                    ? DataTypes::DateTime()
                    : data[5]->GetDateTime()
            },
          }
      );
    }

    ranges::sort(constraintColumns,
      [](const Headers::ConstraintsColumnsHeader& a, const Headers::ConstraintsColumnsHeader& b) {
          return a.ordinalPosition < b.ordinalPosition;
      }
    );

    return constraintColumns;
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

      auto expression = Expressions::Expression::Predicate(1, Expressions::ExpressionOperator::Equal, Field(tableId, 1));

      sysIndexes->ClusteredIndexScan(&selectedIndexes, &expression);

      vector<Headers::IndexHeader> selectedIndexHeaders;

      for (const auto& index : selectedIndexes) {
        const auto& data = index.GetData();

        selectedIndexHeaders.emplace_back(
        Headers::IndexHeader{
          .id = data[0]->GetInt(),
          .tableId = data[1]->GetInt(),
          .name = data[2]->GetString(),
          .isClustered = data[3]->GetBool(),
          .isDisabled = data[4]->GetBool(),
            .additionalInfo{
            .createdAt = data[5]->GetDateTime(),
            .lastModified = data[6]->GetDateTime(),
            .lastModifiedBy = data[7]->GetString(),
            .version = data[8]->GetInt(),
            .isDeleted = data[9]->GetBool(),
            .deletedAt = data[10]->GetBlockData() == nullptr
                  ? DataTypes::DateTime()
                  : data[10]->GetDateTime()
            },
        });
      }

      //get the clustered first
      ranges::sort(selectedIndexHeaders,
      [](const Headers::IndexHeader& a, const Headers::IndexHeader& b) {
        return a.isClustered > b.isClustered;
      });

      return selectedIndexHeaders;
  }

  Headers::IndexHeader ServerInstance::SelectIndexById(const int32_t & indexId) const{
    using namespace DatabaseEngine::StorageTypes;

    Table* sysIndexes = this->masterDb->OpenTable(MasterDbTables::SYSINDEXES);
    vector<Row> selectedIndexes;

    Indexing::Key key;
    key.InsertKey(Indexing::Key(&indexId, sizeof(indexId), ColumnType::Int));

    sysIndexes->ClusteredIndexSeek(&selectedIndexes, &key, &key);

    if(selectedIndexes.empty())
      return {};

    auto indexColumns = this->SelectIndexColumnsByIndexId(indexId);

    vector<Headers::IndexHeader> selectedIndexHeaders;

    const auto& data = selectedIndexes[0].GetData();

    return Headers::IndexHeader{
      .id = data[0]->GetInt(),
      .tableId = data[1]->GetInt(),
      .name = data[2]->GetString(),
      .isClustered = data[3]->GetBool(),
      .isDisabled = data[4]->GetBool(),
      .additionalInfo{
        .createdAt = data[5]->GetDateTime(),
        .lastModified = data[6]->GetDateTime(),
        .lastModifiedBy = data[7]->GetString(),
        .version = data[8]->GetInt(),
        .isDeleted = data[9]->GetBool(),
        .deletedAt = data[10]->GetBlockData() == nullptr
              ? DataTypes::DateTime()
              : data[10]->GetDateTime()
      },
      .columns = std::move(indexColumns),
      };
  }

  vector<Headers::IndexColumnsHeader> ServerInstance::SelectIndexColumnsByIndexId(const int32_t & indexId) const{
    using namespace DatabaseEngine::StorageTypes;

    Table* sysIndexes = this->masterDb->OpenTable(MasterDbTables::SYSINDEXCOLUMNS);
    vector<Row> rows;

    Indexing::Key key;
    key.InsertKey(Indexing::Key(&indexId, sizeof(indexId), ColumnType::Int));

    sysIndexes->ClusteredIndexSeek(&rows, &key, &key);

    if(rows.empty())
      return {};

    vector<Headers::IndexColumnsHeader> indexColumns;

    for (const auto& indexColumn : rows) {
      const auto& data = indexColumn.GetData();

      indexColumns.emplace_back(
        Headers::IndexColumnsHeader{
          .indexId = data[0]->GetInt(),
          .columnId = data[1]->GetInt(),
          .ordinalPosition = data[2]->GetSmallInt(),
          .isIncluded = data[3]->GetBool(),
          .additionalInfo{
            .version = data[4]->GetInt(),
            .isDeleted = data[5]->GetBool(),
            .deletedAt = data[6]->GetBlockData() == nullptr
                  ? DataTypes::DateTime()
                  : data[6]->GetDateTime()
          }
        });
    }

    //get them sorted by ordinal position
    ranges::sort(indexColumns,
    [](const Headers::IndexColumnsHeader& a, const Headers::IndexColumnsHeader& b) {
      return a.ordinalPosition > b.ordinalPosition;
    });

    return indexColumns;
  }

  Dictionary<int32_t, Headers::IndexColumnsHeader> ServerInstance::SelectIndexColumnsByIndexIdToDictionary(const int32_t &indexId) const{
    const auto indexColumns = this->SelectIndexColumnsByIndexId(indexId);

    Dictionary<int32_t, Headers::IndexColumnsHeader> indexColumnsDict;

    for (const auto& indexColumn : indexColumns)
      indexColumnsDict.Add(indexColumn.columnId, indexColumn);

    return indexColumnsDict;
  }

  vector<Headers::IdentityColumnsHeader> ServerInstance::SelectIdentityColumnsByTableId(const int32_t & tableId) const{
      using namespace DatabaseEngine::StorageTypes;

      Table* table = this->masterDb->OpenTable(MasterDbTables::SYSIDENTITYCOLUMNS);
      vector<Row> rows;

      Indexing::Key key;
      key.InsertKey(Indexing::Key(&tableId, sizeof(tableId), ColumnType::Int));

      table->ClusteredIndexSeek(&rows, &key, &key);

      if(rows.empty())
        return {};

      vector<Headers::IdentityColumnsHeader> columns;

      for (const auto& row : rows) {
        const auto& data = row.GetData();

        columns.emplace_back(
          Headers::IdentityColumnsHeader{
            .tableId = data[0]->GetInt(),
            .columnId = data[1]->GetInt(),
            .seedValue = data[2]->GetInt(),
            .increment = data[3]->GetInt(),
            .lastValue = data[4]->GetInt(),
            .isCached = data[5]->GetBool(),
            .cacheBlock = data[6]->GetInt(),
            .additionalInfo{
              .version = data[7]->GetInt(),
              .isDeleted = data[8]->GetBool(),
              .deletedAt = data[9]->GetBlockData() == nullptr
                    ? DataTypes::DateTime()
                    : data[9]->GetDateTime()
            }
          });
      }

      //get them sorted by ordinal position
      ranges::sort(columns,
      [](const Headers::IdentityColumnsHeader& a, const Headers::IdentityColumnsHeader& b) {
        return a.columnId > b.columnId;
      });

      return columns;
  }

  void ServerInstance::UpdateIdentityByColumnId(const int32_t & tableId, const int32_t& columnId, const int32_t& lastValue)const{
    using namespace DatabaseEngine::StorageTypes;

    Table* table = this->masterDb->OpenTable(MasterDbTables::SYSIDENTITYCOLUMNS);

    const vector<Field> updates{
      Field(lastValue, 4)
    };

    auto *leftExpr =
            new Expressions::Expression{
              .type = Expressions::ExpressionType::Predicate,
              .left = nullptr,
              .right = nullptr,
              .operation = Expressions::ExpressionOperator::Equal,
              .value = Field(tableId, 0),
              .columnIndex = 0
          };

    auto *rightExpr =
              new Expressions::Expression{
                .type = Expressions::ExpressionType::Predicate,
                .left = nullptr,
                .right = nullptr,
                .operation = Expressions::ExpressionOperator::Equal,
                .value = Field(columnId, 1),
                .columnIndex = 1
            };

    auto expr = Expressions::Expression::Logical(Expressions::ExpressionType::And, leftExpr, rightExpr);

    table->ClusteredIndexScanUpdate(&expr, updates);
  }

  void ServerInstance::UpdateColumnById(const int32_t &columnId, const std::vector<Field> &updates) const{
    using namespace DatabaseEngine::StorageTypes;

    Table* table = this->masterDb->OpenTable(MasterDbTables::SYSCOLUMNS);
    vector<Row> rows;

    Indexing::Key key;
    key.InsertKey(Indexing::Key(&columnId, sizeof(columnId), ColumnType::Int));

    table->ClusteredIndexSeekUpdate(nullptr, &key, &key, updates);
  }

  void ServerInstance::CreateSystemDatabase(){
    using namespace DatabaseEngine;
    using namespace DatabaseEngine::StorageTypes;

    CreateDatabase(this->sysDbName);

    this->masterDb = new DatabaseEngine::Database(this->sysDbName, true);

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

        for (const auto& key: table.primaryKey) {
          if (column.name != key)
            continue;

          primaryKey.push_back(j);
        }

        columns.push_back(new Column(column.name, columnType, columnSize, j, column.nullable));
      }

      if (primaryKey.empty())
        throw runtime_error("All tables in masterDb must have a primary key");

      if (primaryKey.size() == 1) {
        auto* columnPtr = columns.at(primaryKey[0]);

        columnPtr->SetIdentity(Headers::IdentityColumnsHeader(-1, columnPtr->GetColumnIndex(), 1, 1, 1, true, 10000));
      }

      Headers::Index index(primaryKey);
      this->masterDb->CreateTable(table.id, i, columns, &index);
    }
  }

  bool ServerInstance::CheckIfMasterDbExists() const{ return std::filesystem::exists(this->sysDbPath); }

  Dictionary<int32_t , Headers::IdentityColumnsHeader> ServerInstance::SelectIdentityColumnsByTableIdToDictionary(const int32_t & tableId) const{
    const auto columns = this->SelectIdentityColumnsByTableId(tableId);

    Dictionary<int32_t, Headers::IdentityColumnsHeader> dict;

    for(const auto& column : columns)
        dict.Add(column.columnId, column);

    return dict;
  }
}