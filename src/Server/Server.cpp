#include "Server.h"
#include "Server.Constants.h"

#include "MasterDbColumns.h"
#include "../Systemic/Converter/Converter.h"

#include <fstream>
#include <nlohmann/json.hpp>
#include "../Database/Block/Block.h"
#include "../Systemic/Functions/StringFunctions.h"
#include "../Database/AdditionalFunctions/SortingFunctions.h"
#include "../Database/Logger/WriteAheadLogger/WriteAheadLogger.h"
#include "../Database/TransactionManager/TransactionManager.h"

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
    if (j.contains("hasIdentity"))
      j.at("hasIdentity").get_to(c.hasIdentity);
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
     this->versionDb = nullptr;

     this->baseProperties.batchSize = Constants::DEFAULT_BATCH_SIZE;
  }

  ServerInstance::~ServerInstance() = default;

  void ServerInstance::ReadConfiguration(const std::string &configPath){
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

    this->versionDbName = jsonFile.at("version_db_name");
    this->versionDbPath = jsonFile.at("version_db_path");

    jsonFile.at("tables").get_to(this->sysTables);
  }

  bool ServerInstance::CreateMasterDatabase(){
    using namespace DatabaseEngine;
    using namespace DatabaseEngine::StorageTypes;

    if (this->MasterDbExists()) {
      this->UseMasterDb();
      return true;
    }

    CreateDatabase(this->sysDbName);

    this->masterDb = new DatabaseEngine::Database(this->sysDbName, true);

    for (int i = 0;i < this->sysTables.size(); i++) {
      const auto& tableHeader = this->sysTables[i];

      std::vector<Column *> columns;
      std::vector<column_index_t> primaryKeyIndexes;

      for (int columnIndex = 0;columnIndex < tableHeader.columns.size(); columnIndex++) {
        const auto& columnHeader = tableHeader.columns[columnIndex];

        block_size_t columnSize = 0;

        const auto normalizedColumnType = Functions::String::NormalizeString(columnHeader.type);

        if (!ColumnTypeSizes.TryGetValue(normalizedColumnType, columnSize))
          throw runtime_error("Column type " + columnHeader.type + " does not exist");

        if (columnSize == 0)
          columnSize = columnHeader.size;

        const auto columnType = ColumnTypesDictionary.Get(normalizedColumnType);

        for (const auto& key: tableHeader.primaryKey) {
          if (columnHeader.name != key)
            continue;

          primaryKeyIndexes.push_back(columnIndex);
        }

        auto* columnPtr = new Column(columnHeader.name, columnType, columnSize, columnIndex, columnHeader.nullable);

        if (columnHeader.hasIdentity)
          columnPtr->SetIdentity(Headers::IdentityColumnsHeader(tableHeader.id, columnIndex, 1, 1, 1, true, 10000));

        columns.push_back(columnPtr);
      }

      if (primaryKeyIndexes.empty())
        throw runtime_error("All tables in masterDb must have a primary key");

      Headers::Index index(primaryKeyIndexes);
      this->masterDb->CreateTable(tableHeader.id, i, columns, &index);
    }

    return false;
  }

  void ServerInstance::CreateVersionDatabase() {
    if (this->VersionDbExists()) {
      this->versionDb = new DatabaseEngine::VersionDatabase(this->versionDbName);
      return;
    }

    DatabaseEngine::CreateDatabase(this->versionDbName);
    this->versionDb = new DatabaseEngine::VersionDatabase(this->versionDbName);
  }

  bool ServerInstance::MasterDbExists() const{ return std::filesystem::exists(this->sysDbPath); }

  bool ServerInstance::VersionDbExists() const{ return std::filesystem::exists(this->versionDbPath); }

  std::vector<Security::Role> ServerInstance::SelectRoles() const{
    using namespace DatabaseEngine::StorageTypes;
    std::vector<const Row*> rows;

    std::vector<Security::Role> roles;

    Table* table = this->masterDb->OpenTable(MasterDbTables::SysRoles);

    table->ClusteredIndexScan(this->baseProperties, &rows);

    for (const auto& row : rows) {
      const auto& data = row->GetData();

      roles.emplace_back(
        data[0]->GetInt(),
        data[1]->GetString(),
        static_cast<Security::Permission>(data[2]->GetInt()),
        data[3]->GetBool()
      );
    }

    return roles;
  }

  std::vector<Security::User> ServerInstance::SelectUsers() const{
    using namespace DatabaseEngine::StorageTypes;
    std::vector<const Row*> rows;

    std::vector<Security::User> users;

    Table* table = this->masterDb->OpenTable(MasterDbTables::SysUsers);

    table->ClusteredIndexScan(this->baseProperties, &rows);

    for (const auto& row : rows) {
      const auto& data = row->GetData();
      users.emplace_back(
        Security::User{
          .id = data[0]->GetInt(),
          .name = data[1]->GetString(),
          .passwordHash = data[2]->GetString(),
          .roleId =  data[3]->GetInt(),
          .isActive = data[4]->GetBool(),
        }
      );
    }

    return users;
  }

  void ServerInstance::InsertSystemRoles(const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties){
    const auto admin = std::string(ServerConstants::ADMIN_NAME);
    const auto dbOwner = std::string(ServerConstants::DB_OWNER_NAME);
    const auto dbWriter = std::string(ServerConstants::DB_WRITER_NAME);
    const auto dbReader = std::string(ServerConstants::DB_READER_NAME);
    const auto guest = std::string(ServerConstants::GUEST_NAME);

    auto result = this->InsertRoleToMasterDb(
      properties,
      admin,
      ServerConstants::ADMIN_PERMISSIONS
    );

    auto _ = this->roleManager.AddRole(admin,
        new Security::Role(
        result.primaryKey.AsInt(),
        admin,
        ServerConstants::ADMIN_PERMISSIONS,
        true
      ));

    result = this->InsertRoleToMasterDb(
      properties,
      dbOwner,
      ServerConstants::DB_OWNER_PERMISSIONS
    );

   _ = this->roleManager.AddRole(dbOwner,
        new Security::Role(
        result.primaryKey.AsInt(),
        dbOwner,
        ServerConstants::DB_OWNER_PERMISSIONS,
        true
      ));

    result = this->InsertRoleToMasterDb(
      properties,
      dbWriter,
      ServerConstants::DB_WRITER_PERMISSIONS
    );

    _ = this->roleManager.AddRole(dbWriter,
        new Security::Role(
          result.primaryKey.AsInt(),
          dbWriter,
          ServerConstants::DB_WRITER_PERMISSIONS,
          true
        )
    );

    result = this->InsertRoleToMasterDb(
      properties,
      dbReader,
      ServerConstants::DB_READER_PERMISSIONS
    );

    _ = this->roleManager.AddRole(dbReader,
       new Security::Role(
       result.primaryKey.AsInt(),
        dbReader,
        ServerConstants::DB_READER_PERMISSIONS,
       true
     ));

    result = this->InsertRoleToMasterDb(
      properties,
      guest,
      ServerConstants::GUEST_PERMISSIONS
    );

    _ = this->roleManager.AddRole(guest,
       new Security::Role(
        result.primaryKey.AsInt(),
        guest,
        ServerConstants::GUEST_PERMISSIONS,
        true
     ));
  }

  void ServerInstance::InsertSystemUsers(const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties){
    const auto admin = std::string(ServerConstants::ADMIN_NAME);

    const auto* role = this->roleManager.GetRole(admin);

    std::string hashedPassword;
    if (Security::UserManager::HashPassword(admin, hashedPassword) == false) {
      std::cerr << "Failed to hash password for admin user" << std::endl;
      return;
    }

    const auto result =
      this->InsertUserToMasterDb(
          properties,
        admin,
        hashedPassword,
        role->id,
        true
      );

    const auto _ =
      this->userManager.AddUser(result.primaryKey.AsInt(), admin, hashedPassword, role);
  }

  ServerInstance & ServerInstance::Get(){
    static ServerInstance instance;

    return instance;
  }

  void ServerInstance::Initialize(const string &configPath){
    this->ReadConfiguration(configPath);

    this->CreateVersionDatabase();
    if (this->CreateMasterDatabase())
      return;

    const auto dbInsertResult = this->InsertDbToMasterDb(
      this->baseProperties,
      this->sysDbName,
      this->sysDbPath,
      true
    );

    const auto schemaInsertResult = this->InsertSchemaToMasterDb(
      this->baseProperties,
      dbInsertResult.primaryKey.AsInt(),
      "dbo"
    );

    Dictionary<string, column_index_t> columnNameToIndex;

    int counter=  0;
    for (int i = 0;i < this->sysTables.size(); i++) {
      const auto& table = this->sysTables[i];

      const auto tableResult =
        this->InsertTableToMasterDb(
            this->baseProperties,
          dbInsertResult.primaryKey.AsInt(),
          schemaInsertResult.primaryKey.AsInt(),
          table.name,
          static_cast<int16_t>(i),
          true
        );

      // const auto tableStatsResult =
      //   this->InsertTableStatisticsToMasterDb(
      //     this->baseProperties,
      //     tableResult.primaryKey.AsInt()
      //   );

      int columnPos = 0;

      Dictionary<std::string, int32_t> columnIdsDict;

      for (auto& column: table.columns) {
        counter++;
        const auto normalizedColumnType = Functions::String::NormalizeString(column.type);

        auto columnSize = ColumnTypeSizes.Get(normalizedColumnType);

        if (columnSize == 0)
          columnSize = column.size;

        const auto& type = ColumnTypesDictionary.Get(normalizedColumnType);

        if (counter == 15) {
          int val = 0;
        }

        const auto columnResult =
          this->InsertColumnToMasterDb(
              this->baseProperties,
             tableResult.primaryKey.AsInt(),
             column.name,
             type,
             columnSize,
             Constants::INVALID_DECIMAL_PRECISION,
             Constants::INVALID_DECIMAL_SCALE,
             column.nullable,
             columnPos,
             true
          );

        if (columnResult.code != Errors::RuntimeError::Ok)
          std::cerr << columnResult.message << std::endl;

        if (column.hasIdentity)
          const auto _ = this->InsertIdentityColumnToMasterDb(
                this->baseProperties,
                columnResult.primaryKey.AsInt(0),
                columnResult.primaryKey.AsInt(1),
                1,
                1,
                1,
                true,
                1000
              );

        // const auto columnStatsResult =
        //   this->InsertColumnStatisticsToMasterDb(
        //     this->baseProperties,
        //     columnResult.primaryKey.AsInt()
        //   );

        columnNameToIndex.Add(column.name, columnPos);
        columnIdsDict.Add(column.name,columnResult.primaryKey.AsInt());

        columnPos++;
      }

      std::string concatenatedColumns;
      std::string _columns;

      for (int j = 0; j < table.primaryKey.size(); j++) {
        const auto& key = columnNameToIndex.Get(table.primaryKey[j]);

        concatenatedColumns +=  j > 0  ? "," + to_string(key) : to_string(key);
        _columns +="_" + table.primaryKey[j];
      }

      //TODO keep the last value keys
      const auto indexResult =
        this->InsertIndexToMasterDb(
          this->baseProperties,
          tableResult.primaryKey.AsInt(),
          "PK" + _columns,
          true
      );

      auto indexKey = indexResult.primaryKey.AsInt();

      const auto constraintResult =
        this->InsertConstraintToMasterDb(
          this->baseProperties,
          tableResult.primaryKey.AsInt(),
          "PK" + _columns,
          Headers::ConstraintType::PrimaryKey,
          false,
          &indexKey
      );

      for(int j = 0;j < table.primaryKey.size(); j++){
        // const auto& columnIndex = columnNameToIndex.Get(table.primaryKey[j]);

        auto _ = this->InsertIndexColumnToMasterDb(
            this->baseProperties,
            indexResult.primaryKey.AsInt(),
            columnIdsDict.Get(table.primaryKey[j]),
            static_cast<int16_t>(j),
            true
        );

        _ = this->InsertConstraintColumnToMasterDb(
          this->baseProperties,
          constraintResult.primaryKey.AsInt(),
          columnIdsDict.Get(table.primaryKey[j]),
          static_cast<int16_t>(j)
        );
      }
    }

    this->masterDb->GetColumnsHeaders();
    this->masterDb->UpdateIdentityManagersIds();

    this->InsertSystemRoles(this->baseProperties);
    this->InsertSystemUsers(this->baseProperties);

    std::cout << this->sysDbName << " initialized successfully" << std::endl;
  }

  Errors::RuntimeStatus ServerInstance::GrantRole(
    const DataTypes::Guid& currentSessionId,
    const std::string &username,
    const Security::Role *role
  )const{
    int32_t userId = -1;

    if (!this->userManager.GrantRole(username, role, userId))
      return {
        Errors::RuntimeError::Error,
        "Failed to grant role: " + role->name + " to user: " + username,
      };

    return this->UpdateUserById(currentSessionId, userId, role->id);
  }

  bool ServerInstance::UserExists(const std::string &userName) const{
    return this->userManager.GetUser(userName) != nullptr;
  }

  bool ServerInstance::CreateUser(const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties, const std::string &userName, const std::string &password, const std::string& roleName){
    if (this->userManager.GetUser(userName) != nullptr)
      return false;

    const auto* role = this->roleManager.GetRole(roleName);

    if (role == nullptr)
      return false;

    std::string hashedPassword;
    if (Security::UserManager::HashPassword(password, hashedPassword) == false) {
      std::cerr << "Failed to hash password for user" << userName << std::endl;
      return false;
    }

    const auto result =
      this->InsertUserToMasterDb(
        properties,
        userName,
        hashedPassword,
        role->id,
        true
      );

    if (result.code != Errors::RuntimeError::Ok) {
      std::cerr << "Failed to create user"
                << userName
                << " with error: "
                << result.message << std::endl;

      return false;
    }

    return this->userManager.AddUser(result.primaryKey.AsInt(), userName, hashedPassword, role);
  }

  const Security::User * ServerInstance::Authenticate(const std::string &username, const std::string &password)const{
    return this->userManager.Authenticate(username, password);
  }

  bool ServerInstance::RoleExists(const std::string &role) const{
    return this->roleManager.GetRole(role) != nullptr;
  }

  const Security::Role * ServerInstance::GetRole(const std::string &roleName)const {
    return this->roleManager.GetRole(roleName);
  }

  const Network::Session * ServerInstance::CreateSession(const Security::User* user){
    return this->sessionManager.CreateSession(user);
  }

  const Network::Session * ServerInstance::GetSession(const DataTypes::Guid &key)const{
    return this->sessionManager.GetSession(key);
  }

  bool ServerInstance::CloseSession(const DataTypes::Guid &key) {
    return this->sessionManager.CloseSession(key);
  }


  bool ServerInstance::UpdateSession(const DataTypes::Guid &key, const int32_t &databaseId)const{
    return this->sessionManager.UpdateSession(key, databaseId);
  }

  bool ServerInstance::AddOrSetVariable(const DataTypes::Guid &sessionId, const Variable& variable) const {
    return this->sessionManager.AddOrSetVariable(sessionId, variable);
  }

  QueryPipeline::Cursor * ServerInstance::CreateCursor(
    const DataTypes::Guid &id,
    const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
    QueryPipeline::PhysicalPlan::PhysicalOperator *physicalPlan
  ) const {
    return this->sessionManager.CreateCursor(id, properties, physicalPlan);
  }

  bool ServerInstance::CloseCursor(const DataTypes::Guid &id, const QueryPipeline::PipelineConstants::cursor_id_t& cursorId) const {
    return this->sessionManager.CloseCursor(id, cursorId);
  }

  Errors::RuntimeStatus ServerInstance::InsertDbToMasterDb(
    const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
    const string& dbName,
    const string& dbPath,
    const bool& isSystem,
    const string& user,
    const int& version,
    const bool& isDeleted
  ) const{
      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SysDatabases);

      const auto currentDate = DataTypes::DateTime::Now();

      const vector<Value> fields = {
        Value(dbName, 1),
        Value(dbPath, 2),
        Value(isSystem, 3),
        Value(currentDate, 4),
        Value(currentDate, 5),
        Value(user, 6),
        Value(version, 7),
        Value(isDeleted, 8),
        Value(nullptr, 9),
    };

    // const auto transactionId = this->masterDb->StartLogTransaction();

    const auto result = table->InsertRow(properties, fields);

    cout << "Inserted database: "<< dbName << " to master db" << endl;

    return result;
  }

  Errors::RuntimeStatus  ServerInstance::InsertSchemaToMasterDb(
    const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
    const int32_t &databaseId,
    const string &schemaName,
    const string &user,
    const int& version,
    const bool& isDeleted) const{
     DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SysSchemas);
     const auto currentDate = DataTypes::DateTime::Now();

     const vector<Value> fields = {
        Value(databaseId, 1),
        Value(schemaName, 2),
        Value(currentDate, 3),
        Value(currentDate, 4),
        Value(user, 5),
        Value(version, 6),
        Value(isDeleted, 7),
        Value(nullptr, 8),
     };

    // const auto transactionId = this->masterDb->StartLogTransaction();

    const auto result = table->InsertRow(properties, fields);

    cout << "Inserted schema: "<< schemaName << " to master db" << endl;

    return result;
  }

  Errors::RuntimeStatus  ServerInstance::InsertTableToMasterDb(
    const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
    const int32_t & databaseId,
    const int32_t & schemaId,
    const string& tableName,
    const int16_t& ordinalPosition,
    const bool& isSystem,
    const string& user,
    const int& version,
    const bool& isDeleted
  ) const{

      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SysTables);
      const auto currentDate = DataTypes::DateTime::Now();

      const vector<Value> fields = {
        Value(databaseId, 1),
        Value(schemaId, 2),
        Value(tableName, 3),
        Value(ordinalPosition, 4),
        Value(isSystem, 5),
        Value(currentDate, 6),
        Value(currentDate, 7),
        Value(user, 8),
        Value(version, 9),
        Value(isDeleted, 10),
        Value(nullptr, 11),
      };

    // const auto transactionId = this->masterDb->StartLogTransaction();

      const auto result = table->InsertRow(properties, fields);

      cout << "Inserted table: "<< tableName << " to master db" << endl;

      return result;
  }

  Errors::RuntimeStatus ServerInstance::InsertColumnToMasterDb(
    const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
    const int32_t & tableId,
    const string &columnName,
    const DataType &columnType,
    const int &columnSize,
    const int8_t& precision,
    const int8_t& scale,
    const bool& isNullable,
    const int &ordinalPosition,
    const bool& isSystem,
    const string& user,
    const int& version,
    const bool& isDeleted
  ) const{
      auto* table = this->masterDb->OpenTable(MasterDbTables::SysColumns);

      const auto currentDate = DataTypes::DateTime::Now();

      std::vector<Value> fields = {
        Value(tableId, static_cast<column_index_t>(SysColumns::TableId)),
        Value(columnName, static_cast<column_index_t>(SysColumns::Name)),
        Value(static_cast<int8_t>(columnType), static_cast<column_index_t>(SysColumns::DataType)),
        Value(columnSize, static_cast<column_index_t>(SysColumns::RecordSize)),
        Value(nullptr, static_cast<column_index_t>(SysColumns::Precision)),
        Value(nullptr, static_cast<column_index_t>(SysColumns::Scale)),
        Value(isNullable, static_cast<column_index_t>(SysColumns::IsNullable)),
        Value(ordinalPosition, static_cast<column_index_t>(SysColumns::OrdinalPosition)),
        Value(isSystem, static_cast<column_index_t>(SysColumns::IsSystemColumn)),
        Value(currentDate, static_cast<column_index_t>(SysColumns::CreatedAt)),
        Value(currentDate, static_cast<column_index_t>(SysColumns::LastModifiedAt)),
        Value(user, static_cast<column_index_t>(SysColumns::LastModifiedBy)),
        Value(version, static_cast<column_index_t>(SysColumns::Version)),
        Value(isDeleted, static_cast<column_index_t>(SysColumns::IsDeleted)),
        Value(nullptr, static_cast<column_index_t>(SysColumns::DeletedAt)),
      };

      if (precision != Constants::INVALID_DECIMAL_PRECISION) {
        fields[4] = Value(precision, 5);
        fields[5] = Value(scale, 6);
      }

      const auto result = table->InsertRow(properties, fields);

      cout << "Inserted column: "<< columnName << " to master db" << endl;

      return result;
  }

  Errors::RuntimeStatus  ServerInstance::InsertIndexToMasterDb(
    const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
    const int32_t & tableId,
    const string &indexName,
    const bool &isClustered,
    const bool &isDisabled,
    const string &user,
    const int& version,
    const bool& isDeleted) const{
     DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SysIndexes);
     const auto currentDate = DataTypes::DateTime::Now();

     const vector<Value> fields = {
       Value(tableId, 1),
       Value(indexName, 2),
       Value(isClustered, 3),
       Value(isDisabled, 4),
       Value(currentDate, 5),
       Value(currentDate, 6),
       Value(user, 7),
       Value(version, 8),
       Value(isDeleted, 9),
      Value(nullptr, 10),
     };

    // const auto transactionId = this->masterDb->StartLogTransaction();

      const auto result = table->InsertRow(properties, fields);

      cout << "Inserted index: "<< indexName << " to master db" << endl;

      return result;
  }

  Errors::RuntimeStatus ServerInstance::InsertIndexColumnToMasterDb(
    const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
    const int32_t & indexId,
    const int32_t & columnId,
    const int16_t & ordinalPosition,
    const bool & isIncluded,
    const int& version,
    const bool& isDeleted) const{
    DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SysIndexColumns);
    const auto currentDate = DataTypes::DateTime::Now();

    const vector<Value> fields = {
      Value(indexId, 0),
      Value(columnId, 1),
      Value(ordinalPosition, 2),
      Value(isIncluded, 3),
      Value(version, 4),
      Value(isDeleted, 5),
      Value(nullptr, 6),
    };

    // const auto transactionId = this->masterDb->StartLogTransaction();

    const auto result = table->InsertRow(properties, fields);

    cout << "Inserted index column to master db" << endl;

    return result;
  }

  Errors::RuntimeStatus ServerInstance::InsertConstraintToMasterDb(
      const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
      const int32_t & tableId,
      const string & constraintName,
      const Headers::ConstraintType & constraintType,
      const bool & isDisabled,
      const int32_t *constraintIndexId,
      const string & user,
      const int& version,
      const bool& isDeleted
  ) const{

      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SysConstraints);
      const auto currentDate = DataTypes::DateTime::Now();

      vector<Value> fields = {
          Value(tableId, 1),
          Value(constraintName, 2),
          Value(static_cast<int8_t>(constraintType), 3),
          Value(isDisabled, 4),
          Value(nullptr, 5),
          Value(currentDate, 6),
          Value(currentDate, 7),
          Value(user, 8),
          Value(version, 9),
          Value(isDeleted, 10),
          Value(nullptr, 11),
      };

      if(constraintIndexId != nullptr)
          fields.at(4).SetData(*constraintIndexId);

    // const auto transactionId = this->masterDb->StartLogTransaction();

    const auto result = table->InsertRow(properties, fields);

    cout << "Inserted constraint: "<< constraintName <<" to master db" << endl;

    return result;
  }

  Errors::RuntimeStatus ServerInstance::InsertConstraintColumnToMasterDb(
    const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
    const int32_t & constraintId,
    const int32_t & columnId,
    const int32_t & ordinalPosition,
    const int& version,
    const bool& isDeleted) const{

    DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SysConstraintColumns);
    const auto currentDate = DataTypes::DateTime::Now();

    const vector<Value> fields = {
        Value(constraintId, 0),
        Value(columnId, 1),
        Value(ordinalPosition, 2),
        Value(version, 3),
        Value(isDeleted, 4),
        Value(nullptr, 5),
    };

    // const auto transactionId = this->masterDb->StartLogTransaction();

    const auto result = table->InsertRow(properties, fields);

    cout << "Inserted constraint column to master db" << endl;

    return result;
  }

  Errors::RuntimeStatus ServerInstance::InsertIdentityColumnToMasterDb(
      const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
      const int32_t & tableId,
      const int32_t & columnId,
      const int32_t & seedValue,
      const int32_t & increment,
      const int32_t & lastValue,
      const bool & isCached,
      const int32_t & cacheBlock,
      const int& version,
      const bool& isDeleted
  ) const{

      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SysIdentityColumns);
      const auto currentDate = DataTypes::DateTime::Now();

      const vector<Value> fields = {
        Value(tableId, 0),
        Value(columnId, 1),
        Value(seedValue, 2),
        Value(increment, 3),
        Value(lastValue, 4),
        Value(isCached, 5),
        Value(cacheBlock, 6),
        Value(version, 7),
        Value(isDeleted, 8),
        Value(nullptr, 9),
      };

    // const auto transactionId = this->masterDb->StartLogTransaction();

    const auto result = table->InsertRow(properties, fields);

    cout << "Inserted identity column to master db" << endl;

    return result;
  }

  Errors::RuntimeStatus ServerInstance::InsertDefaultValuesToMasterDb(
    const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
    const int32_t &columnId,
    const Value &value,
    const int &version,
    const bool &isDeleted) const{

      DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SysDefaultValues);
      const auto currentDate = DataTypes::DateTime::Now();

      const vector<Value> fields = {
        Value(columnId, 0),
        Value(std::string(reinterpret_cast<const char*>(value.GetRawData()), value.GetSize()), 1),
        Value(version, 2),
        Value(isDeleted, 3),
        Value(nullptr, 4),
      };

    // const auto transactionId = this->masterDb->StartLogTransaction();

      const auto result = table->InsertRow(properties, fields);

      std::cout << "Inserted default value " << value << " to master db" << std::endl;

      return result;
  }

  Errors::RuntimeStatus ServerInstance::InsertTableStatisticsToMasterDb(
    const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
    const int32_t &tableId,
    const int64_t& rowCount,
    const int32_t& rowSize,
    const int &version,
    const bool &isDeleted
  ) const{

    DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SysTableStats);
    const auto currentDate = DataTypes::DateTime::Now();

    const std::string lastModifiedBy = "system";

    const vector<Value> fields = {
      Value(tableId, static_cast<column_index_t>(SysTableStats::TableId)),
      Value(rowCount, static_cast<column_index_t>(SysTableStats::RowCount)),
      Value(rowSize, static_cast<column_index_t>(SysTableStats::AvgRowSize))
    };

    const auto result = table->InsertRow(properties, fields);

    std::cout << "Inserted table stats for table with id: " << tableId << std::endl;

    return result;
  }

  Errors::RuntimeStatus ServerInstance::InsertColumnStatisticsToMasterDb(
    const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
    const int32_t &columnId,
    const int64_t &distinctCount,
    const int64_t &nullCount
  ) const{

    DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SysColumnStats);
    const auto currentDate = DataTypes::DateTime::Now();

    const std::string lastModifiedBy = "system";

    const std::vector<Value> fields = {
      Value(columnId, static_cast<column_index_t>(SysColumnStats::ColumnId)),
      Value(distinctCount, static_cast<column_index_t>(SysColumnStats::DistinctCount)),
      Value(nullptr, static_cast<column_index_t>(SysColumnStats::MinimumValue)),
      Value(nullptr, static_cast<column_index_t>(SysColumnStats::MaximumValue)),
      Value(nullCount, static_cast<column_index_t>(SysColumnStats::NullCount))
    };

    const auto result = table->InsertRow(properties, fields);

    std::cout << "Inserted column stats for column with id: " << columnId << std::endl;

    return result;
  }

  Errors::RuntimeStatus ServerInstance::InsertColumnHistogramsToMasterDb(
    const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties &properties,
    const int32_t &columnId,
    const Value &min,
    const Value &max,
    const int64_t &distinctCount
  ) const {

    const std::vector<Value> fields = {
      Value(columnId, static_cast<column_index_t>(SysColumnHistograms::ColumnId)),
      Value(std::string(reinterpret_cast<const char*>(min.GetRawData()), min.GetSize()), static_cast<column_index_t>(SysColumnHistograms::RangeStart)),
      Value(std::string(reinterpret_cast<const char*>(max.GetRawData()), max.GetSize()), static_cast<column_index_t>(SysColumnHistograms::RangeEnd)),
      Value(0, static_cast<column_index_t>(SysColumnHistograms::RowCount)),
      Value(distinctCount, static_cast<column_index_t>(SysColumnHistograms::DistinctCount)),
    };

    auto* table = this->masterDb->OpenTable(MasterDbTables::SysColumnHistograms);

    const auto result = table->InsertRow(properties, fields);

    std::cout << "Inserted histogram Bucket for column: " << columnId << std::endl;

    return result;
  }

  Errors::RuntimeStatus ServerInstance::InsertRoleToMasterDb(
    const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
    const std::string &roleName,
    const Security::Permission &permissions,
    const bool& isSystem,
    const int &version,
    const bool &isDeleted
  ) const{

    auto* table = this->masterDb->OpenTable(MasterDbTables::SysRoles);
    const auto currentDate = DataTypes::DateTime::Now();

    const std::string lastModifiedBy = "system";

    const vector<Value> fields = {
      Value(roleName, static_cast<column_index_t>(SysRoles::RoleName)),
      Value(static_cast<int>(permissions), static_cast<column_index_t>(SysRoles::Permissions)),
      Value(isSystem, static_cast<column_index_t>(SysRoles::IsSystemRole)),
      Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(SysRoles::CreatedAt)),
      Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(SysRoles::LastModifiedAt)),
      Value(lastModifiedBy, static_cast<column_index_t>(SysRoles::LastModifiedBy)),
      Value(version, static_cast<column_index_t>(SysRoles::Version)),
      Value(isDeleted, static_cast<column_index_t>(SysRoles::IsDeleted)),
      Value(nullptr, static_cast<column_index_t>(SysRoles::DeletedAt)),
    };

    const auto result = table->InsertRow(properties, fields);

    std::cout << "Inserted Role " << roleName << std::endl;

    return result;
  }

  Errors::RuntimeStatus ServerInstance::InsertUserToMasterDb(
    const QueryPipeline::PhysicalPlan::PhysicalPlanExecutionProperties& properties,
    const std::string &username,
    const std::string &passwordHash,
    const int32_t &roleId,
    const bool& isActive,
    const int &version,
    const bool &isDeleted
  ) const{

    DatabaseEngine::StorageTypes::Table* table = this->masterDb->OpenTable(MasterDbTables::SysUsers);
    const auto currentDate = DataTypes::DateTime::Now();

    const std::string lastModifiedBy = "system";

    const vector<Value> fields = {
      Value(username, static_cast<column_index_t>(SysUsers::UserName)),
      Value(passwordHash, static_cast<column_index_t>(SysUsers::PasswordHash)),
      Value(roleId, static_cast<column_index_t>(SysUsers::RoleId)),
      Value(isActive, static_cast<column_index_t>(SysUsers::IsActive)),
      Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(SysUsers::CreatedAt)),
      Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(SysUsers::LastModifiedAt)),
      Value(lastModifiedBy, static_cast<column_index_t>(SysUsers::LastModifiedBy)),
      Value(version, static_cast<column_index_t>(SysUsers::Version)),
      Value(isDeleted, static_cast<column_index_t>(SysUsers::IsDeleted)),
      Value(nullptr, static_cast<column_index_t>(SysUsers::DeletedAt)),
    };

    // const auto transactionId = this->masterDb->StartLogTransaction();

    const auto result = table->InsertRow(properties, fields);

    std::cout << "Inserted User " << username << std::endl;

    return result;
  }

  Errors::RuntimeStatus ServerInstance::UpdateUserById(
    const DataTypes::Guid& callerSessionId,
    const int32_t &userId,
    const int32_t &roleId
  )const{
    auto* table = this->masterDb->OpenTable(MasterDbTables::SysUsers);

    const auto* currentSession = this->sessionManager.GetSession(callerSessionId);

    if (currentSession == nullptr || currentSession->user == nullptr)
      return{
          Errors::RuntimeError::InvalidSession,
          "Failed to validate session"
      };

    const auto currentDate = DataTypes::DateTime::Now();

    const std::vector<Value> updates = {
      Value(roleId, static_cast<column_index_t>(SysUsers::RoleId)),
      Value(currentDate, static_cast<Constants::column_index_t>(SysUsers::LastModifiedAt)),
      Value(currentSession->user->name, static_cast<Constants::column_index_t>(SysUsers::LastModifiedBy))
    };

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&userId, sizeof(userId), DataType::Int));

    return table->ClusteredIndexSeekUpdate(this->baseProperties, nullptr, &key, &key, updates);
  }

  vector<Headers::DatabaseHeader> ServerInstance::GetCatalog() const{
     using namespace DatabaseEngine::StorageTypes;

     Table* sysDatabases = this->masterDb->OpenTable(MasterDbTables::SysDatabases);

     std::vector<const Row*> selectedDatabases;

     sysDatabases->ClusteredIndexScan(this->baseProperties, &selectedDatabases);

     vector<Headers::DatabaseHeader> databasesHeaders;

     if (selectedDatabases.empty())
       return {};

    for (const auto& row : selectedDatabases) {

      const auto& data = row->GetData();

      const auto databaseId = data[0]->GetInt();

      const auto& dbName = data[1]->GetString();

      auto schemas = this->SelectSchemas(databaseId);

      auto dbTables = this->SelectTables(dbName);

      for (auto& table : dbTables) {
        table.columns = this->SelectColumns(table.id);
        table.statistics = this->SelectTableStatisticsById(table.id);

        const auto identityColumns = this->SelectIdentityColumnsByTableIdToDictionary(table.id);

        for (auto& column : table.columns) {
          Headers::IdentityColumnsHeader identityHeader;
          identityColumns.TryGetValue(column.id, identityHeader);

          column.identity = std::move(identityHeader);
          column.defaultValue = this->SelectDefaultValueByColumnId(column.id);
          column.statistics = this->SelectColumnStatisticsById(column.id, static_cast<DataType>(column.dataType));
        }

        table.constraints = this->SelectConstraints(table.id);
        // table.identity = this->SelectIdentityColumnsByTableId(table.id);
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

  bool ServerInstance::DatabaseExists(const string &dbName) const{
      using namespace DatabaseEngine::StorageTypes;

      Table* sysDatabases = this->masterDb->OpenTable(MasterDbTables::SysDatabases);
      std::vector<const Row*> selectedDatabases;

      DataTypes::Indexing::Key key;
      key.InsertKey(DataTypes::Indexing::Key(dbName.data(), dbName.size(), DataType::String));

      auto* columnOperation = new Expressions::ColumnExpression(1);
      auto* literaValue = new Expressions::ConstantExpression(Value(dbName, 1));

      const Expressions::BinaryExpression binaryExpr(columnOperation, literaValue, Expressions::BinaryOperator::Equal);

      sysDatabases->ClusteredIndexScan(this->baseProperties, &selectedDatabases, &binaryExpr);

      return !selectedDatabases.empty();
  }

  Headers::DatabaseHeader ServerInstance::SelectDatabase(const std::string &name) const{
    using namespace DatabaseEngine::StorageTypes;

    auto* columnOperation = new Expressions::ColumnExpression(1);
    auto* literaValue = new Expressions::ConstantExpression(Value(name, 1));

    const Expressions::BinaryExpression binaryExpr(columnOperation, literaValue, Expressions::BinaryOperator::EqualIgnoreOrdinalCase);

    Table* sysDatabases = this->masterDb->OpenTable(MasterDbTables::SysDatabases);
    std::vector<const Row*> selectedDatabases;

    sysDatabases->ClusteredIndexScan(this->baseProperties, &selectedDatabases, &binaryExpr);

    if (selectedDatabases.empty())
      return {};

    const auto& data = selectedDatabases[0]->GetData();

    return Headers::DatabaseHeader{
      .id = data[0]->GetInt(),
      .name = data[1]->GetString(),
      .filepath = data[2]->GetString(),
      .isSystem = data[3]->GetBool(),
    };
  }

  Headers::DatabaseHeader ServerInstance::SelectDatabaseById(const int32_t & databaseId) const{
    using namespace DatabaseEngine::StorageTypes;

    Table* sysDatabases = this->masterDb->OpenTable(MasterDbTables::SysDatabases);
    std::vector<const Row*> selectedDatabases;

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&databaseId, sizeof(databaseId), DataType::Int));

    sysDatabases->ClusteredIndexSeek(this->baseProperties, &selectedDatabases, key);

    if (selectedDatabases.empty())
      return {};

    const auto& data = selectedDatabases[0]->GetData();

    return Headers::DatabaseHeader{
      .id = data[0]->GetInt(),
      .name = data[1]->GetString(),
      .filepath = data[2]->GetString(),
      .isSystem = data[3]->GetBool(),
    };
  }

  vector<Headers::SchemaHeader> ServerInstance::SelectSchemas(const int32_t& databaseId) const{
     using namespace DatabaseEngine::StorageTypes;

     Table* sysSchemas = this->masterDb->OpenTable(MasterDbTables::SysSchemas);
     std::vector<const Row*> selectedSchemas;

    auto* columnOperation = new Expressions::ColumnExpression(1);
    auto* literaValue = new Expressions::ConstantExpression(Value(databaseId, 1));

    const Expressions::BinaryExpression binaryExpr(columnOperation, literaValue, Expressions::BinaryOperator::Equal);

    sysSchemas->ClusteredIndexScan(this->baseProperties, &selectedSchemas, &binaryExpr);

    if (selectedSchemas.empty())
      return {};

    vector<Headers::SchemaHeader> schemas;

    for (const auto& row : selectedSchemas) {
      const auto& data = row->GetData();

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

  Dictionary<std::string, Headers::SchemaHeader> ServerInstance::SelectSchemasToDictionary(const int32_t &databaseId) const{
    const auto& schemas = this->SelectSchemas(databaseId);

    Dictionary<string, Headers::SchemaHeader> selectedSchemas;

    for (const auto& schema : schemas)
      selectedSchemas.Add(schema.name, schema);

    return selectedSchemas;
  }

  bool ServerInstance::SchemaExists(const int32_t &databaseId, const std::string &schema) const{
    using namespace DatabaseEngine::StorageTypes;

    auto* leftColumnOperation = new Expressions::ColumnExpression(1);
    auto* leftLiteraValue = new Expressions::ConstantExpression(Value(databaseId, 1));

    const auto binaryExpr = Expressions::BinaryExpression(leftColumnOperation, leftLiteraValue, Expressions::BinaryOperator::EqualIgnoreOrdinalCase);

    // auto* rightColumnOperation = new Expressions::ColumnExpression(2);
    // auto* rightLiteraValue = new Expressions::LiteralExpression(Value(schema, 2));
    //
    // auto* rightBinaryExpr = new Expressions::BinaryExpression(rightColumnOperation, rightLiteraValue, Expressions::ExpressionOperator::Equal);
    //
    // const Expressions::LogicalExpression logicalExpr(leftBinaryExpr, rightBinaryExpr, Expressions::ExpressionType::And);

    Table* sysSchemas = this->masterDb->OpenTable(MasterDbTables::SysSchemas);
    std::vector<const Row*> selectedSchemas;

    sysSchemas->ClusteredIndexScan(this->baseProperties, &selectedSchemas, &binaryExpr);

    for (const auto& row : selectedSchemas) {
      const auto& currentSchemaName = row->GetColumnByIndex(2);

      if (Functions::String::Lower(currentSchemaName.GetString())
          == Functions::String::Lower(schema))
        return true;
    }

    return false;
  }

  vector<Headers::TableHeader> ServerInstance::SelectTables(const string &dbName) const{
    using namespace DatabaseEngine::StorageTypes;

    const auto databaseHeader = this->SelectDatabase(dbName);

    auto* columnOperation = new Expressions::ColumnExpression(1);
    auto* literaValue = new Expressions::ConstantExpression(Value(databaseHeader.id, 1));

    const Expressions::BinaryExpression binaryExpr(columnOperation, literaValue, Expressions::BinaryOperator::Equal);

    std::vector<const Row*> selectedTables;

    Table* sysTablesPtr = this->masterDb->OpenTable(MasterDbTables::SysTables);

    sysTablesPtr->ClusteredIndexScan(this->baseProperties, &selectedTables, &binaryExpr);

    if (selectedTables.empty())
      return {};

    vector<Headers::TableHeader> selectedTableHeaders;
    selectedTableHeaders.reserve(selectedTables.size());

    for (const auto& row : selectedTables) {
      const auto& data = row->GetData();

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

    auto* columnOperation = new Expressions::ColumnExpression(1);
    auto* literaValue = new Expressions::ConstantExpression(Value(databaseId, 1));

    const Expressions::BinaryExpression binaryExpr(columnOperation, literaValue, Expressions::BinaryOperator::Equal);

    std::vector<const Row*> selectedTables;

    Table* sysTablesPtr = this->masterDb->OpenTable(MasterDbTables::SysTables);

    sysTablesPtr->ClusteredIndexScan(this->baseProperties, &selectedTables, &binaryExpr);

    if (selectedTables.empty())
      return {};

    vector<Headers::TableHeader> selectedTableHeaders;
    selectedTableHeaders.reserve(selectedTables.size());

    for (const auto& row : selectedTables) {
      const auto& data = row->GetData();

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

    const auto databaseHeader = this->SelectDatabase(dbName);

    return this->SelectTable(databaseHeader.id, tableName, ServerConstants::DEFAULT_SCHEMA_NAME.data());
  }

  Headers::TableHeader ServerInstance::SelectTable(const int32_t &databaseId, const string &tableName, const std::string& schema) const{
    using namespace DatabaseEngine::StorageTypes;

    if (!this->SchemaExists(databaseId, schema) && !schema.empty())
      return {};

    std::vector<const Row*> selectedTables;
    Table* sysTablesPtr = this->masterDb->OpenTable(MasterDbTables::SysTables);

    auto* leftColumnOperation = new Expressions::ColumnExpression(1);
    auto* leftLiteraValue = new Expressions::ConstantExpression(Value(databaseId, 1));

    auto* leftBinaryExpr = new Expressions::BinaryExpression(leftColumnOperation, leftLiteraValue, Expressions::BinaryOperator::Equal);

    auto* rightColumnOperation = new Expressions::ColumnExpression(3);
    auto* rightLiteraValue = new Expressions::ConstantExpression(Value(tableName, 3));

    auto* rightBinaryExpr = new Expressions::BinaryExpression(rightColumnOperation, rightLiteraValue, Expressions::BinaryOperator::EqualIgnoreOrdinalCase);

    const Expressions::LogicalExpression logicalExpr(leftBinaryExpr, rightBinaryExpr, Expressions::LogicalType::And);

    sysTablesPtr->ClusteredIndexScan(this->baseProperties, &selectedTables, &logicalExpr);

    if (selectedTables.empty())
      return {};

    const auto& data = selectedTables[0]->GetData();

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

  vector<Headers::ConstraintsHeader> ServerInstance::SelectConstraints(const int32_t & tableId) const{
    using namespace DatabaseEngine::StorageTypes;

    std::vector<const Row*> selectedConstraints;
    Table* constraintsTable = this->masterDb->OpenTable(MasterDbTables::SysConstraints);

    auto* columnOperation = new Expressions::ColumnExpression(1);
    auto* literaValue = new Expressions::ConstantExpression(Value(tableId, 1));

    const Expressions::BinaryExpression binaryExpr(columnOperation, literaValue, Expressions::BinaryOperator::Equal);

    constraintsTable->ClusteredIndexScan(this->baseProperties, &selectedConstraints, &binaryExpr);

    if (selectedConstraints.empty())
      return {};

    vector<Headers::ConstraintsHeader> selectedConstraintsHeader;
    selectedConstraintsHeader.reserve(selectedConstraints.size());

    for (const auto& row : selectedConstraints) {
      const auto& data = row->GetData();

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

  vector<Headers::ColumnHeader> ServerInstance::SelectColumns(const int32_t& tableId) const{
    using namespace DatabaseEngine::StorageTypes;

    std::vector<const Row*> selectedColumns;
    Table* sysColumns = this->masterDb->OpenTable(MasterDbTables::SysColumns);

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int));

    sysColumns->ClusteredIndexSeek(this->baseProperties, &selectedColumns, key);

    if (selectedColumns.empty())
      return {};

    vector<Headers::ColumnHeader> selectedColumnHeaders;
    selectedColumnHeaders.reserve(selectedColumns.size());

    for (const auto& row : selectedColumns) {
      const auto& data = row->GetData();

      selectedColumnHeaders.emplace_back(
        Headers::ColumnHeader{
          .id = data[1]->GetInt(),
          .tableId = data[0]->GetInt(),
          .name = data[2]->GetString(),
          .dataType = static_cast<uint8_t>(data[3]->GetTinyInt()),
          .recordSize = data[4]->GetInt(),
          .precision = data[5]->GetBlockData() == nullptr
              ? Constants::INVALID_DECIMAL_PRECISION
              : data[5]->GetTinyInt(),
          .scale = data[6]->GetBlockData() == nullptr
              ? Constants::INVALID_DECIMAL_SCALE
              : data[6]->GetTinyInt(),
          .isNullable = data[7]->GetBool(),
          .ordinalPosition = data[8]->GetSmallInt(),
          .isSystem = data[9]->GetBool(),
          .additionalInfo{
            .createdAt = data[10]->GetDateTime(),
            .lastModified = data[11]->GetDateTime(),
            .lastModifiedBy = data[12]->GetString(),
            .version = data[13]->GetInt(),
            .isDeleted = data[14]->GetBool(),
            .deletedAt = data[15]->GetBlockData() == nullptr
                      ? DataTypes::DateTime::Now()
                      : data[15]->GetDateTime(),
            }
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

  Dictionary<string, Headers::ColumnHeader> ServerInstance::SelectColumnsToDictionary(const int32_t& tableId) const{
      const auto columns = this->SelectColumns(tableId);

      Dictionary<string, Headers::ColumnHeader> selectedColumns;

      for (const auto& column : columns)
        selectedColumns.Add(Functions::String::Lower(column.name), column);

      return selectedColumns;
    }

  vector<Headers::IndexHeader> ServerInstance::SelectIndexes(const int32_t& tableId) const{
      using namespace DatabaseEngine::StorageTypes;

      Table* sysIndexes = this->masterDb->OpenTable(MasterDbTables::SysIndexes);
      std::vector<const Row*> selectedIndexes;

      auto* columnOperation = new Expressions::ColumnExpression(1);
      auto* literaValue = new Expressions::ConstantExpression(Value(tableId, 1));

      const Expressions::BinaryExpression binaryExpr(columnOperation, literaValue, Expressions::BinaryOperator::Equal);

      sysIndexes->ClusteredIndexScan(this->baseProperties, &selectedIndexes, &binaryExpr);

      vector<Headers::IndexHeader> selectedIndexHeaders;

      for (const auto& row : selectedIndexes) {
        const auto& data = row->GetData();

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

    Table* sysIndexes = this->masterDb->OpenTable(MasterDbTables::SysIndexes);
    std::vector<const Row*> selectedIndexes;

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&indexId, sizeof(indexId), DataType::Int));

    sysIndexes->ClusteredIndexSeek(this->baseProperties, &selectedIndexes, key);

    if(selectedIndexes.empty())
      return {};

    auto indexColumns = this->SelectIndexColumnsByIndexId(indexId);

    vector<Headers::IndexHeader> selectedIndexHeaders;

    const auto& data = selectedIndexes.at(0)->GetData();

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

    Table* sysIndexes = this->masterDb->OpenTable(MasterDbTables::SysIndexColumns);
    std::vector<const Row*> rows;

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&indexId, sizeof(indexId), DataType::Int));

    sysIndexes->ClusteredIndexSeek(this->baseProperties, &rows, key);

    if(rows.empty())
      return {};

    vector<Headers::IndexColumnsHeader> indexColumns;

    for (const auto& row : rows) {
      const auto& data = row->GetData();

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
      return a.ordinalPosition < b.ordinalPosition;
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

      Table* table = this->masterDb->OpenTable(MasterDbTables::SysIdentityColumns);
      std::vector<const Row*> rows;

      DataTypes::Indexing::Key key;
      key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int));

      table->ClusteredIndexSeek(this->baseProperties, &rows, key);

      if(rows.empty())
        return {};

      vector<Headers::IdentityColumnsHeader> columns;

      for (const auto& row : rows) {
        const auto& data = row->GetData();

        columns.emplace_back(
          Headers::IdentityColumnsHeader{
            .tableId = data[0]->GetInt(),
            .columnId = data[1]->GetInt(),
            .seedValue = data[2]->GetInt(),
            .increment = data[3]->GetInt(),
            .lastValue = data[4]->GetBigInt(),
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

  Dictionary<int32_t , Headers::IdentityColumnsHeader> ServerInstance::SelectIdentityColumnsByTableIdToDictionary(const int32_t & tableId) const{
    const auto columns = this->SelectIdentityColumnsByTableId(tableId);

    Dictionary<int32_t, Headers::IdentityColumnsHeader> dict;

    for(const auto& column : columns)
        dict.Add(column.columnId, column);

    return dict;
  }

  vector<Headers::ConstraintsColumnsHeader> ServerInstance::SelectConstraintColumnsByConstraintId(const int32_t & constraintId) const{
    using namespace DatabaseEngine::StorageTypes;

    Table* sysIndexes = this->masterDb->OpenTable(MasterDbTables::SysConstraintColumns);
    std::vector<const Row*> rows;

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&constraintId, sizeof(constraintId), DataType::Int));

    sysIndexes->ClusteredIndexSeek(this->baseProperties, &rows, key);

    if(rows.empty())
      return {};

    vector<Headers::ConstraintsColumnsHeader> constraintColumns;

    for(const auto& row : rows){
      const auto& data = row->GetData();

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

  Dictionary<int32_t, Headers::ConstraintsColumnsHeader> ServerInstance::SelectConstraintColumnsByConstraintIdToDictionary(const int32_t &constraintId) const{
    const auto columns = this->SelectConstraintColumnsByConstraintId(constraintId);

    Dictionary<int32_t, Headers::ConstraintsColumnsHeader> constraintColumns;

    for (const auto& constraint: columns)
      constraintColumns.Add(constraint.columnId, constraint);

    return constraintColumns;
  }

  Headers::DefaultValuesHeader ServerInstance::SelectDefaultValueByColumnId(const int32_t &columnId) const{
    using namespace DatabaseEngine::StorageTypes;

    Table* sysValues = this->masterDb->OpenTable(MasterDbTables::SysDefaultValues);
    std::vector<const Row*> rows;

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int));

    sysValues->ClusteredIndexSeek(this->baseProperties, &rows, key);

    if(rows.empty())
      return {};

    const auto& data = rows.at(0)->GetData();

    return Headers::DefaultValuesHeader{
      .columnId = data[0]->GetInt(),
      .value = data[1]->GetString(),
      .additionalInfo{
        .version = data[2]->GetInt(),
        .isDeleted = data[3]->GetBool(),
        .deletedAt = data[4]->GetBlockData() == nullptr
              ? DataTypes::DateTime()
              : data[4]->GetDateTime()
      },
    };
  }

  Headers::TableStatistics ServerInstance::SelectTableStatisticsById(const int32_t &tableId) const{
    using namespace DatabaseEngine::StorageTypes;

    Table* sysIndexes = this->masterDb->OpenTable(MasterDbTables::SysTableStats);
    std::vector<const Row*> selectedStats;

    auto* columnOperation = new Expressions::ColumnExpression(static_cast<column_index_t>(SysTableStats::TableId));
    auto* literaValue = new Expressions::ConstantExpression(Value(tableId, static_cast<column_index_t>(SysTableStats::TableId)));

    const Expressions::BinaryExpression binaryExpr(columnOperation, literaValue, Expressions::BinaryOperator::Equal);

    sysIndexes->ClusteredIndexScan(this->baseProperties, &selectedStats, &binaryExpr);

    if (selectedStats.empty())
      return {};

    const auto& data = selectedStats.front()->GetData();

    return Headers::TableStatistics{
      .tableId = data[0]->GetInt(),
      .rowCount = data[1]->GetBigInt(),
      .avgRowSize = data[2]->GetInt()
    };
  }

  Headers::ColumnStatistics ServerInstance::SelectColumnStatisticsById(
    const int32_t& columnId,
    const DataType& columnType
  ) const{
    using namespace DatabaseEngine::StorageTypes;

    Table* sysIndexes = this->masterDb->OpenTable(MasterDbTables::SysColumnStats);
    std::vector<const Row*> selectedStats;

    auto* columnOperation = new Expressions::ColumnExpression(0);
    auto* literaValue = new Expressions::ConstantExpression(Value(columnId, 0));

    const Expressions::BinaryExpression binaryExpr(columnOperation, literaValue, Expressions::BinaryOperator::Equal);

    sysIndexes->ClusteredIndexScan(this->baseProperties, &selectedStats, &binaryExpr);

    if (selectedStats.empty())
      return {};

    const auto& data = selectedStats.front()->GetData();

    return Headers::ColumnStatistics{
        .columnId = data[0]->GetInt(),
        .distinctCount = data[1]->GetBigInt(),
        .min = Value(data[2]->GetBlockData(), data[2]->GetBlockSize(), columnType),
        .max = Value(data[3]->GetBlockData(), data[3]->GetBlockSize(), columnType),
        .nullCount = data[4]->GetBigInt()
      };
  }

  std::vector<Headers::ColumnHistograms> ServerInstance::SelectColumnHistogramsByColumnId(
    const int32_t &columnId,
    const Constants::DataType& columnType
  ) const {

    std::vector<Headers::ColumnHistograms> result;
    result.reserve(Constants::NUMBER_OF_HISTOGRAM_BUCKETS);

    auto* table = this->masterDb->OpenTable(MasterDbTables::SysColumnHistograms);

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int));

    std::vector<const DatabaseEngine::StorageTypes::Row*> rows;
    table->ClusteredIndexSeek(this->baseProperties, &rows, key);

    for (const auto& row : rows) {
      const auto& data = row->GetData();

      result.emplace_back(Headers::ColumnHistograms{
        .columnId = data[0]->GetInt(),
        .histogramId = data[1]->GetInt(),
        .rangeStart = Value(data[2]->GetBlockData(), data[2]->GetBlockSize(), columnType),
        .rangeEnd = Value(data[3]->GetBlockData(), data[3]->GetBlockSize(), columnType),
        .rowCount = data[4]->GetInt(),
        .distinctCount = data[5]->GetInt(),
      });
    }

    return result;
  }

  void ServerInstance::UpdateIdentityByColumnId(const int32_t & tableId, const int32_t& columnId, const int64_t& lastValue)const{
    auto* table = this->masterDb->OpenTable(MasterDbTables::SysIdentityColumns);

    const vector<Value> updates{
      Value(lastValue, 4)
    };

    auto* leftColumnOperation = new Expressions::ColumnExpression(0);
    auto* leftLiteraValue = new Expressions::ConstantExpression(Value(tableId, 0));

    auto* leftBinaryExpr = new Expressions::BinaryExpression(leftColumnOperation, leftLiteraValue, Expressions::BinaryOperator::Equal);

    auto* rightColumnOperation = new Expressions::ColumnExpression(1);
    auto* rightLiteraValue = new Expressions::ConstantExpression(Value(columnId, 1));

    auto* rightBinaryExpr = new Expressions::BinaryExpression(rightColumnOperation, rightLiteraValue, Expressions::BinaryOperator::Equal);

    const Expressions::LogicalExpression logicalExpr(leftBinaryExpr, rightBinaryExpr, Expressions::LogicalType::And);

    table->ClusteredIndexScanUpdate(this->baseProperties, &logicalExpr, updates);
  }

  void ServerInstance::UpdateTableStatisticsById(
    const int32_t &tableId,
    const int64_t& rowCount,
    const int32_t& rowSize
  ) const{
    using namespace DatabaseEngine::StorageTypes;

    const std::vector<Value> updates = {
      Value(rowCount, static_cast<column_index_t>(SysTableStats::RowCount)),
      Value(rowSize, static_cast<column_index_t>(SysTableStats::AvgRowSize))
    };

    Table* table = this->masterDb->OpenTable(MasterDbTables::SysTableStats);

    auto* columnOperation = new Expressions::ColumnExpression(static_cast<column_index_t>(SysTableStats::TableId));
    auto* literaValue = new Expressions::ConstantExpression(Value(tableId, static_cast<column_index_t>(SysTableStats::TableId)));

    const Expressions::BinaryExpression binaryExpr(columnOperation, literaValue, Expressions::BinaryOperator::Equal);

    table->ClusteredIndexScanUpdate(this->baseProperties, &binaryExpr, updates);
  }

  void ServerInstance::UpdateColumnStatisticsById(
    const int32_t &columnId,
    const int64_t& distinctCount,
    const int64_t& nullCount,
    const Value& min,
    const Value& max
  ) const{
    using namespace DatabaseEngine::StorageTypes;

    const std::vector<Value> updates = {
      Value(distinctCount, static_cast<column_index_t>(SysColumnStats::DistinctCount)),
      Value(nullCount, static_cast<column_index_t>(SysColumnStats::NullCount)),
      Value(std::string(reinterpret_cast<const char*>(min.GetRawData()), min.GetSize()), static_cast<column_index_t>(SysColumnStats::MinimumValue)),
      Value(std::string(reinterpret_cast<const char*>(max.GetRawData()), max.GetSize()), static_cast<column_index_t>(SysColumnStats::MaximumValue))
    };

    Table* table = this->masterDb->OpenTable(MasterDbTables::SysColumnStats);

    auto* columnOperation = new Expressions::ColumnExpression(static_cast<column_index_t>(SysColumnStats::ColumnId));
    auto* literalValue = new Expressions::ConstantExpression(Value(columnId, static_cast<column_index_t>(SysColumnStats::ColumnId)));

    const Expressions::BinaryExpression binaryExpr(columnOperation, literalValue, Expressions::BinaryOperator::Equal);

    table->ClusteredIndexScanUpdate(this->baseProperties, &binaryExpr, updates);
  }

  Errors::RuntimeStatus ServerInstance::UpdateColumnById(const int32_t &columnId, const std::vector<Value> &updates) const{
    using namespace DatabaseEngine::StorageTypes;

    Table* table = this->masterDb->OpenTable(MasterDbTables::SysColumns);

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int));

    return table->ClusteredIndexSeekUpdate(this->baseProperties, nullptr, &key, &key, updates);
  }

  DatabaseEngine::Database * ServerInstance::GetMasterDb()const{ return this->masterDb; }

  void ServerInstance::Shutdown(){
    for (const auto &database: this->databases | views::values){
      database->UpdateMasterDatabase();
      delete database;
    }

    this->masterDb->UpdateMasterDatabase();

    delete this->masterDb;
    delete this->versionDb;
  }

  DatabaseEngine::Database* ServerInstance::UseDatabase(const int32_t & databaseId, const bool& isServerInitialization){
    DatabaseEngine::Database *db = nullptr;

    if (databaseId == 1)
      return this->masterDb;

    if (this->databases.TryGetValue(databaseId, db))
      return db;

    const auto dbHeader = this->SelectDatabaseById(databaseId);

    db = new DatabaseEngine::Database(dbHeader.name, isServerInitialization);

    // for (const auto& log : db->RecoverLogs()) {
    //   std::cout << log << std::endl;
    // }
    // db->GetIdentityColumns();

    //master db id
    this->databases.Add(databaseId, db);

    return db;
  }

  void ServerInstance::UseMasterDb(){
    this->masterDb = new DatabaseEngine::Database(this->sysDbName, this->sysTables);
    this->masterDb->GetColumnsHeaders();
    this->masterDb->GetIdentityColumns();

    for (const auto& role : this->SelectRoles())
      const auto _ = this->roleManager.AddRole(role.name, new Security::Role(role));

    for (const auto& user : this->SelectUsers()) {
      const auto* role = this->roleManager.GetRole(user.roleId);
      const auto _ = this->userManager.AddUser(user.id, user.name, user.passwordHash, role);
    }

    const auto checkpoint = DatabaseEngine::Logging::WriteAheadLogger::Get().RecoverLastCheckPoint();

    DatabaseEngine::TransactionManager::Get().SetTransactionId(checkpoint.transactionId + 1);

    std::cout << this->sysDbName << " initialized successfully" << std::endl;
  }

  DatabaseEngine::VersionDatabase * ServerInstance::GetVersionDatabase() const{ return this->versionDb; }
}