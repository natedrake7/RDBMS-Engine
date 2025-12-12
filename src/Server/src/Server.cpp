#include "../include/Server.h"
#include "../../Systemic/include/Converter.h"

#include <fstream>
#include <nlohmann/json.hpp>
#include "../../DatabaseEngine/include/DataStorage/Block.h"
#include "../../DatabaseEngine/include/Managers/TransactionManager.h"
#include "../../DatabaseEngine/include/SystemDatabases/SystemCatalog.h"

#include <iostream>

using json = nlohmann::json;

namespace Network {
   Server::Server(){
     this->systemCatalog = nullptr;
     this->versionDb = nullptr;
  }

  Server::~Server() = default;

  void Server::ReadConfiguration(const std::string &configPath){
    std::ifstream file(configPath);

    if (!file.is_open())
      throw std::runtime_error("System Tables file: " + configPath + " could not be opened");

    json jsonFile;

    try {
      file >> jsonFile;
    }
    catch (std::exception &e)
    {
      throw std::runtime_error(e.what());
    }

    this->versionDbName = jsonFile.at("version_db_name");
    this->versionDbPath = jsonFile.at("version_db_path");
  }

  void Server::CreateVersionDatabase() {
    if (this->VersionDbExists()) {
      this->versionDb = new DatabaseEngine::VersionDatabase(this->versionDbName);
      return;
    }

    DatabaseEngine::CreateDatabase(this->versionDbName);
    this->versionDb = new DatabaseEngine::VersionDatabase(this->versionDbName);
  }

  bool Server::VersionDbExists() const{ return std::filesystem::exists(this->versionDbPath); }

  void Server::CreateSystemRoles() {
    const auto roles = this->systemCatalog->InsertSystemRoles();

    for (const auto& role : roles)
      const auto _ = this->roleManager.AddRole(role->name, role);
  }

  void Server::CreateSystemUsers() {
    const auto admin = std::string(Constants::ADMIN_NAME);

    const auto defaultRole = this->roleManager.GetRole(admin);

    std::string hashedPassword;
    if (Security::UserManager::HashPassword(admin, hashedPassword) == false) {
      std::cerr << "Failed to hash password for admin user" << std::endl;
      return;
    }

    auto* user = this->systemCatalog->InsertSystemUsers(hashedPassword, defaultRole->id);

    const auto _ = this->userManager.AddSystemUser(user);
  }

  Server & Server::Get(){
    static Server instance;

    return instance;
  }

  void Server::Initialize(const string &configPath){
    this->ReadConfiguration(configPath);

    this->CreateVersionDatabase();

    this->systemCatalog = &DatabaseEngine::SystemCatalog::Get();
    if (this->systemCatalog->Initialize(configPath)){
      this->CreateSystemRoles();
      this->CreateSystemUsers();
      return;
    }

    for (const auto& role : this->systemCatalog->SelectRoles())
      const auto _ = this->roleManager.AddRole(role.name, new Security::Role(role));

    for (const auto& user : this->systemCatalog->SelectUsers()) {
      const auto* role = this->roleManager.GetRole(user.roleId);

      const auto _ = this->userManager.AddUser(user.id, user.name, user.passwordHash, role);
    }
  }

  Errors::RuntimeStatus Server::GrantRole(
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

  Errors::RuntimeStatus Server::UpdateUserById(
    const DataTypes::Guid& callerSessionId,
    const int32_t &userId,
    const int32_t &roleId
  )const{

      const auto* currentSession = this->sessionManager.GetSession(callerSessionId);

      if (currentSession == nullptr || currentSession->user == nullptr)
        return{
          Errors::RuntimeError::InvalidSession,
          "Failed to validate session"
      };

      return this->systemCatalog->UpdateUserById(
        currentSession->user->name,
        userId,
        roleId
      );
    }

  bool Server::UserExists(const std::string &userName) const{
    return this->userManager.GetUser(userName) != nullptr;
  }

  bool Server::CreateUser(const DatabaseEngine::ExecutionProperties& properties, const std::string &userName, const std::string &password, const std::string& roleName){
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
      this->systemCatalog->InsertUserToMasterDb(
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

  const Security::User * Server::Authenticate(const std::string &username, const std::string &password)const{
    return this->userManager.Authenticate(username, password);
  }

  bool Server::RoleExists(const std::string &role) const{
    return this->roleManager.GetRole(role) != nullptr;
  }

  const Security::Role * Server::GetRole(const std::string &roleName)const {
    return this->roleManager.GetRole(roleName);
  }

  const Network::Session * Server::CreateSession(const Security::User* user){
    return this->sessionManager.CreateSession(user);
  }

  const Network::Session * Server::GetSession(const DataTypes::Guid &key)const{
    return this->sessionManager.GetSession(key);
  }

  bool Server::CloseSession(const DataTypes::Guid &key) {
    return this->sessionManager.CloseSession(key);
  }

  bool Server::UpdateSession(const DataTypes::Guid &key, const int32_t &databaseId)const{
    return this->sessionManager.UpdateSession(key, databaseId);
  }

  bool Server::AddOrSetVariable(const DataTypes::Guid &sessionId, const Variable& variable) const {
    return this->sessionManager.AddOrSetVariable(sessionId, variable);
  }

  QueryPipeline::Cursor * Server::CreateCursor(
    const DataTypes::Guid &id,
    const DatabaseEngine::ExecutionProperties& properties,
    QueryPipeline::PhysicalPlan::ExecutionNode *physicalPlan
  ) const {
    return this->sessionManager.CreateCursor(id, properties, physicalPlan);
  }

  bool Server::CloseCursor(const DataTypes::Guid &id, const QueryPipeline::PipelineConstants::cursor_id_t& cursorId) const {
    return this->sessionManager.CloseCursor(id, cursorId);
  }

  void Server::Shutdown(){
    for (const auto &database: this->databases | views::values){
      database->UpdateMasterDatabase();
      delete database;
    }

    this->systemCatalog->Shutdown();
    delete this->versionDb;
  }

  DatabaseEngine::Database* Server::UseDatabase(const int32_t & databaseId, const bool& isServerInitialization){
    DatabaseEngine::Database *db = nullptr;

    if (databaseId == 1)
      return this->systemCatalog->GetDatabase();

    if (this->databases.TryGetValue(databaseId, db))
      return db;

    const auto dbHeader = this->systemCatalog->SelectDatabaseById(databaseId);

    db = new DatabaseEngine::Database(dbHeader.name, isServerInitialization);

    // for (const auto& log : db->RecoverLogs()) {
    //   std::cout << log << std::endl;
    // }
    // db->GetIdentityColumns();

    //master db id
    this->databases.Add(databaseId, db);

    return db;
  }

  DatabaseEngine::VersionDatabase * Server::GetVersionDatabase() const{ return this->versionDb; }
}