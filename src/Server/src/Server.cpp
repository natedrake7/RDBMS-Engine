#include "../include/Server.h"
#include "../../DatabaseEngine/include/Logger/WriteAheadLogger.h"
#include "../../DatabaseEngine/include/Managers/TransactionManager.h"
#include "../../DatabaseEngine/include/SystemDatabases/SystemCatalog.h"
#include "../../DatabaseEngine/include/SystemDatabases/TemporaryDatabase.h"
#include "../../Systemic/include/Guards/ReaderGuard.h"
#include "../../Systemic/include/Guards/WriterGuard.h"

#include <iostream>

#include "ValidationMessages.h"

namespace Network {
   Server::Server(){
    this->temporaryDatabase = nullptr;
    this->systemCatalog = nullptr;
    this->versionDatabase = nullptr;
  }

  Server::~Server() = default;

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

    user->role = defaultRole;
    const auto _ = this->userManager.AddSystemUser(user);
  }

  Server & Server::Get(){
    static Server instance;

    return instance;
  }

  void Server::Initialize(const std::string_view configPath){
    this->temporaryDatabase = &DatabaseEngine::TemporaryDatabase::Get();
    this->temporaryDatabase->Initialize(configPath);

    this->versionDatabase = &DatabaseEngine::VersionDatabase::Get();
    this->versionDatabase->Initialize(configPath);

    this->systemCatalog = &DatabaseEngine::SystemCatalog::Get();
    if (this->systemCatalog->Initialize(configPath)){
      this->CreateSystemRoles();
      this->CreateSystemUsers();
      return;
    }

    const auto lastCheckpoint = DatabaseEngine::Logging::WriteAheadLogger::Get().RecoverLastCheckPoint();
    DatabaseEngine::TransactionManager::Get().SetTransactionId(lastCheckpoint.transactionId + 1);

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
    Int userId = -1;

    if (!this->userManager.GrantRole(username, role, userId))
      return {
        Errors::RuntimeError::Error,
        "Failed to grant role: " + role->name + " to user: " + username,
      };

    return this->UpdateUserById(currentSessionId, userId, role->id);
  }

  Errors::RuntimeStatus Server::UpdateUserById(
    const DataTypes::Guid& callerSessionId,
    const Int userId,
    const Int roleId
  )const{

      const auto* currentSession = this->sessionManager.GetSession(callerSessionId);

      if (currentSession == nullptr || currentSession->user == nullptr)
        return Errors::RuntimeStatus(Errors::RuntimeError::InvalidSession, Messages::FAILED_TO_GET_USER_SESSION);

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

  bool Server::UpdateSession(const DataTypes::Guid &key, const Int databaseId)const{
    return this->sessionManager.UpdateSession(key, databaseId);
  }

  bool Server::AddOrSetVariable(const DataTypes::Guid &sessionId, const Variable& variable) const {
    return this->sessionManager.AddOrSetVariable(sessionId, variable);
  }

  QueryPipeline::Cursor * Server::CreateCursor(
    const DataTypes::Guid &id,
    DatabaseEngine::ExecutionProperties& properties,
    QueryPipeline::PhysicalPlan::ExecutionNode *physicalPlan
  ) const {
    return this->sessionManager.CreateCursor(id, properties, physicalPlan);
  }

  bool Server::CloseCursor(const DataTypes::Guid &id, const QueryPipeline::PipelineConstants::cursor_id_t cursorId) const {
    return this->sessionManager.CloseCursor(id, cursorId);
  }

  void Server::Shutdown(){
    for (const auto &database: this->databases | std::views::values){
      database->UpdateMasterDatabase();
      delete database;
    }

    this->temporaryDatabase->Shutdown();
    this->systemCatalog->Shutdown();
    // this->versionDatabase->
  }

  DatabaseEngine::Database* Server::UseDatabase(const Int databaseId, const bool isServerInitialization){
    DatabaseEngine::Database *db = nullptr;

    if (databaseId == CATALOG_ID)
      return this->systemCatalog->GetDatabase();

    MultiThreading::ReaderGuard lock(&this->databasesLatch);

    if (this->databases.TryGetValue(databaseId, db))
      return db;

    const auto dbHeader = this->systemCatalog->SelectDatabaseById(databaseId);

    MultiThreading::WriterGuard::Promote(&this->databasesLatch, lock);

    if (this->databases.TryGetValue(databaseId, db))
      return db;

    db = new DatabaseEngine::Database(dbHeader.name, isServerInitialization);

    this->databases.Add(databaseId, db);

    return db;
  }

  const Dictionary<Int, DatabaseEngine::Database *> & Server::GetDatabases() const{ return this->databases; }

  MultiThreading::ReadWriteMutex & Server::GetDatabasesLatch(){ return this->databasesLatch; }
}
