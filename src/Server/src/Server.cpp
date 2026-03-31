#include "../include/Server.h"
#include "../../CoreEngine/include/Logger/WriteAheadLogger.h"
#include "../../CoreEngine/include/Managers/TransactionManager.h"
#include "../../CoreEngine/include/SystemDatabases/SystemCatalog.h"
#include "../../CoreEngine/include/SystemDatabases/TemporaryDatabase.h"
#include "../../Systemic/include/Guards/ReaderGuard.h"
#include "../../Systemic/include/Guards/WriterGuard.h"

#include <iostream>

#include "ValidationMessages.h"
#include "../../CoreEngine/include/Managers/GlobalMemoryManager.h"

namespace Network {
   Server::Server(){
    this->temporaryDatabase = nullptr;
    this->systemCatalog = nullptr;
    this->versionDatabase = nullptr;
  }

  Server::~Server() = default;

  void Server::CreateSystemRoles(const CoreEngine::ExecutionContext& baseContext) {
    const auto roles = this->systemCatalog->InsertSystemRoles(baseContext);

    for (const auto* role : roles)
      const auto _ = this->roleManager.AddRole(role->name.ToView(), role);
  }

  void Server::CreateSystemUsers(const CoreEngine::ExecutionContext& baseContext) {
    const auto defaultRole = this->roleManager.GetRole(Constants::ADMIN_NAME);

    DataTypes::String hashedPassword(baseContext.GetAllocator());
    if (Security::UserManager::HashPassword(Constants::ADMIN_NAME, hashedPassword) == false) {
      std::cerr << "Failed to hash password for admin user" << std::endl;
      return;
    }

    const auto user = this->systemCatalog->InsertSystemUsers(baseContext, hashedPassword, defaultRole->id);
    const auto _ = this->userManager.AddUser(user.id, user.name, hashedPassword, defaultRole);
    // user->role = defaultRole;
    // const auto _ = this->userManager.AddSystemUser(user);
  }

  Server& Server::Get(){
    static Server instance;

    return instance;
  }

  void Server::Initialize(const DataTypes::StringView& configPath){
    const CoreEngine::ExecutionContext _baseContext;

    this->temporaryDatabase = &CoreEngine::TemporaryDatabase::Get();
    this->temporaryDatabase->Initialize(_baseContext.GetAllocator(), configPath);

    this->versionDatabase = &CoreEngine::VersionDatabase::Get();
    this->versionDatabase->Initialize(_baseContext, configPath);

    this->systemCatalog = &CoreEngine::SystemCatalog::Get();
    if (this->systemCatalog->Initialize(_baseContext, configPath)){
        this->CreateSystemRoles(_baseContext);
        this->CreateSystemUsers(_baseContext);
        return;
    }

    const auto lastCheckpoint = CoreEngine::Logging::WriteAheadLogger::Get().RecoverLastCheckPoint();
    CoreEngine::TransactionManager::Get().SetTransactionId(lastCheckpoint.transactionId + 1);

    const CoreEngine::Memory::Allocator allocator;
    for (const auto& role : this->systemCatalog->SelectRoles(&allocator))
        const auto _ = this->roleManager.AddRole(role.name.ToView(), &role);

    for (const auto& user : this->systemCatalog->SelectUsers(&allocator)) {
        const auto* role = this->roleManager.GetRole(user.roleId);
        const auto _ = this->userManager.AddUser(user.id, user.name, user.passwordHash, role);
    }
  }

  Errors::RuntimeStatus Server::GrantRole(
    const CoreEngine::ExecutionContext& context,
    const DataTypes::Guid& currentSessionId,
    const DataTypes::String& username,
    const Security::Role *role
  )const{
    Int userId = -1;

    if (!this->userManager.GrantRole(username.ToView(), role, userId))
      return {
        Errors::RuntimeError::Error,
        "Failed to grant role: " + role->name + " to user: " + username,
      };

    return this->UpdateUserById(context, currentSessionId, userId, role->id);
  }

    Errors::RuntimeStatus Server::UpdateUserById(
        const CoreEngine::ExecutionContext& context,
        const DataTypes::Guid& callerSessionId,
        const Int userId,
        const Int roleId
    )const{
        const auto* currentSession = this->sessionManager.GetSession(callerSessionId);

        if (currentSession == nullptr || currentSession->user == nullptr)
            return Errors::RuntimeStatus(
                Errors::RuntimeError::InvalidSession,
                Messages::FAILED_TO_GET_USER_SESSION,
                context.GetAllocator()
            );

        return this->systemCatalog->UpdateUserById(
            context,
            currentSession->user->name.ToView(),
            userId,
            roleId
        );
    }

  bool Server::UserExists(const DataTypes::String& userName) const{
    return this->userManager.GetUser(userName.ToView()) != nullptr;
  }

  bool Server::CreateUser(
      const CoreEngine::ExecutionContext& context,
      const DataTypes::String& userName,
      const DataTypes::String& password,
      const DataTypes::String& roleName
    ){
    if (this->userManager.GetUser(userName.ToView()) != nullptr)
      return false;

    const auto* role = this->roleManager.GetRole(roleName.ToView());

    if (role == nullptr)
      return false;

    DataTypes::String hashedPassword(context.GetAllocator());
    if (Security::UserManager::HashPassword(password.ToView(), hashedPassword) == false) {
      std::cerr << "Failed to hash password for user" << userName << std::endl;
      return false;
    }

    const auto result =
      this->systemCatalog->InsertUserToMasterDb(
        context,
        userName.ToView(),
        hashedPassword.ToView(),
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

  const Security::User* Server::Authenticate(const DataTypes::String& username, const DataTypes::String& password)const{
    return this->userManager.Authenticate(username.ToView(), password.ToView());
  }

  const Security::User* Server::Authenticate(const std::string& username, const std::string& password) const{
      const auto usernameView = DataTypes::StringView(username);
      const auto passwordView = DataTypes::StringView(password);

      return this->userManager.Authenticate(usernameView, passwordView);
  }

  bool Server::RoleExists(const DataTypes::String& role) const{
    return this->roleManager.GetRole(role.ToView()) != nullptr;
  }

  const Security::Role * Server::GetRole(const DataTypes::String& roleName)const {
    return this->roleManager.GetRole(roleName.ToView());
  }

  const Session * Server::CreateSession(const Security::User* user){
    return this->sessionManager.CreateSession(user);
  }

  const Session * Server::GetSession(const DataTypes::Guid &key)const{
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

  QueryPipeline::Cursor* Server::CreateCursor(
    const DataTypes::Guid &id,
    CoreEngine::ExecutionContext& context,
    QueryPipeline::PhysicalPlan::ExecutionNode *physicalPlan
  ) const {
    return this->sessionManager.CreateCursor(id, context, physicalPlan);
  }

  bool Server::CloseCursor(const DataTypes::Guid &id, const QueryPipeline::PipelineConstants::cursor_id_t cursorId) const {
    return this->sessionManager.CloseCursor(id, cursorId);
  }

  void Server::Shutdown(){
    const CoreEngine::Memory::Allocator allocator;
    for (const auto &database: this->databases | std::views::values){
          database->UpdateMasterDatabase(&allocator);
          delete database;
    }

    this->temporaryDatabase->Shutdown();
    this->systemCatalog->Shutdown();
    // this->versionDatabase->
  }

    CoreEngine::Database* Server::UseDatabase(
        const CoreEngine::ExecutionContext& context,
        const Int databaseId,
        const bool isServerInitialization
    ){
        CoreEngine::Database *db = nullptr;

        if (databaseId == Constants::SYSTEM_CATALOG_ID) return this->systemCatalog->GetDatabase();

        MultiThreading::ReaderGuard lock(&this->databasesLatch);

        if (this->databases.TryGetValue(databaseId, db)) return db;

        const auto dbHeader = this->systemCatalog->SelectDatabaseById(context.GetAllocator(), databaseId);

        MultiThreading::WriterGuard::Promote(&this->databasesLatch, lock);

        if (this->databases.TryGetValue(databaseId, db)) return db;

        db = new CoreEngine::Database(
            context.GetAllocator(),
            databaseId,
            dbHeader.name,
            isServerInitialization
        );
        this->databases.Add(databaseId, db);

        return db;
    }

    const Dictionary<Int, CoreEngine::Database *> & Server::GetDatabases() const{ return this->databases; }

    MultiThreading::ReadWriteMutex & Server::GetDatabasesLatch(){ return this->databasesLatch; }
}
