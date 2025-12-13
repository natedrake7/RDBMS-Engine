#pragma once
#include "../../Systemic/include/Headers.h"
#include "../../Systemic/include/Errors.h"
#include "../../DatabaseEngine/include/Database.h"
#include "../../DatabaseEngine/include/SystemDatabases/VersionDatabase.h"
#include "../../Systemic/include/Security/Security.h"
#include "RoleManager.h"
#include "SessionManager.h"
#include "UserManager.h"

#include <string>
#include <vector>

namespace DatabaseEngine {
  class SystemCatalog;
}

namespace DatabaseEngine {
  class Database;

}

namespace Network {
  class Server {
    std::string versionDbName;
    std::string versionDbPath;

    DatabaseEngine::VersionDatabase *versionDb;

    Dictionary<int32_t, DatabaseEngine::Database*> databases;
    MultiThreading::ReadWriteMutex databasesLatch;

    DatabaseEngine::SystemCatalog* systemCatalog;

    Sessions::SessionManager sessionManager;

    Security::RoleManager roleManager;
    Security::UserManager userManager;

    Server();
    ~Server();

    void ReadConfiguration(const std::string& configPath);
    void CreateVersionDatabase();
    [[nodiscard]] bool VersionDbExists()const;

    void CreateSystemRoles();
    void CreateSystemUsers();

  public:
    [[nodiscard]] static Server& Get();
    void Initialize(const std::string& configPath);

    //Security Functions
    [[nodiscard]]Errors::RuntimeStatus GrantRole(const DataTypes::Guid& currentSessionId, const std::string& username, const Security::Role* role)const;
    bool UserExists(const std::string& userName)const;
    bool CreateUser(const DatabaseEngine::ExecutionProperties& properties, const std::string& userName, const std::string& password, const std::string& roleName);
    Errors::RuntimeStatus UpdateUserById(
      const DataTypes::Guid& callerSessionId,
      const int32_t &userId,
      const int32_t &roleId
    )const;
    [[nodiscard]] const Security::User* Authenticate(const std::string& username, const std::string& password)const;

    bool RoleExists(const std::string& role)const;
    const Security::Role* GetRole(const std::string& roleName)const;

    //Session Functions
    [[nodiscard]] const Network::Session* CreateSession(const Security::User* user);
    [[nodiscard]] const Network::Session* GetSession(const DataTypes::Guid& key)const;
    [[nodiscard]] bool CloseSession(const DataTypes::Guid& key);
    [[nodiscard]] bool UpdateSession(const DataTypes::Guid& key, const int32_t& databaseId)const;
    [[nodiscard]] bool AddOrSetVariable(const DataTypes::Guid& sessionId, const Variable& variable)const;

    [[nodiscard]] QueryPipeline::Cursor* CreateCursor(
      const DataTypes::Guid &id,
      const DatabaseEngine::ExecutionProperties& properties,
      QueryPipeline::PhysicalPlan::ExecutionNode *physicalPlan
    )const;
    [[nodiscard]] bool CloseCursor(const DataTypes::Guid &id, const QueryPipeline::PipelineConstants::cursor_id_t& cursorId)const;

    //Cursor Functions
    // QueryPipeline::Cursor* CreateCursor(QueryPipeline::PhysicalPlan::PhysicalOperator* plan);
    // QueryPipeline::Cursor* GetCursor(const QueryPipeline::PipelineConstants::cursor_id_t& cursorId)const;
    // void DeleteCursor(const QueryPipeline::PipelineConstants::cursor_id_t& cursorId)const;

    void Shutdown();
    [[nodiscard]] DatabaseEngine::Database* UseDatabase(const int32_t & databaseId, const bool& isServerInitialization = false);
    DatabaseEngine::VersionDatabase* GetVersionDatabase()const;

    const Dictionary<int32_t, DatabaseEngine::Database*>& GetDatabases()const;
    MultiThreading::ReadWriteMutex& GetDatabasesLatch();
    
  };
}