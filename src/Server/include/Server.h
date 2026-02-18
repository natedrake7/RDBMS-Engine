#pragma once
#include "../../Systemic/include/Errors.h"
#include "../../DatabaseEngine/include/Database.h"
#include "../../DatabaseEngine/include/SystemDatabases/VersionDatabase.h"
#include "../../Systemic/include/Security/Security.h"
#include "RoleManager.h"
#include "SessionManager.h"
#include "UserManager.h"

#include <string>

namespace DatabaseEngine {
  class TemporaryDatabase;
}

namespace DatabaseEngine {
  class SystemCatalog;
}

namespace DatabaseEngine {
  class Database;
}

namespace Network {
  class Server {

    Dictionary<Int, DatabaseEngine::Database*> databases;
    MultiThreading::ReadWriteMutex databasesLatch;

    DatabaseEngine::TemporaryDatabase* temporaryDatabase;
    DatabaseEngine::SystemCatalog* systemCatalog;
    DatabaseEngine::VersionDatabase *versionDatabase;

    Sessions::SessionManager sessionManager;

    Security::RoleManager roleManager;
    Security::UserManager userManager;

    Server();
    ~Server();

    void CreateSystemRoles();
    void CreateSystemUsers();

  public:
    [[nodiscard]] static Server& Get();
    void Initialize(std::string_view configPath);
    void Shutdown();

    //Security Functions
    [[nodiscard]]Errors::RuntimeStatus GrantRole(const DataTypes::Guid& currentSessionId, const std::string& username, const Security::Role* role)const;
    bool UserExists(const std::string& userName)const;
    bool CreateUser(const DatabaseEngine::ExecutionProperties& properties, const std::string& userName, const std::string& password, const std::string& roleName);
    Errors::RuntimeStatus UpdateUserById(
      const DataTypes::Guid& callerSessionId,
      Int userId,
      Int roleId
    )const;
    [[nodiscard]] const Security::User* Authenticate(const std::string& username, const std::string& password)const;

    bool RoleExists(const std::string& role)const;
    const Security::Role* GetRole(const std::string& roleName)const;

    //Session Functions
    [[nodiscard]] const Network::Session* CreateSession(const Security::User* user);
    [[nodiscard]] const Network::Session* GetSession(const DataTypes::Guid& key)const;
    [[nodiscard]] bool CloseSession(const DataTypes::Guid& key);
    [[nodiscard]] bool UpdateSession(const DataTypes::Guid& key, Int databaseId)const;
    [[nodiscard]] bool AddOrSetVariable(const DataTypes::Guid& sessionId, const Variable& variable)const;

    [[nodiscard]] QueryPipeline::Cursor* CreateCursor(
      const DataTypes::Guid &id,
      DatabaseEngine::ExecutionProperties& properties,
      QueryPipeline::PhysicalPlan::ExecutionNode *physicalPlan
    )const;
    [[nodiscard]] bool CloseCursor(const DataTypes::Guid &id, QueryPipeline::PipelineConstants::cursor_id_t cursorId)const;

    //Cursor Functions
    // QueryPipeline::Cursor* CreateCursor(QueryPipeline::PhysicalPlan::PhysicalOperator* plan);
    // QueryPipeline::Cursor* GetCursor(const QueryPipeline::PipelineConstants::cursor_id_t& cursorId)const;
    // void DeleteCursor(const QueryPipeline::PipelineConstants::cursor_id_t& cursorId)const;

    [[nodiscard]] DatabaseEngine::Database* UseDatabase(Int databaseId, bool isServerInitialization = false);
    const Dictionary<Int, DatabaseEngine::Database*>& GetDatabases()const;
    MultiThreading::ReadWriteMutex& GetDatabasesLatch();
    
  };
}