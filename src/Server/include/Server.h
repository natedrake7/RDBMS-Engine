#pragma once
#include "../../CoreEngine/include/Errors.h"
#include "../../CoreEngine/include/SystemDatabases/VersionDatabase.h"
#include "Security/Security.h"
#include "RoleManager.h"
#include "SessionManager.h"
#include "UserManager.h"

namespace QueryPipeline
{
    class CompileContext;
}

namespace CoreEngine {
  class TemporaryDatabase;
}

namespace CoreEngine {
  class SystemCatalog;
}

namespace CoreEngine {
  class Database;
}

namespace Network {
  class Server {
    Security::RoleManager roleManager;
    Security::UserManager userManager;
    Sessions::SessionManager sessionManager;

    Dictionary<Int, CoreEngine::Database*> databases;

    MultiThreading::Mutex databasesLatch;

    CoreEngine::TemporaryDatabase* temporaryDatabase;
    CoreEngine::SystemCatalog* systemCatalog;
    CoreEngine::VersionDatabase *versionDatabase;

    Server();

    void CreateSystemRoles(const CoreEngine::ExecutionContext& baseContext);
    void CreateSystemUsers(const CoreEngine::ExecutionContext& baseContext);

  public:
    [[nodiscard]] static Server& Get();
    void Initialize(const DataTypes::StringView& configPath);
    void Shutdown();

    //Security Functions
    [[nodiscard]]Errors::RuntimeStatus GrantRole(
        const CoreEngine::ExecutionContext& context,
        const DataTypes::Guid& currentSessionId,
        const DataTypes::String& username,
        const Security::Role* role
    )const;
    bool UserExists(const DataTypes::String& userName)const;
    bool CreateUser(
        const CoreEngine::ExecutionContext& context,
        const DataTypes::String& userName,
        const DataTypes::String& password,
        const DataTypes::String& roleName
    );
    Errors::RuntimeStatus UpdateUserById(
        const CoreEngine::ExecutionContext& context,
        const DataTypes::Guid& callerSessionId,
        Int userId,
        Int roleId
    )const;
    [[nodiscard]] const Security::User* Authenticate(const DataTypes::String& username, const DataTypes::String& password)const;
    [[nodiscard]] const Security::User* Authenticate(const DataTypes::StringView& username, const DataTypes::StringView& password)const;

    bool RoleExists(const DataTypes::String& role)const;
    const Security::Role* GetRole(const DataTypes::String& roleName)const;

    //Session Functions
    [[nodiscard]] const Network::Session* CreateSession(const Security::User* user);
    [[nodiscard]] const Network::Session* GetSession(const DataTypes::Guid& key)const;
    [[nodiscard]] bool CloseSession(const DataTypes::Guid& key);
    [[nodiscard]] bool UpdateSession(const DataTypes::Guid& key, Int databaseId)const;
    [[nodiscard]] bool AddOrSetVariable(const DataTypes::Guid& sessionId, const Variable& variable)const;

    [[nodiscard]] QueryPipeline::Cursor* CreateCursor(
        const DataTypes::Guid &id,
        const QueryPipeline::CompileContext& compileContext,
        CoreEngine::ExecutionContext& executionContext,
        QueryPipeline::PhysicalPlan::PlanNode *physicalPlan
    )const;
    [[nodiscard]] bool CloseCursor(const DataTypes::Guid &id, QueryPipeline::PipelineConstants::cursor_id_t cursorId)const;

    //Cursor Functions
    // QueryPipeline::Cursor* CreateCursor(QueryPipeline::PhysicalPlan::PhysicalOperator* plan);
    // QueryPipeline::Cursor* GetCursor(const QueryPipeline::PipelineConstants::cursor_id_t& cursorId)const;
    // void DeleteCursor(const QueryPipeline::PipelineConstants::cursor_id_t& cursorId)const;

    [[nodiscard]] CoreEngine::Database* UseDatabase(
        const CoreEngine::ExecutionContext& context,
        Int databaseId,
        bool isServerInitialization = false
    );
    const Dictionary<Int, CoreEngine::Database*>& GetDatabases()const;
    MultiThreading::Mutex& GetDatabasesLatch();
    
  };
}