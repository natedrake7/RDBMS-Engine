#pragma once
#include "../../QueryPipeline/include/Cursor.h"
#include "../../Systemic/include/DataStructures/Dictionary.h"
#include "../../Systemic/include/Guards/Mutex.h"
#include "Security/Session.h"

namespace QueryPipeline{
    class CompileContext;
}

namespace Network::Sessions {
  class SessionManager {
    Dictionary<DataTypes::Guid, Session*> _sessions;

    mutable MultiThreading::Mutex mutex;

    [[nodiscard]] Session* TryGetSessionWithoutLock(const DataTypes::Guid& id)const;

  public:
    ~SessionManager();

    const Session* CreateSession(const Security::User* user);
    const Session* GetSession(const DataTypes::Guid& id)const;
    [[nodiscard]] bool CloseSession(const DataTypes::Guid& id);
    [[nodiscard]] bool UpdateSession(const DataTypes::Guid& id, Int databaseId)const;

    [[nodiscard]] bool AddOrSetVariable(const DataTypes::Guid& id, const Variable& variable)const;

    [[nodiscard]] QueryPipeline::Cursor* CreateCursor(
        const DataTypes::Guid &id,
        const QueryPipeline::CompileContext& compileContext,
        CoreEngine::ExecutionContext& executionContext,
        QueryPipeline::PhysicalPlan::PlanNode *physicalPlan
    )const;
    [[nodiscard]] bool CloseCursor(const DataTypes::Guid &id, QueryPipeline::PipelineConstants::cursor_id_t cursorId)const;
  };
}
