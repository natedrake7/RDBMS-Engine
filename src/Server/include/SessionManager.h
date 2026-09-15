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
    Dictionary<session_id_t, std::shared_ptr<Session>> _sessions;
    mutable MultiThreading::Mutex mutex;
    std::atomic<session_id_t> _nextSessionId;

    [[nodiscard]] Session* TryGetSessionWithoutLock(session_id_t id)const;
    [[nodiscard]] session_id_t NextSessionId();

    public:
        SessionManager();

        const Session* CreateSession(const Security::User* user);
        const Session* GetSession(session_id_t id)const;
        [[nodiscard]] bool CloseSession(session_id_t id);
        [[nodiscard]] bool UpdateSession(session_id_t id, Int databaseId)const;

        [[nodiscard]] bool AddOrSetVariable(session_id_t id, const Variable& variable)const;

        [[nodiscard]] QueryPipeline::Cursor* CreateCursor(
            session_id_t id,
            const QueryPipeline::CompileContext& compileContext,
            CoreEngine::ExecutionContext& executionContext,
            QueryPipeline::PhysicalPlan::PlanNode *physicalPlan
        )const;
        [[nodiscard]] bool CloseCursor(session_id_t id, QueryPipeline::PipelineConstants::cursor_id_t cursorId)const;
  };
}
