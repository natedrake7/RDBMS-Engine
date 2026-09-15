#include "../include/SessionManager.h"

#include "../../Systemic/include/Guards/ReaderGuard.h"
#include "../../Systemic/include/Guards/WriterGuard.h"

#include <ranges>

#include "../../CoreEngine/include/Managers/GlobalMemoryManager.h"
#include "../../QueryPipeline/include/CompileContext.h"

#include "../../Systemic/include/DataTypes/BoundVariable.h"

namespace Network::Sessions {
    const Session* SessionManager::CreateSession(const Security::User* user){
        MultiThreading::WriterGuard guard(&this->mutex);

        const auto session = std::make_shared<Session>(user);
        session->databaseId = Constants::SYSTEM_CATALOG_ID;
        session->sessionId = this->NextSessionId();
        this->_sessions.Add(session->sessionId, session);
        return session.get();
    }

    const Session* SessionManager::GetSession(const session_id_t id)const{
        std::shared_ptr<Session> session;

        MultiThreading::ReaderGuard guard(&this->mutex);
        this->_sessions.TryGetValue(id, session);

        return session.get();
    }

    Session* SessionManager::TryGetSessionWithoutLock(const session_id_t id)const{
        std::shared_ptr<Session> session;
        this->_sessions.TryGetValue(id, session);
        return session.get();
    }

    session_id_t SessionManager::NextSessionId(){
        return this->_nextSessionId.fetch_add(1, std::memory_order_relaxed);
    }

    SessionManager::SessionManager()
        : _nextSessionId(INVALID_SESSION_ID + 1){}

    bool SessionManager::CloseSession(const session_id_t id){
        std::shared_ptr<Session> session;

        MultiThreading::WriterGuard guard(&this->mutex);

        if (this->_sessions.Contains(id)){
            this->_sessions.Remove(id);
            return true;
        }

        return false;
    }

    bool SessionManager::UpdateSession(const session_id_t id, const Int databaseId)const{
        MultiThreading::WriterGuard guard(&this->mutex);

        auto* session = this->TryGetSessionWithoutLock(id);

        if (session == nullptr)
            return false;

        session->databaseId = databaseId;

        return true;
    }

    bool SessionManager::AddOrSetVariable(
        const session_id_t id,
        const Variable& variable
    )const {
        MultiThreading::WriterGuard guard(&this->mutex);

        auto* session = this->TryGetSessionWithoutLock(id);
        if (session == nullptr)
            return false;

        std::unique_ptr<BoundVariable>* existingVar = nullptr;
        const auto nameView = DataTypes::StringView::ViewOf(variable.GetNormalizedName());
        if (session->variables.TryGetValue(nameView, existingVar)){
            (*existingVar)->SetValue(variable);
            return true;
        }

        auto boundVariable = std::make_unique<BoundVariable>(variable);
        const auto* raw = boundVariable.get();

        session->variables.AddOrUpdate(
            DataTypes::StringView::ViewOf(raw->GetNormalizedName()),
            std::move(boundVariable)
        );

        return true;
    }

    QueryPipeline::Cursor* SessionManager::CreateCursor(
        const session_id_t id,
        const QueryPipeline::CompileContext& compileContext,
        CoreEngine::ExecutionContext& executionContext,
        QueryPipeline::PhysicalPlan::PlanNode *physicalPlan
    )const{
        MultiThreading::WriterGuard guard(&this->mutex);

        auto* session = this->TryGetSessionWithoutLock(id);

        if (session == nullptr)
            return nullptr;

        const auto cursorId = session->nextCursorId++;

        auto* cursor = compileContext.Allocate<QueryPipeline::Cursor>(
            cursorId,
            executionContext,
            physicalPlan
        );

        session->cursors.Add(cursorId, cursor);
        return cursor;
    }

    bool SessionManager::CloseCursor(
        const session_id_t id,
        const QueryPipeline::PipelineConstants::cursor_id_t cursorId
    ) const{
        MultiThreading::WriterGuard guard(&this->mutex);

        auto* session = this->TryGetSessionWithoutLock(id);

        if (session == nullptr)
            return false;

        const auto* cursor = session->cursors.Get(cursorId);
        cursor->GetExecutionContext().ReleaseAllocator();

        session->cursors.Remove(cursorId);
        return true;
    }
}
