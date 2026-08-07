#include "../../include/SessionManager.h"

#include "../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../../Systemic/include/Guards/WriterGuard.h"

#include <ranges>

#include "../../../CoreEngine/include/Managers/GlobalMemoryManager.h"
#include "../../../QueryPipeline/include/CompileContext.h"

#include "../../../Systemic/include/DataTypes/BoundVariable.h"

namespace Network::Sessions {
    SessionManager::~SessionManager(){
        for (const auto* session : this->_sessions | std::views::values)
            delete session;
    }

    const Session* SessionManager::CreateSession(const Security::User* user){
        MultiThreading::WriterGuard guard(&this->mutex);

        auto* session = new Session(user);
        this->_sessions.Add(session->sessionId, session);
        return session;
    }

    const Session* SessionManager::GetSession(const DataTypes::Guid &id)const{
        Session* session = nullptr;

        MultiThreading::ReaderGuard guard(&this->mutex);
        this->_sessions.TryGetValue(id, session);

        return session;
    }

    Session* SessionManager::TryGetSessionWithoutLock(const DataTypes::Guid &id)const{
        Session* session = nullptr;
        this->_sessions.TryGetValue(id, session);
        return session;
    }

    bool SessionManager::CloseSession(const DataTypes::Guid &id){
        Session* session = nullptr;

        MultiThreading::WriterGuard guard(&this->mutex);

        if (this->_sessions.TryGetValue(id, session)) {
            this->_sessions.Remove(id);
            delete session;
            return true;
        }

        return false;
    }

    bool SessionManager::UpdateSession(const DataTypes::Guid &id, const Int databaseId)const{
        MultiThreading::WriterGuard guard(&this->mutex);

        auto* session = this->TryGetSessionWithoutLock(id);

        if (session == nullptr)
            return false;

        session->databaseId = databaseId;

        return true;
    }

    bool SessionManager::AddOrSetVariable(
        const DataTypes::Guid &id,
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
        const DataTypes::Guid &id,
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
        const DataTypes::Guid &id,
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
