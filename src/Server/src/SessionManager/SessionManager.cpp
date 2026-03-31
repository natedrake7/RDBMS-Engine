#include "../../include/SessionManager.h"

#include "../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../../Systemic/include/Guards/WriterGuard.h"

#include <ranges>

#include "../../../CoreEngine/include/Managers/GlobalMemoryManager.h"

namespace Network::Sessions {
    SessionManager::SessionManager() = default;

    SessionManager::~SessionManager(){
        for (const auto* session : this->sessions | std::views::values)
            delete session;
    }

    const Session * SessionManager::CreateSession(const Security::User* user){
        MultiThreading::WriterGuard guard(&this->mutex);

        auto* session = new Session(user);
        // auto* session = DatabaseEngine::AllocateMiscEntity<Session>(user);

        this->sessions.Add(session->sessionId, session);

        return session;
    }

    const Session * SessionManager::GetSession(const DataTypes::Guid &id)const{
        Session* session = nullptr;

        MultiThreading::ReaderGuard guard(&this->mutex);
        this->sessions.TryGetValue(id, session);

        return session;
    }

    Session* SessionManager::TryGetSessionWithoutLock(const DataTypes::Guid &id)const{
        Session* session = nullptr;
        this->sessions.TryGetValue(id, session);
        return session;
    }

    bool SessionManager::CloseSession(const DataTypes::Guid &id){
        Session* session = nullptr;

        MultiThreading::WriterGuard guard(&this->mutex);

        if (this->sessions.TryGetValue(id, session)) {
            this->sessions.Remove(id);
            delete session;
            // DatabaseEngine::DeallocateMiscEntity(session);
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

    bool SessionManager::AddOrSetVariable(const DataTypes::Guid &id, const Variable& variable)const {
        MultiThreading::WriterGuard guard(&this->mutex);

        auto* session = this->TryGetSessionWithoutLock(id);

        if (session == nullptr)
            return false;

        session->variables.AddOrUpdate(variable.GetNormalizedName(), variable);
        return true;
    }

    QueryPipeline::Cursor* SessionManager::CreateCursor(
        const DataTypes::Guid &id,
        CoreEngine::ExecutionContext& context,
        QueryPipeline::PhysicalPlan::ExecutionNode *physicalPlan
    )const{
        MultiThreading::WriterGuard guard(&this->mutex);

        auto* session = this->TryGetSessionWithoutLock(id);

        if (session == nullptr)
            return nullptr;

        const auto cursorId = session->nextCursorId++;
        auto* cursor = context.Allocate<QueryPipeline::Cursor>(
            cursorId,
            context,
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
        cursor->GetExecutionContext().ResetAllocator();

        session->cursors.Remove(cursorId);
        return true;
    }
}
