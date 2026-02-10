#include "../../include/SessionManager.h"

#include "../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../../Systemic/include/Guards/WriterGuard.h"

#include <ranges>

namespace Network::Sessions {
  SessionManager::SessionManager() = default;

  SessionManager::~SessionManager(){
    for (const auto &session : this->sessions | std::views::values)
      delete session;
  }

  const Network::Session * SessionManager::CreateSession(const Security::User* user){
    MultiThreading::WriterGuard guard(&this->mutex);

    auto* session = new Network::Session(user);

    this->sessions.Add(session->sessionId, session);

    return session;
  }

  const Network::Session * SessionManager::GetSession(const DataTypes::Guid &id)const{
    Network::Session* session = nullptr;

    MultiThreading::ReaderGuard guard(&this->mutex);

    this->sessions.TryGetValue(id, session);

    return session;
  }

  Network::Session * SessionManager::TryGetSessionWithoutLock(const DataTypes::Guid &id)const{
    Network::Session* session = nullptr;

    this->sessions.TryGetValue(id, session);

    return session;
  }

  bool SessionManager::CloseSession(const DataTypes::Guid &id){
    Network::Session* session = nullptr;

    MultiThreading::WriterGuard guard(&this->mutex);

    if (this->sessions.TryGetValue(id, session)) {
      this->sessions.Remove(id);
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
    DatabaseEngine::ExecutionProperties& properties,
    QueryPipeline::PhysicalPlan::ExecutionNode *physicalPlan
  )const{
    MultiThreading::WriterGuard guard(&this->mutex);

    auto* session = this->TryGetSessionWithoutLock(id);

    if (session == nullptr)
      return nullptr;

    const auto cursorId = session->nextCursorId++;

    auto* cursor = new QueryPipeline::Cursor(cursorId, properties, physicalPlan);

    session->cursors.Add(cursorId, cursor);
    return cursor;
  }

  bool SessionManager::CloseCursor(const DataTypes::Guid &id, QueryPipeline::PipelineConstants::cursor_id_t cursorId) const{
    MultiThreading::WriterGuard guard(&this->mutex);

    auto* session = this->TryGetSessionWithoutLock(id);

    if (session == nullptr)
      return false;

    const auto* cursor = session->cursors.Get(cursorId);
    delete cursor;

    session->cursors.Remove(cursorId);
    return true;
  }

}