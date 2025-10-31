#include "SessionManager.h"

#include "../../Systemic/MultiThreading/Guards/ReaderGuard/ReaderGuard.h"
#include "../../Systemic/MultiThreading/Guards/WriterGuard/WriterGuard.h"

#include <ranges>

namespace Server::Sessions {
  SessionManager::SessionManager() = default;

  SessionManager::~SessionManager(){
    for (const auto &session : this->sessions | views::values)
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

  bool SessionManager::UpdateSession(const DataTypes::Guid &id, const int32_t &databaseId)const{
    MultiThreading::WriterGuard guard(&this->mutex);

    auto* session = this->TryGetSessionWithoutLock(id);

    if (session == nullptr)
      return false;

    session->databaseId = databaseId;

    return true;
  }

  QueryPipeline::Cursor* SessionManager::CreateCursor(const DataTypes::Guid &id, QueryPipeline::PhysicalPlan::PhysicalOperator *physicalPlan)const{
    MultiThreading::WriterGuard guard(&this->mutex);

    auto* session = this->TryGetSessionWithoutLock(id);

    if (session == nullptr)
      return nullptr;

    delete session->cursor;

    session->cursor = new QueryPipeline::Cursor(0, physicalPlan, 10000);
    return session->cursor;
  }

  bool SessionManager::CloseCursor(const DataTypes::Guid &id) const{
    MultiThreading::WriterGuard guard(&this->mutex);

    auto* session = this->TryGetSessionWithoutLock(id);

    if (session == nullptr)
      return false;

    delete session->cursor;
    session->cursor = nullptr;

    return true;
  }

}