#include "SessionManager.h"
#include <ranges>

namespace Server::Sessions {
  SessionManager::SessionManager() = default;

  SessionManager::~SessionManager(){
    for (const auto &session : this->sessions | views::values)
      delete session;
  }

  Network::Session * SessionManager::CreateSession(const std::string& username){
    this->mutex.lock();

    auto* session = new Network::Session();
    session->username = username;

    this->sessions.Add(session->sessionId, session);
    this->mutex.unlock();

    return session;
  }

  Network::Session * SessionManager::GetSession(const DataTypes::Guid &id){
    Network::Session* session = nullptr;

    std::lock_guard<std::mutex> lock(mutex);

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

    std::lock_guard<std::mutex> lock(mutex);

    if (this->sessions.TryGetValue(id, session)) {
      this->sessions.Remove(id);
      delete session;

      return true;
    }

    return false;
  }

  bool SessionManager::UpdateSession(const DataTypes::Guid &id, const int32_t &databaseId){
    std::lock_guard<std::mutex> lock(mutex);

    auto* session = this->TryGetSessionWithoutLock(id);

    if (session == nullptr)
      return false;

    session->databaseId = databaseId;

    return true;
  }



}