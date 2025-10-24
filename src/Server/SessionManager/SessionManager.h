#pragma once
#include "../../Systemic/DataStructures/Dictionary/Dictionary.h"
#include "../../Systemic/Network/Session.h"

#include <mutex>

namespace Server::Sessions {
  class SessionManager {

    Dictionary<DataTypes::Guid, Network::Session*> sessions;

    std::mutex mutex;

    [[nodiscard]] Network::Session* TryGetSessionWithoutLock(const DataTypes::Guid& id)const;

  public:
    SessionManager();
    ~SessionManager();

    Network::Session* CreateSession(const std::string& username);
    Network::Session* GetSession(const DataTypes::Guid& id);
    [[nodiscard]] bool CloseSession(const DataTypes::Guid& id);
    [[nodiscard]] bool UpdateSession(const DataTypes::Guid& id, const int32_t& databaseId);
  };
}
