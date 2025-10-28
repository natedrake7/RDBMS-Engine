#pragma once
#include "../../QueryPipeline/Cursor/Cursor.h"
#include "../../Systemic/DataStructures/Dictionary/Dictionary.h"
#include "../../Systemic/MultiThreading/ReadWriteMutex/ReadWriteMutex.h"
#include "../../Systemic/Network/Session.h"

namespace Server::Sessions {
  class SessionManager {

    Dictionary<DataTypes::Guid, Network::Session*> sessions;

    mutable MultiThreading::ReadWriteMutex mutex;

    [[nodiscard]] Network::Session* TryGetSessionWithoutLock(const DataTypes::Guid& id)const;

  public:
    SessionManager();
    ~SessionManager();

    const Network::Session* CreateSession(const Security::User* user);
    const Network::Session* GetSession(const DataTypes::Guid& id)const;
    [[nodiscard]] bool CloseSession(const DataTypes::Guid& id);
    [[nodiscard]] bool UpdateSession(const DataTypes::Guid& id, const int32_t& databaseId)const;

    [[nodiscard]] QueryPipeline::Cursor* CreateCursor(const DataTypes::Guid &id, QueryPipeline::PhysicalPlan::PhysicalOperator *physicalPlan)const;
    [[nodiscard]] bool CloseCursor(const DataTypes::Guid &id)const;
  };
}
