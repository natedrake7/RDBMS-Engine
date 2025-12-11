#pragma once
#include "../DataStructures/SortedDictionary.h"
#include "../DataTypes/Guid.h"
#include "../DataTypes/DateTime.h"
#include "../DataTypes/Variable.h"
#include "Security.h"

namespace QueryPipeline {
  class Cursor;
}

namespace Network {

  struct Session {
    DataTypes::Guid sessionId;

    const Security::User* user;

    DataTypes::DateTime createdAt;
    DataTypes::DateTime lastActive;

    int32_t databaseId;

    SortedDictionary<uint16_t, QueryPipeline::Cursor*> cursors;
    Dictionary<std::string, Variable> variables;

    uint16_t nextCursorId;

    transaction_id_t transactionId;

    //add permissions later and session variables etc
    explicit Session(const Security::User* user) {
      this->sessionId = DataTypes::Guid::NewGuid();
      this->createdAt = DataTypes::DateTime();
      this->lastActive = DataTypes::DateTime();
      this->nextCursorId = 0;

      this->user = user;

      //default to masterdb
      this->databaseId = 1;
      this->transactionId = 0;
    }
  };
}