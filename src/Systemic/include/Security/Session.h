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

    Int databaseId;

    SortedDictionary<UnsignedSmallInt, QueryPipeline::Cursor*> cursors;
    Dictionary<DataTypes::String, Variable> variables;

    UnsignedSmallInt nextCursorId;

    transaction_id_t transactionId;

    //add permissions later and session variables etc
    explicit Session(const Security::User* user) {
      this->sessionId = DataTypes::Guid::NewGuid();
      this->createdAt = DataTypes::DateTime();
      this->lastActive = DataTypes::DateTime();
      this->nextCursorId = 0;

      this->user = user;

      //default to masterdb
      this->databaseId = Constants::SYSTEM_CATALOG_ID;
      this->transactionId = 0;
    }
  };
}