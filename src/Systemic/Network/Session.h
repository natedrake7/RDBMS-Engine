#pragma once
#include "../DataTypes/Guid/Guid.h"
#include "../DataTypes/DateTime/DateTime.h"
#include "../Security/Security.h"

namespace Network {
  struct Session {
    DataTypes::Guid sessionId;

    const Security::User* user;

    DataTypes::DateTime createdAt;
    DataTypes::DateTime lastActive;

    int32_t databaseId;

    QueryPipeline::Cursor* cursor;

    //add permissions later and session variables etc
    explicit Session(const Security::User* user) {
      this->sessionId = DataTypes::Guid();
      this->createdAt = DataTypes::DateTime();
      this->lastActive = DataTypes::DateTime();

      this->cursor = nullptr;

      this->user = user;

      //default to masterdb
      this->databaseId = 1;
    }
  };
}