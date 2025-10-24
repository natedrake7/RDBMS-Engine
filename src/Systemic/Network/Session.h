#pragma once
#include "../DataTypes/Guid/Guid.h"
#include "../DataTypes/DateTime/DateTime.h"

namespace Network {
  struct Session {
    DataTypes::Guid sessionId;

    std::string username;

    DataTypes::DateTime createdAt;
    DataTypes::DateTime lastActive;

    int32_t databaseId;

    //add permissions later and session variables etc
    Session() {
      this->sessionId = DataTypes::Guid();
      this->createdAt = DataTypes::DateTime();
      this->lastActive = DataTypes::DateTime();

      //default to masterdb
      this->databaseId = 1;
    }
  };
}