#pragma once
#include "../../Systemic/DataStructures/Dictionary/Dictionary.h"
#include "../../Systemic/MultiThreading/ReadWriteMutex/ReadWriteMutex.h"

#include <string>

namespace Security {
  struct Role;
  struct User;

  class UserManager {
    Dictionary<std::string, User*> users;
    mutable MultiThreading::ReadWriteMutex mutex;

    public:
      UserManager();
      ~UserManager();

      [[nodiscard]] User* Authenticate(const std::string& name, const std::string& password)const;
      [[nodiscard]] const User* GetUser(const std::string& name)const;
      [[nodiscard]] bool AddUser(
        const int32_t& id,
        const std::string &name,
        const std::string &passwordHash,
        const Security::Role* role
      );
      [[nodiscard]]bool RemoveUser(const std::string& name);

      bool GrantRole(const std::string& name, const Security::Role* role, int32_t& outUserId)const;

      static bool HashPassword(const std::string& password, string& outHash);
  };
}
