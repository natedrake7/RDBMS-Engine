#pragma once
#include "../../Systemic/include/DataStructures/Dictionary.h"
#include "../../Systemic/include/Guards/ReadWriteMutex.h"

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
      [[nodiscard]] bool AddSystemUser(User* user);
      [[nodiscard]]bool RemoveUser(const std::string& name);

      bool GrantRole(const std::string& name, const Security::Role* role, int32_t& outUserId)const;

      static bool HashPassword(const std::string& password, std::string& outHash);
  };
}
