#pragma once
#include "../../Systemic/DataStructures/Dictionary/Dictionary.h"

#include <mutex>
#include <string>

namespace Security {
  struct Role;
  struct User;

  class UserManager {
    Dictionary<std::string, User*> users;
    std::mutex mutex;

    public:
      UserManager();
      ~UserManager();

      [[nodiscard]]User* Authenticate(const std::string& name, const std::string& password);
      [[nodiscard]]User* GetUser(const std::string& name);
      [[nodiscard]]bool AddUser(
        const int32_t& id,
        const std::string &name,
        const std::string &passwordHash,
        const Security::Role* role
      );
      [[nodiscard]]bool RemoveUser(const std::string& name);

      static bool HashPassword(const std::string& password, string& outHash);
  };
}
