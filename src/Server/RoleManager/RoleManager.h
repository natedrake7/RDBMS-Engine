#pragma once
#include "../../Systemic/DataStructures/Dictionary/Dictionary.h"

#include <mutex>
#include <string>

namespace Security {
  struct Role;

  class RoleManager {
    Dictionary<int32_t, Security::Role*> roles;
    Dictionary<std::string, int32_t> rolesNames;

    std::mutex mutex;
    public:
      RoleManager();
      ~RoleManager();

      [[nodiscard]] Role* GetRole(const int32_t& roleId);
      [[nodiscard]] Role* GetRole(const std::string& name);
      [[nodiscard]] bool AddRole(const std::string& name, Role* role);
      [[nodiscard]] bool RemoveRole(const std::string& name);
  };
}
