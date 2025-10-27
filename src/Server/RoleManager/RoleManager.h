#pragma once
#include "../../Systemic/DataStructures/Dictionary/Dictionary.h"
#include "../../Systemic/MultiThreading/ReadWriteMutex/ReadWriteMutex.h"

#include <string>

namespace Security {
  struct Role;

  class RoleManager {
    Dictionary<int32_t, Security::Role*> roles;
    Dictionary<std::string, int32_t> rolesNames;

    mutable MultiThreading::ReadWriteMutex mutex;
    public:
      RoleManager();
      ~RoleManager();

      [[nodiscard]] const Role* GetRole(const int32_t& roleId)const;
      [[nodiscard]] const Role* GetRole(const std::string& name)const;
      [[nodiscard]] bool AddRole(const std::string& name, Role* role);
      [[nodiscard]] bool RemoveRole(const std::string& name);
  };
}
