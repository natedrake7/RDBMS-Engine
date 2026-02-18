#pragma once
#include "../../Systemic/include/DataStructures/Dictionary.h"
#include "../../Systemic/include/Guards/ReadWriteMutex.h"
#include "../../Systemic/include/DataTypes/DataTypes.h"

#include <string>

namespace Security {
    struct Role;

    class RoleManager {
    Dictionary<Int, Role*> roles;
    Dictionary<std::string, Int> rolesNames;

    mutable MultiThreading::ReadWriteMutex mutex;
    public:
      RoleManager();
      ~RoleManager();

      [[nodiscard]] const Role* GetRole(Int roleId)const;
      [[nodiscard]] const Role* GetRole(const std::string& name)const;
      [[nodiscard]] bool AddRole(const std::string& name, Role* role);
      [[nodiscard]] bool RemoveRole(const std::string& name);
  };
}
