#include "RoleManager.h"

#include "../../Systemic/Security/Security.h"

#include <iostream>
#include <ranges>

namespace Security {
  RoleManager::RoleManager() = default;

  RoleManager::~RoleManager(){
   for (const auto &role: this->roles | views::values)
     delete role;
  }

  Role * RoleManager::GetRole(const int32_t &roleId){
    Role *role = nullptr;

    std::lock_guard<std::mutex> lock(this->mutex);

    this->roles.TryGetValue(roleId, role);

    return role;
  }

  Role* RoleManager::GetRole(const std::string &name){
    Role *role = nullptr;

    std::lock_guard<std::mutex> lock(this->mutex);

    int roleId = -1;
    this->rolesNames.TryGetValue(name, roleId);
    this->roles.TryGetValue(roleId, role);

    return role;
  }

  bool RoleManager::AddRole(const std::string &name, Role *role){
    std::lock_guard<std::mutex> lock(this->mutex);

    if (this->rolesNames.Contains(name)){
      std::cerr << "Role" << name << " already exists." << std::endl;
      return false;
    }

    this->rolesNames.Add(name, role->id);
    this->roles.Add(role->id, role);

    return true;
  }

  bool RoleManager::RemoveRole(const std::string &name){
    std::lock_guard<std::mutex> lock(this->mutex);

    Role *role = nullptr;

    int32_t roleId = -1;
    if (!this->rolesNames.TryGetValue(name, roleId))
      return false;

    if (!this->roles.TryGetValue(roleId, role)
      || role->isSystem)
      return false;

    this->rolesNames.Remove(name);
    this->roles.Remove(roleId);
    delete role;

    return true;
  }


}
