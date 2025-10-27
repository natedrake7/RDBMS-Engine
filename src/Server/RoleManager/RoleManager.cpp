#include "RoleManager.h"

#include "../../Systemic/MultiThreading/Guards/ReaderGuard/ReaderGuard.h"
#include "../../Systemic/MultiThreading/Guards/WriterGuard/WriterGuard.h"
#include "../../Systemic/Security/Security.h"

#include <iostream>
#include <ranges>

namespace Security {
  RoleManager::RoleManager() = default;

  RoleManager::~RoleManager(){
   for (const auto &role: this->roles | views::values)
     delete role;
  }

  const Role * RoleManager::GetRole(const int32_t &roleId)const{
    Role *role = nullptr;

    MultiThreading::ReaderGuard guard(&this->mutex);

    this->roles.TryGetValue(roleId, role);

    return role;
  }

  const Role* RoleManager::GetRole(const std::string &name)const{
    Role *role = nullptr;

    MultiThreading::ReaderGuard guard(&this->mutex);

    int roleId = -1;
    this->rolesNames.TryGetValue(name, roleId);
    this->roles.TryGetValue(roleId, role);

    return role;
  }

  bool RoleManager::AddRole(const std::string &name, Role *role){
    MultiThreading::WriterGuard guard(&this->mutex);

    if (this->rolesNames.Contains(name)){
      std::cerr << "Role" << name << " already exists." << std::endl;
      return false;
    }

    this->rolesNames.Add(name, role->id);
    this->roles.Add(role->id, role);

    return true;
  }

  bool RoleManager::RemoveRole(const std::string &name){
    MultiThreading::WriterGuard guard(&this->mutex);

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
