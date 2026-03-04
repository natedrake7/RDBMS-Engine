#include "../../include/RoleManager.h"

#include "../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../../Systemic/include/Guards/WriterGuard.h"
#include "../../../Systemic/include/Security/Security.h"

#include <iostream>
#include <ranges>

#include "../../../Systemic/include/DataTypes/DataTypes.h"
#include "../../../Systemic/include/DataTypes/StringView.h"

namespace Security {
  RoleManager::RoleManager() = default;

    RoleManager::~RoleManager(){
        for (const auto &role: this->roles | std::views::values)
            delete role;
    }

  const Role* RoleManager::GetRole(const Int roleId)const{
    Role *role = nullptr;

    MultiThreading::ReaderGuard guard(&this->mutex);

    this->roles.TryGetValue(roleId, role);

    return role;
  }

  const Role* RoleManager::GetRole(const DataTypes::StringView& name)const{
    Role* role = nullptr;

    MultiThreading::ReaderGuard guard(&this->mutex);

    auto roleId = -1;
    this->rolesNames.TryGetValue(name, roleId);
    this->roles.TryGetValue(roleId, role);

    return role;
  }

  bool RoleManager::AddRole(const DataTypes::StringView& name, Role* role){
    MultiThreading::WriterGuard guard(&this->mutex);

    if (this->rolesNames.Contains(name)){
      std::cerr << "Role" << name.Data() << " already exists." << std::endl;
      return false;
    }

    this->rolesNames.Add(name, role->id);
    this->roles.Add(role->id, role);

    return true;
  }

  bool RoleManager::RemoveRole(const DataTypes::StringView& name){
    MultiThreading::WriterGuard guard(&this->mutex);

    Role *role = nullptr;

    Int roleId = -1;
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
