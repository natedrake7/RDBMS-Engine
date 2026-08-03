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
        this->_allocator.Release();
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

        Int roleId = -1;
        this->rolesNames.TryGetValue(name, roleId);
        this->roles.TryGetValue(roleId, role);

        return role;
    }

    bool RoleManager::AddRole(const DataTypes::StringView& name, const Role* role){
        MultiThreading::WriterGuard guard(&this->mutex);

        if (this->rolesNames.Contains(name)){
            std::cerr << "Role" << name << " already exists." << std::endl;
            return false;
        }

        //copy the role using the local allocator
        auto roleName = DataTypes::String(role->name, &this->_allocator);
        auto* newRole = this->_allocator.Allocate<Role>(
            role->id,
            roleName,
            role->permission,
            role->isSystem
        );

        //the key should be a view to the copied role string to ensure
        //it remains valid as long as the role exists
        this->rolesNames.Add(DataTypes::StringView::ViewOf(newRole->name), role->id);
        this->roles.Add(role->id, newRole);

        return true;
    }

    bool RoleManager::RemoveRole(const DataTypes::StringView& name){
        MultiThreading::WriterGuard guard(&this->mutex);

        Int roleId = -1;
        if (!this->rolesNames.TryGetValue(name, roleId))
            return false;

        Role *role = nullptr;
        if (
            !this->roles.TryGetValue(roleId, role)
            || role->isSystem
        ) return false;

        this->rolesNames.Remove(name);
        this->roles.Remove(roleId);

        return true;
    }
}
