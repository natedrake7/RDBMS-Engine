#pragma once
#include <CoreEngine/Memory/PersistentAllocator.h>
#include <Systemic/DataStructures/Dictionary.h>
#include <Systemic/Guards/Mutex.h>
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataTypes/StringView.h>

namespace Security {
    struct Role;

    class RoleManager {
        Dictionary<Int, Role*> roles;
        Dictionary<DataTypes::StringView, Int> rolesNames;
        mutable MultiThreading::Mutex mutex;

        const CoreEngine::Memory::PersistentAllocator _allocator;

        public:
          RoleManager();
          ~RoleManager();

          [[nodiscard]] const Role* GetRole(Int roleId)const;
          [[nodiscard]] const Role* GetRole(const DataTypes::StringView& name)const;
          [[nodiscard]] bool AddRole(const DataTypes::StringView& name, const Role* role);
          [[nodiscard]] bool RemoveRole(const DataTypes::StringView& name);
  };
}
