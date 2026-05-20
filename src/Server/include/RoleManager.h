#pragma once
#include "../../CoreEngine/include/Memory/PersistentAllocator.h"
#include "../../Systemic/include/DataStructures/Dictionary.h"
#include "../../Systemic/include/Guards/Mutex.h"
#include "../../Systemic/include/DataTypes/DataTypes.h"
#include "../../Systemic/include/DataTypes/StringView.h"

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
