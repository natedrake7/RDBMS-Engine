#pragma once
#include "../../CoreEngine/include/Memory/PersistentAllocator.h"
#include "../../Systemic/include/DataStructures/Dictionary.h"
#include "../../Systemic/include/DataTypes/DataTypes.h"
#include "../../Systemic/include/Guards/ReadWriteMutex.h"
#include "../../Systemic/include/DataTypes/StringView.h"

namespace DataTypes{
    class String;
}

namespace Security {
    struct Role;
    struct User;

    class UserManager {
        const CoreEngine::Memory::PersistentAllocator _allocator;

        Dictionary<DataTypes::StringView, User*> users;
        mutable MultiThreading::ReadWriteMutex mutex;

    public:
        UserManager();
        ~UserManager();

        [[nodiscard]] User* Authenticate(const DataTypes::StringView& name, const DataTypes::StringView& password)const;
        [[nodiscard]] const User* GetUser(const DataTypes::StringView& name)const;
        [[nodiscard]] bool AddUser(
            Int id,
            const DataTypes::String& name,
            const DataTypes::String& passwordHash,
            const Role* role
        );
        // void AddUser(User* user);
        // [[nodiscard]] bool AddSystemUser(User* user);
        [[nodiscard]]bool RemoveUser(const DataTypes::StringView& name);

        bool GrantRole(const DataTypes::StringView& name, const Role* role, Int& outUserId)const;

        static bool HashPassword(const DataTypes::StringView& password, DataTypes::String& outHash);
    };
}
