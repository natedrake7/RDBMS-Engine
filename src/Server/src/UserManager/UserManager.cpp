#include "../../include/UserManager.h"

#include "../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../../Systemic/include/Guards/WriterGuard.h"
#include "../../../Systemic/include/Security/Security.h"

#include <iostream>

#include <argon2.h>

#include "../../../DatabaseEngine/include/Database.h"
#include "../../../DatabaseEngine/include/Memory/PersistentAllocator.h"
#include "../../../Systemic/include/DataTypes/DataTypes.h"

namespace Security {
    UserManager::UserManager() =  default;

    UserManager::~UserManager(){
        this->_allocator.Reset();
    }

    User* UserManager::Authenticate(const DataTypes::StringView& name, const DataTypes::StringView& password)const{
        MultiThreading::ReaderGuard guard(&this->mutex);
        User* user = nullptr;

        if (!this->users.TryGetValue(name, user)) return nullptr;

        char hash[128];
        std::memcpy(hash, user->passwordHash.Data(), user->passwordHash.Size());

        return (argon2id_verify(hash, password.Data(), password.Size()) == Argon2_ErrorCodes::ARGON2_OK)
            ? user
            : nullptr;
    }

    const User * UserManager::GetUser(const DataTypes::StringView& name)const{
        User *user = nullptr;
        MultiThreading::ReaderGuard guard(&this->mutex);
        this->users.TryGetValue(name, user);
        return user;
    }

    bool UserManager::HashPassword(const DataTypes::StringView& password, DataTypes::String& outHash){
        static constexpr UnsignedInt t_cost = 3;        // iterations
        static constexpr UnsignedInt m_cost = 1 << 16;  // 64 MiB
        static constexpr UnsignedInt parallelism = 1;

        static constexpr auto HASH_SALT = DataTypes::StringView("kalispera");

        char hash[128];
        const int result = argon2id_hash_encoded(
            t_cost,
            m_cost,
            parallelism,
            password.Data(),
            password.Size(),
            HASH_SALT.Data(),
            HASH_SALT.Size(),
            32,
            hash,
            sizeof(hash)
        );

        if (result != Argon2_ErrorCodes::ARGON2_OK) return false;
        outHash.Append(hash);
        return true;
    }

    bool UserManager::AddUser(
        const Int id,
        const DataTypes::String& name,
        const DataTypes::String& passwordHash,
        const Role* role
    ){
        MultiThreading::WriterGuard guard(&this->mutex);

        if (this->users.Contains(name.ToView())) {
            std::cerr << "Role" << name << " already exists." << std::endl;
            return false;
        }

        auto hash = DataTypes::String(passwordHash, &this->_allocator);
        auto username = DataTypes::String(name, &this->_allocator);

        auto* user = _allocator.Allocate<User>(
            id,
            std::move(username),
            std::move(hash),
            role->id,
            role,
            true
        );

        this->users.Add(user->name.ToView(), user);
        return true;
    }
    //
    // void UserManager::AddUser(User* user){
    //     MultiThreading::WriterGuard guard(&this->mutex);
    //     this->users.Add(user->name.ToView(), user);
    // }

    // bool UserManager::AddSystemUser(User *user) {
    //     MultiThreading::WriterGuard guard(&this->mutex);
    //     this->users.Add(user->name.ToView(), user);
    //     return true;
    // }

    bool UserManager::RemoveUser(const DataTypes::StringView& name){
        MultiThreading::WriterGuard guard(&this->mutex);

        if (!this->users.Contains(name)) return false;
        this->users.Remove(name);

        return true;
    }

    bool UserManager::GrantRole(
        const DataTypes::StringView& name,
        const Role *role,
        Int& outUserId
    )const{
        MultiThreading::WriterGuard guard(&this->mutex);

        User *user = nullptr;
        if (!this->users.TryGetValue(name, user)) return false;

        user->roleId = role->id;
        user->role = role;

        outUserId = user->id;

        return true;
    }
}
