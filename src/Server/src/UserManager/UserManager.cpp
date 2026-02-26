#include "../../include/UserManager.h"

#include "../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../../Systemic/include/Guards/WriterGuard.h"
#include "../../../Systemic/include/Security/Security.h"

#include <ranges>
#include <iostream>

#include <argon2.h>

#include "../../../Systemic/include/DataTypes/DataTypes.h"

namespace Security {
  UserManager::UserManager() =  default;

  // UserManager::UserManager() {
  //   if (sodium_init() < 0)
  //     throw std::runtime_error("Failed to initialize libsodium library.");
  // }

  UserManager::~UserManager(){
    for (const auto &user : this->users | std::views::values) {
      delete user;
    }
  }

  User* UserManager::Authenticate(const std::string &name, const std::string &password)const{
    MultiThreading::ReaderGuard guard(&this->mutex);

    User* user = nullptr;
    if (!this->users.TryGetValue(name, user))
      return nullptr;

    return (argon2id_verify(user->passwordHash.c_str(), password.c_str(), password.length()) == Argon2_ErrorCodes::ARGON2_OK)
      ? user
      : nullptr;
    // return (crypto_pwhash_str_verify(user->passwordHash.c_str(), password.c_str(), password.length()) == 0)
    //     ? user
    //     : nullptr;
  }

  const User * UserManager::GetUser(const std::string &name)const{
    User *user = nullptr;

    MultiThreading::ReaderGuard guard(&this->mutex);

    this->users.TryGetValue(name, user);

    return user;
  }

  bool UserManager::HashPassword(const std::string &password, std::string &outHash){
    // char hashed[crypto_pwhash_STRBYTES];
    //
    // const auto result = crypto_pwhash_str(
    //         hashed,
    //         password.c_str(),
    //         password.size(),
    //         crypto_pwhash_OPSLIMIT_INTERACTIVE,
    //         crypto_pwhash_MEMLIMIT_INTERACTIVE);
    //
    // if (result != 0)
    //   return false;
    //
    // outHash = string(hashed, crypto_pwhash_STRBYTES);
    // return true;

   constexpr uint32_t t_cost = 3;        // iterations
   constexpr uint32_t m_cost = 1 << 16;  // 64 MiB
   constexpr uint32_t parallelism = 1;

    const std::string salt = "kalispera";

    char hash[128];
    const int result = argon2id_hash_encoded(
        t_cost,
        m_cost,
        parallelism,
        password.data(),
        password.size(),
        salt.data(),
      salt.size(),
        32,  // output length in bytes
        hash,
        sizeof(hash)
    );

    if (result != Argon2_ErrorCodes::ARGON2_OK)
      return false;

    outHash = std::string(hash);
    return true;
  }

  bool UserManager::AddUser(
    const Int id,
    const std::string &name,
    const std::string &passwordHash,
    const Role* role
  ){
    MultiThreading::WriterGuard guard(&this->mutex);

    if (this->users.Contains(name)) {
      std::cerr << "Role" << name << " already exists." << std::endl;
      return false;
    }

    this->users.Add(name, new User(
      id,
      name,
      passwordHash,
      role->id,
      role,
      true
    ));

    return true;
  }

  void UserManager::AddUser(User* user){
      MultiThreading::WriterGuard guard(&this->mutex);
      this->users.Add(user->name, user);
  }

  bool UserManager::AddSystemUser(User *user) {
    MultiThreading::WriterGuard guard(&this->mutex);

    this->users.Add(user->name, user);

    return true;
  }

  bool UserManager::RemoveUser(const std::string &name){
    MultiThreading::WriterGuard guard(&this->mutex);

    User *user = nullptr;
    if (!this->users.TryGetValue(name, user))
      return false;

    this->users.Remove(name);
    delete user;

    return true;
  }

  bool UserManager::GrantRole(
    const std::string &name,
    const Role *role,
    Int& outUserId
  )const{
    MultiThreading::WriterGuard guard(&this->mutex);

    User *user = nullptr;
    if (!this->users.TryGetValue(name, user))
      return false;

    user->roleId = role->id;
    user->role = role;

    outUserId = user->id;

    return true;
  }
}
