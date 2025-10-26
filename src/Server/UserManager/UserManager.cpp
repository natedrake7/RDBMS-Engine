#include "UserManager.h"
#include <ranges>
#include <iostream>

#include <sodium.h>

#include "../../Systemic/Security/Security.h"

namespace Security {
  UserManager::UserManager() {
    if (sodium_init() < 0)
      throw std::runtime_error("Failed to initialize libsodium library.");
  }

  UserManager::~UserManager(){
    for (const auto &user : this->users | views::values) {
      delete user;
    }
  }

  User* UserManager::Authenticate(const std::string &name, const std::string &password){
    std::lock_guard<std::mutex> lock(mutex);

    User* user = nullptr;
    if (!this->users.TryGetValue(name, user))
      return nullptr;

    return (crypto_pwhash_str_verify(user->passwordHash.c_str(), password.c_str(), password.size()) == 0)
      ? user
      : nullptr;
  }

  User * UserManager::GetUser(const std::string &name){
    User *user = nullptr;

    std::lock_guard<std::mutex> lock(this->mutex);

    this->users.TryGetValue(name, user);

    return user;
  }

  bool UserManager::HashPassword(const std::string &password, string &outHash){
    char hashed[crypto_pwhash_STRBYTES];

    const auto result = crypto_pwhash_str(
            hashed,
            password.c_str(),
            password.size(),
            crypto_pwhash_OPSLIMIT_INTERACTIVE,
            crypto_pwhash_MEMLIMIT_INTERACTIVE);

    if (result != 0)
      return false;

    outHash = string(hashed, crypto_pwhash_STRBYTES);
    return true;
  }

  bool UserManager::AddUser(
    const int32_t& id,
    const std::string &name,
    const std::string &passwordHash,
    const Security::Role* role
  ){
    std::lock_guard<std::mutex> lock(this->mutex);

    if (this->users.Contains(name)) {
      std::cerr << "Role" << name << " already exists." << std::endl;
      return false;
    }

    this->users.Add(name, new User{
      .id = id,
      .name = name,
      .passwordHash = passwordHash,
      .role = role
    });

    return true;
  }

  bool UserManager::RemoveUser(const std::string &name){
    std::lock_guard<std::mutex> lock(this->mutex);

    User *user = nullptr;
    if (!this->users.TryGetValue(name, user))
      return false;

    this->users.Remove(name);
    delete user;

    return true;
  }
}
