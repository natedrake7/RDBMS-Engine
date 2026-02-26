#pragma once
#include <string>
#include "../../Systemic/include/DataTypes/DataTypes.h"

namespace Security {
  enum class Permission : UnsignedInt {
    NONE                  = 0,
    SELECT                = 1 << 0,
    INSERT                = 1 << 1,
    UPDATE                = 1 << 2,
    DELETE_PERMISSION     = 1 << 3,
    CREATE                = 1 << 4,
    DROP                  = 1 << 5,
    ALTER                 = 1 << 6,
    GRANT                 = 1 << 7,
    MANAGE_USERS          = 1 << 8,
    MANAGE_DB             = 1 << 9,
    ALL                   = 0xFFFFFFFF
  };

  // Bitwise AND
  inline constexpr Permission operator&(const Permission lhs, const Permission rhs) {
    return static_cast<Permission>(
        static_cast<UnsignedInt>(lhs) & static_cast<UnsignedInt>(rhs)
    );
}

  // Bitwise OR
  inline constexpr Permission operator|(const Permission lhs, const Permission rhs) {
    return static_cast<Permission>(
        static_cast<UnsignedInt>(lhs) | static_cast<UnsignedInt>(rhs)
    );
  }

  // Bitwise XOR
  inline constexpr Permission operator^(const Permission lhs, const Permission rhs) {
    return static_cast<Permission>(
        static_cast<UnsignedInt>(lhs) ^ static_cast<UnsignedInt>(rhs)
    );
  }

  // Bitwise NOT
  inline constexpr Permission operator~(const Permission lhs) {
    return static_cast<Permission>(
        ~static_cast<UnsignedInt>(lhs)
    );
  }

  // AND assignment
  inline Permission& operator&=(Permission &lhs, const Permission rhs) {
    lhs = lhs & rhs;
    return lhs;
  }

  // OR assignment
  inline Permission& operator|=(Permission &lhs, const Permission rhs) {
    lhs = lhs | rhs;
    return lhs;
  }

  // XOR assignment
  inline Permission& operator^=(Permission &lhs, const Permission rhs) {
    lhs = lhs ^ rhs;
    return lhs;
  }

  struct Role {
    Int id;

    std::string name;
    Permission permission;

    bool isSystem;

    Role(const Int id, const std::string& name, const Permission permission, const bool isSystem)
      : id(id), name(name), permission(permission), isSystem(isSystem) {}
    Role(const Role& role) {
      this->id = role.id;
      this->name = role.name;
      this->permission = role.permission;
      this->isSystem = role.isSystem;
    }

    Role(Role&& role) noexcept{
        this->id = std::move(role.id);
        this->name = std::move(role.name);
        this->permission = std::move(role.permission);
        this->isSystem = std::move(role.isSystem);
    }

    [[nodiscard]] bool HasPermission(const Permission permissions) const {
      return permissions == Permission::NONE
        || (this->permission & permissions) != Permission::NONE;
    }
  };

  struct User {
    Int id;

    std::string name;
    std::string passwordHash;

    Int roleId;
    const Role* role;

    bool isActive;

    User(){
        this->id = 0;
        this->roleId = 0;
        this->role = nullptr;
        this->isActive = false;
    }

    User(
        const Int id,
        const std::string& name,
        const std::string& passwordHash,
        const Int roleId,
        const Role* role,
        const bool isActive
    ): id(id), name(name), passwordHash(passwordHash), roleId(roleId), role(role), isActive(isActive) {}

    User(User&& other) noexcept{
        this->id = other.id;
        this->name = std::move(other.name);
        this->passwordHash = std::move(other.passwordHash);
        this->roleId = other.roleId;
        this->role = other.role;
        this->isActive = other.isActive;

        other.role = nullptr;
    }
  };
}
