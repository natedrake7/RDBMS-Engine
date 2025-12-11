#pragma once
#include <cstdint>
#include <string>

namespace Security {
  enum class Permission : uint32_t {
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
  inline constexpr Permission operator&(const Permission& lhs, const Permission& rhs) {
    return static_cast<Permission>(
        static_cast<uint32_t>(lhs) & static_cast<uint32_t>(rhs)
    );
}

  // Bitwise OR
  inline constexpr Permission operator|(const Permission& lhs, const Permission& rhs) {
    return static_cast<Permission>(
        static_cast<uint32_t>(lhs) | static_cast<uint32_t>(rhs)
    );
  }

  // Bitwise XOR
  inline constexpr Permission operator^(const Permission& lhs, const Permission& rhs) {
    return static_cast<Permission>(
        static_cast<uint32_t>(lhs) ^ static_cast<uint32_t>(rhs)
    );
  }

  // Bitwise NOT
  inline constexpr Permission operator~(const Permission& lhs) {
    return static_cast<Permission>(
        ~static_cast<uint32_t>(lhs)
    );
  }

  // AND assignment
  inline Permission& operator&=(Permission &lhs, const Permission& rhs) {
    lhs = lhs & rhs;
    return lhs;
  }

  // OR assignment
  inline Permission& operator|=(Permission &lhs, const Permission& rhs) {
    lhs = lhs | rhs;
    return lhs;
  }

  // XOR assignment
  inline Permission& operator^=(Permission &lhs, const Permission& rhs) {
    lhs = lhs ^ rhs;
    return lhs;
  }

  struct Role {
    int32_t id;

    std::string name;
    Permission permission;

    bool isSystem;

    Role(const int32_t& id, const std::string& name, const Permission& permission, const bool& isSystem)
      : id(id), name(name), permission(permission), isSystem(isSystem) {}
    Role(const Role& role) {
      id = role.id;
      name = role.name;
      permission = role.permission;
      isSystem = role.isSystem;
    }

    [[nodiscard]] bool HasPermission(const Permission& permissions) const {
      return permissions == Permission::NONE
        || (this->permission & permissions) != Permission::NONE;
    }
  };

  struct User {
    int32_t id;

    std::string name;
    std::string passwordHash;

    int32_t roleId;
    const Role* role;

    bool isActive;
  };
}