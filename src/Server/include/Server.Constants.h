#pragma once
#include <string_view>

namespace Server {
  class ServerConstants {
    public:
      static constexpr std::string_view DEFAULT_SCHEMA_NAME = "dbo";
      static constexpr std::string_view ADMIN_NAME = "admin";
      static constexpr std::string_view DB_OWNER_NAME = "db_owner";
      static constexpr std::string_view DB_WRITER_NAME = "db_writer";
      static constexpr std::string_view DB_READER_NAME = "db_reader";
      static constexpr std::string_view GUEST_NAME = "guest";

      static constexpr auto ADMIN_PERMISSIONS = Security::Permission::ALL;

      static constexpr auto GUEST_PERMISSIONS =
        Security::Permission::NONE;

      static constexpr Security::Permission DB_READER_PERMISSIONS =
          Security::Permission::SELECT
          | GUEST_PERMISSIONS;

      static constexpr Security::Permission DB_WRITER_PERMISSIONS =
          Security::Permission::INSERT
          | Security::Permission::UPDATE
          | Security::Permission::DELETE_PERMISSION
          | DB_READER_PERMISSIONS;

      static constexpr Security::Permission DB_OWNER_PERMISSIONS =
          DB_WRITER_PERMISSIONS
          | Security::Permission::CREATE
          | Security::Permission::DROP
          | Security::Permission::ALTER;

      static constexpr int INVALID_FILE_DESCRIPTOR = -1;
      static constexpr int MAX_CONNECTIONS = 10;

  };

}