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
      static constexpr auto DB_OWNER_PERMISSIONS =
          Security::Permission::SELECT
          | Security::Permission::INSERT
          | Security::Permission::UPDATE
          | Security::Permission::DELETE
          | Security::Permission::CREATE
          | Security::Permission::DROP
          | Security::Permission::ALTER;

      static constexpr auto DB_WRITER_PERMISSIONS =
        Security::Permission::INSERT
        | Security::Permission::UPDATE
        | Security::Permission::DELETE
        | Security::Permission::SELECT;

      static constexpr auto DB_READER_PERMISSIONS =
        Security::Permission::SELECT;

      static constexpr auto GUEST_PERMISSIONS =
        Security::Permission::NONE;
  };

}