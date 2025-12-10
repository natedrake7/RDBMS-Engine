#pragma once
#include <cstdint>


namespace Server {
  enum class SysColumns : uint16_t {
    TableId = 0,
    ColumnId = 1,
    Name = 2,
    DataType = 3,
    RecordSize = 4,
    Precision = 5,
    Scale = 6,
    IsNullable = 7,
    OrdinalPosition = 8,
    IsSystemColumn = 9,
    CreatedAt = 10,
    LastModifiedAt = 11,
    LastModifiedBy = 12,
    Version = 13,
    IsDeleted = 14,
    DeletedAt = 15,
  };

  enum class SysTableStats : uint8_t {
    TableId = 0,
    RowCount = 1,
    AvgRowSize = 2
  };

  enum class SysColumnStats : uint8_t {
    ColumnId = 0,
    DistinctCount = 1,
    MinimumValue = 2,
    MaximumValue = 3,
    NullCount = 4
  };

  enum class SysColumnHistograms : uint8_t {
    ColumnId = 0,
    HistogramId = 1,
    RangeStart = 2,
    RangeEnd = 3,
    RowCount = 4,
    DistinctCount = 5,
  };

  enum class SysRoles : uint8_t {
    RoleId = 0,
    RoleName = 1,
    Permissions = 2,
    IsSystemRole = 3,
    CreatedAt = 4,
    LastModifiedAt = 5,
    LastModifiedBy = 6,
    Version = 7,
    IsDeleted = 8,
    DeletedAt = 9,
  };

  enum class SysUsers : uint8_t {
    UserId = 0,
    UserName = 1,
    PasswordHash = 2,
    RoleId = 3,
    IsActive = 4,
    CreatedAt = 5,
    LastModifiedAt = 6,
    LastModifiedBy = 7,
    Version = 8,
    IsDeleted = 9,
    DeletedAt = 10,
  };
}

