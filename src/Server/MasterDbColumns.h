#pragma once
#include <cstdint>


namespace Server {
  enum class SysColumns : uint16_t {
    ColumnId = 0,
    TableId = 1,
    Name = 2,
    DataType = 3,
    RecordSize = 4,
    IsNullable = 5,
    OrdinalPosition = 6,
    IsSystemColumn = 7,
    CreatedAt = 8,
    LastModifiedAt = 9,
    LastModifiedBy = 10,
    Version = 11,
    IsDeleted = 12,
    DeletedAt = 13,
  };
}

