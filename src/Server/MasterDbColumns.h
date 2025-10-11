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

  enum class SysTableStats : uint8_t {
    TableId = 0,
    RowCount = 1,
    AvgRowSize = 2,
    CreatedAt = 3,
    LastModifiedAt = 4,
    LastModifiedBy = 5,
    Version = 6,
    IsDeleted = 7,
    DeletedAt = 8,
  };

  enum class SysColumnStats : uint8_t {
    ColumnId = 0,
    DistinctCount = 1,
    MininimumValue = 2,
    MaxmimumValue = 3,
    NullCount = 4,
    CreatedAt = 5,
    LastModifiedAt = 6,
    LastModifiedBy = 7,
    Version = 8,
    IsDeleted = 9,
    DeletedAt = 10,
  };
}

