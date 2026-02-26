#pragma once
#include <cstdint>

namespace DatabaseEngine {
  enum CatalogTables: UnsignedTinyInt {
    SysDatabases = 0,
    SysSchemas = 1,
    SysTables = 2,
    SysColumns = 3,
    SysIndexes = 4,
    SysIdentityColumns = 5,
    SysIndexColumns = 6,
    SysConstraints = 7,
    SysConstraintColumns = 8,
    SysDefaultValues = 9,
    SysTableStats = 10,
    SysColumnStats = 11,
    SysColumnHistograms = 12,
    SysRoles = 13,
    SysUsers = 14,
    SysIndexStats = 15,
  };

  enum class SysDatabases : UnsignedTinyInt {
    DatabaseId = 0,
    Name = 1,
    FilePath = 2,
    IsSystem = 3,
    CreatedAt = 4,
    LastModifiedAt = 5,
    LastModifiedBy = 6,
    Version = 7,
    IsDeleted = 8,
    DeletedAt = 9,
  };

  enum class SysSchemas : UnsignedTinyInt {
    DatabaseId = 0,
    SchemaId = 1,
    Name = 2,
    CreatedAt = 3,
    LastModifiedAt = 4,
    LastModifiedBy = 5,
    Version = 6,
    IsDeleted = 7,
    DeletedAt = 8,
  };

  enum class SysTables : UnsignedTinyInt {
    DatabaseId = 0,
    TableId = 1,
    SchemaId = 2,
    Name = 3,
    OrdinalPosition = 4,
    IsSystemTable = 5,
    CreatedAt = 6,
    LastModifiedAt = 7,
    LastModifiedBy = 8,
    Version = 9,
    IsDeleted = 10,
    DeletedAt = 11,
  };

  enum class SysColumns : UnsignedTinyInt {
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

  enum class SysIndexes : UnsignedTinyInt {
    TableId = 0,
    IndexId = 1,
    Name = 2,
    IsClustered = 3,
    IsDisabled = 4,
    CreatedAt = 5,
    LastModifiedAt = 6,
    LastModifiedBy = 7,
    Version = 8,
    IsDeleted = 9,
    DeletedAt = 10,
  };

  enum class SysIndexColumns : UnsignedTinyInt {
    IndexId = 0,
    ColumnId = 1,
    OrdinalPosition = 2,
    IsIncluded = 3,
    Version = 4,
    IsDeleted = 5,
    DeletedAt = 6,
  };

  enum class SysConstraints : UnsignedTinyInt {
    TableId = 0,
    ConstraintId = 1,
    Name = 2,
    Type = 3,
    IsDisabled = 4,
    IndexId = 5,
    CreatedAt = 6,
    LastModifiedAt = 7,
    LastModifiedBy = 8,
    Version = 9,
    IsDeleted = 10,
    DeletedAt = 11,
  };

  enum class SysConstraintColumns : UnsignedTinyInt {
    ConstraintId = 0,
    ColumnId = 1,
    OrdinalPosition = 2,
    Version = 3,
    IsDeleted = 4,
    DeletedAt = 5,
  };

  enum class SysIdentityColumns : UnsignedTinyInt {
    TableId = 0,
    ColumnId = 1,
    SeedValue = 2,
    IncrementValue = 3,
    LastValue = 4,
    IsCached = 5,
    CacheBlock = 6,
    Version = 7,
    IsDeleted = 8,
    DeletedAt = 9,
  };

  enum class SysDefaultValues : UnsignedTinyInt {
    ColumnId = 0,
    Value = 1,
    Version = 2,
    IsDeleted = 3,
    DeletedAt = 4,
  };

  enum class SysTableStats : UnsignedTinyInt {
    TableId = 0,
    RowCount = 1,
    AvgRowSize = 2,
    PageCount = 3,
    LastUpdatedAt = 4
  };

  enum class SysColumnStats : UnsignedTinyInt {
    ColumnId = 0,
    DistinctCount = 1,
    MinimumValue = 2,
    MaximumValue = 3,
    NullCount = 4
  };

  enum class SysColumnHistograms : UnsignedTinyInt {
    ColumnId = 0,
    HistogramId = 1,
    RangeStart = 2,
    RangeEnd = 3,
    RowCount = 4,
    DistinctCount = 5,
  };

  enum class SysRoles : UnsignedTinyInt {
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

  enum class SysUsers : UnsignedTinyInt {
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

  enum class SysIndexStats : UnsignedTinyInt {
    TableId = 0,
    IndexId = 1,
    LeafPages = 2,
    Depth = 3,
    AverageFragmentation = 4,
    LastUpdated = 5
  };
}

