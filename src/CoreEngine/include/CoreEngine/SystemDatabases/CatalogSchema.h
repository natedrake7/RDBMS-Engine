#pragma once
#include <iterator>
#include <span>
#include <string_view>

#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataTypes/DataTypes.StaticData.h>
#include <Systemic/Reflection/Enum.h>
#include <CoreEngine/DatabaseConstants.h>

namespace Memory{
    class IAllocator;
}

namespace CoreEngine::Catalog{
    struct TableDefinition;
}

// Ids, positions and description types of the system catalog's own tables. Their layout is defined by the
// row structs in CatalogRows.h: they are never rebuilt from catalog rows, the rows describing them only serve
// introspection. Any change to a row struct (a column, a type, a key, the table order) must bump CATALOG_VERSION.
namespace CoreEngine {
    inline constexpr UnsignedInt CATALOG_VERSION = 1;

    /**
     * @name Reserved ids
     * System objects get fixed ids below FIRST_USER_OBJECT_ID, so the in-memory schema, the catalog rows
     * describing it and every restart agree without any mapping (PostgreSQL's FirstNormalObjectId).
     * User schemas, tables, columns, indexes and constraints are numbered from FIRST_USER_OBJECT_ID.
     * @{
     */

    inline constexpr Int SYSTEM_SCHEMA_ID = 1;
    inline constexpr Int FIRST_USER_OBJECT_ID = 10'000;

    // System tables have fewer than SYSTEM_COLUMN_ID_STRIDE columns (checked in SystemTablesAreValid)
    inline constexpr Int SYSTEM_COLUMN_ID_STRIDE = 100;

    [[nodiscard]] constexpr Int SystemColumnId(const table_id_t tableId, const column_index_t ordinal){
        return static_cast<Int>(tableId) * SYSTEM_COLUMN_ID_STRIDE + ordinal;
    }

    /** @} End of: Reserved ids */

    /**
     * @name Table and column positions
     * Used by the catalog accessors to read and write rows by position.
     * @{
     */

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

    /** @} End of: Table and column positions */

    /**
     * @name System table definitions
     * @{
     */

    struct SystemColumn{
        DataTypes::StringView name;
        DataType type;
        block_size_t size = 0;                                   // used only when the type has no fixed size (FixedColumnSize == 0)
        bool nullable = false;
        bool identity = false;
        BigInt identityStart = Constants::DEFAULT_IDENTITY_VALUE; // first value the identity hands out in a freshly created catalog
        DataTypes::StringView defaultValue{};                         // SQL text, empty = no default
    };

    struct SystemTable{
        CatalogTables ordinal;
        table_id_t id;
        DataTypes::StringView name;
        std::span<const SystemColumn> columns;
        std::span<const column_index_t> primaryKey;              // column ordinals, in key order
    };

    template <typename TColumn>
    [[nodiscard]] constexpr column_index_t ColumnOf(const TColumn column){
        return static_cast<column_index_t>(column);
    }

    // The definitions themselves (SYSTEM_TABLES, SystemTableOf) are generated from the row structs:
    // see CatalogRows.h.

    /** @} End of: System table definitions */

    // SystemTable -> the same TableDefinition user tables are built from (headers allocated in `scratch`)
    [[nodiscard]] Catalog::TableDefinition SystemTableDefinition(
        const ::Memory::IAllocator* allocator,
        const SystemTable& table
    );
}
