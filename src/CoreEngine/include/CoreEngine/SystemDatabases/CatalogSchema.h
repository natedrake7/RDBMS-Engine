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

// Definition of the system catalog's own tables. This is the source of truth for their layout:
// they are never rebuilt from catalog rows, the rows describing them only serve introspection.
// Any change here (a column, a type, a key, the table order) must bump CATALOG_VERSION.
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

    namespace SystemTableDefinitions{
        // ---- sys_databases
        inline constexpr SystemColumn SYS_DATABASES_COLUMNS[] = {
            { .name = "database_id",      .type = DataType::Int,      .identity = true, .identityStart = Constants::SYSTEM_CATALOG_ID },
            { .name = "name",             .type = DataType::String,   .size = 100 },
            { .name = "filepath",         .type = DataType::String,   .size = 100 },
            { .name = "is_system",        .type = DataType::Bool },
            { .name = "created_at",       .type = DataType::DateTime },
            { .name = "last_modified",    .type = DataType::DateTime },
            { .name = "last_modified_by", .type = DataType::String,   .size = 100 },
            { .name = "version",          .type = DataType::Int,      .defaultValue = "1" },
            { .name = "is_deleted",       .type = DataType::Bool,     .defaultValue = "0" },
            { .name = "deleted_at",       .type = DataType::DateTime, .nullable = true },
        };
        inline constexpr column_index_t SYS_DATABASES_KEY[] = {
            ColumnOf(SysDatabases::DatabaseId)
        };

        // ---- sys_schemas
        inline constexpr SystemColumn SYS_SCHEMAS_COLUMNS[] = {
            { .name = "database_id",      .type = DataType::Int },
            { .name = "schema_id",        .type = DataType::Int,      .identity = true, .identityStart = FIRST_USER_OBJECT_ID },
            { .name = "name",             .type = DataType::String,   .size = 100 },
            { .name = "created_at",       .type = DataType::DateTime },
            { .name = "last_modified",    .type = DataType::DateTime },
            { .name = "last_modified_by", .type = DataType::String,   .size = 100 },
            { .name = "version",          .type = DataType::Int,      .defaultValue = "1" },
            { .name = "is_deleted",       .type = DataType::Bool,     .defaultValue = "0" },
            { .name = "deleted_at",       .type = DataType::DateTime, .nullable = true },
        };
        inline constexpr column_index_t SYS_SCHEMAS_KEY[] = {
            ColumnOf(SysSchemas::DatabaseId),
            ColumnOf(SysSchemas::SchemaId)
        };

        // ---- sys_tables
        inline constexpr SystemColumn SYS_TABLES_COLUMNS[] = {
            { .name = "database_id",      .type = DataType::Int },
            { .name = "table_id",         .type = DataType::Int,      .identity = true, .identityStart = FIRST_USER_OBJECT_ID },
            { .name = "schema_id",        .type = DataType::Int },
            { .name = "name",             .type = DataType::String,   .size = 100 },
            { .name = "ordinal_position", .type = DataType::SmallInt },
            { .name = "is_system",        .type = DataType::Bool },
            { .name = "created_at",       .type = DataType::DateTime },
            { .name = "last_modified",    .type = DataType::DateTime },
            { .name = "last_modified_by", .type = DataType::String,   .size = 100 },
            { .name = "version",          .type = DataType::Int,      .defaultValue = "1" },
            { .name = "is_deleted",       .type = DataType::Bool,     .defaultValue = "0" },
            { .name = "deleted_at",       .type = DataType::DateTime, .nullable = true },
        };
        inline constexpr column_index_t SYS_TABLES_KEY[] = {
            ColumnOf(SysTables::DatabaseId),
            ColumnOf(SysTables::TableId)
        };

        // ---- sys_columns
        inline constexpr SystemColumn SYS_COLUMNS_COLUMNS[] = {
            { .name = "table_id",         .type = DataType::Int },
            { .name = "column_id",        .type = DataType::Int,      .identity = true, .identityStart = FIRST_USER_OBJECT_ID },
            { .name = "name",             .type = DataType::String,   .size = 100 },
            { .name = "data_type",        .type = DataType::TinyInt },
            { .name = "record_size",      .type = DataType::Int },
            { .name = "precision",        .type = DataType::TinyInt,  .nullable = true },
            { .name = "scale",            .type = DataType::TinyInt,  .nullable = true },
            { .name = "is_nullable",      .type = DataType::Bool },
            { .name = "ordinal_position", .type = DataType::SmallInt },
            { .name = "is_system",        .type = DataType::Bool },
            { .name = "created_at",       .type = DataType::DateTime },
            { .name = "last_modified",    .type = DataType::DateTime },
            { .name = "last_modified_by", .type = DataType::String,   .size = 100 },
            { .name = "version",          .type = DataType::Int,      .defaultValue = "1" },
            { .name = "is_deleted",       .type = DataType::Bool,     .defaultValue = "0" },
            { .name = "deleted_at",       .type = DataType::DateTime, .nullable = true },
        };
        inline constexpr column_index_t SYS_COLUMNS_KEY[] = {
            ColumnOf(SysColumns::TableId),
            ColumnOf(SysColumns::ColumnId)
        };

        // ---- sys_indexes
        inline constexpr SystemColumn SYS_INDEXES_COLUMNS[] = {
            { .name = "table_id",         .type = DataType::Int },
            { .name = "index_id",         .type = DataType::Int,      .identity = true, .identityStart = FIRST_USER_OBJECT_ID },
            { .name = "name",             .type = DataType::String,   .size = 100 },
            { .name = "is_clustered",     .type = DataType::Bool },
            { .name = "is_disabled",      .type = DataType::Bool },
            { .name = "created_at",       .type = DataType::DateTime },
            { .name = "last_modified",    .type = DataType::DateTime },
            { .name = "last_modified_by", .type = DataType::String,   .size = 100 },
            { .name = "version",          .type = DataType::Int,      .defaultValue = "1" },
            { .name = "is_deleted",       .type = DataType::Bool,     .defaultValue = "0" },
            { .name = "deleted_at",       .type = DataType::DateTime, .nullable = true },
        };
        inline constexpr column_index_t SYS_INDEXES_KEY[] = {
            ColumnOf(SysIndexes::TableId),
            ColumnOf(SysIndexes::IndexId)
        };

        // ---- sys_identity_columns
        inline constexpr SystemColumn SYS_IDENTITY_COLUMNS_COLUMNS[] = {
            { .name = "table_id",         .type = DataType::Int },
            { .name = "column_id",        .type = DataType::Int },
            { .name = "seed_value",       .type = DataType::Int },
            { .name = "increment",        .type = DataType::Int },
            { .name = "last_value",       .type = DataType::BigInt },
            { .name = "is_cached",        .type = DataType::Bool },
            { .name = "cache_block",      .type = DataType::Int },
            { .name = "version",          .type = DataType::Int,      .defaultValue = "1" },
            { .name = "is_deleted",       .type = DataType::Bool,     .defaultValue = "0" },
            { .name = "deleted_at",       .type = DataType::DateTime, .nullable = true },
        };
        inline constexpr column_index_t SYS_IDENTITY_COLUMNS_KEY[] = {
            ColumnOf(SysIdentityColumns::TableId),
            ColumnOf(SysIdentityColumns::ColumnId)
        };

        // ---- sys_index_columns
        inline constexpr SystemColumn SYS_INDEX_COLUMNS_COLUMNS[] = {
            { .name = "index_id",         .type = DataType::Int },
            { .name = "column_id",        .type = DataType::Int },
            { .name = "ordinal_position", .type = DataType::SmallInt },
            { .name = "is_included",      .type = DataType::Bool },
            { .name = "version",          .type = DataType::Int,      .defaultValue = "1" },
            { .name = "is_deleted",       .type = DataType::Bool,     .defaultValue = "0" },
            { .name = "deleted_at",       .type = DataType::DateTime, .nullable = true },
        };
        inline constexpr column_index_t SYS_INDEX_COLUMNS_KEY[] = {
            ColumnOf(SysIndexColumns::IndexId),
            ColumnOf(SysIndexColumns::ColumnId)
        };

        // ---- sys_constraints
        inline constexpr SystemColumn SYS_CONSTRAINTS_COLUMNS[] = {
            { .name = "table_id",            .type = DataType::Int },
            { .name = "constraint_id",       .type = DataType::Int,      .identity = true, .identityStart = FIRST_USER_OBJECT_ID },
            { .name = "name",                .type = DataType::String,   .size = 100 },
            { .name = "constraint_type",     .type = DataType::TinyInt },
            { .name = "is_disabled",         .type = DataType::Bool },
            { .name = "constraint_index_id", .type = DataType::Int,      .nullable = true },
            { .name = "created_at",          .type = DataType::DateTime },
            { .name = "last_modified",       .type = DataType::DateTime },
            { .name = "last_modified_by",    .type = DataType::String,   .size = 100 },
            { .name = "version",             .type = DataType::Int,      .defaultValue = "1" },
            { .name = "is_deleted",          .type = DataType::Bool,     .defaultValue = "0" },
            { .name = "deleted_at",          .type = DataType::DateTime, .nullable = true },
        };
        inline constexpr column_index_t SYS_CONSTRAINTS_KEY[] = {
            ColumnOf(SysConstraints::TableId),
            ColumnOf(SysConstraints::ConstraintId)
        };

        // ---- sys_constraint_columns
        inline constexpr SystemColumn SYS_CONSTRAINT_COLUMNS_COLUMNS[] = {
            { .name = "constraint_id",    .type = DataType::Int },
            { .name = "column_id",        .type = DataType::Int },
            { .name = "ordinal_position", .type = DataType::SmallInt },
            { .name = "version",          .type = DataType::Int,      .defaultValue = "1" },
            { .name = "is_deleted",       .type = DataType::Bool,     .defaultValue = "0" },
            { .name = "deleted_at",       .type = DataType::DateTime, .nullable = true },
        };
        inline constexpr column_index_t SYS_CONSTRAINT_COLUMNS_KEY[] = {
            ColumnOf(SysConstraintColumns::ConstraintId),
            ColumnOf(SysConstraintColumns::ColumnId)
        };

        // ---- sys_default_values
        inline constexpr SystemColumn SYS_DEFAULT_VALUES_COLUMNS[] = {
            { .name = "column_id",  .type = DataType::Int },
            { .name = "value",      .type = DataType::String,   .size = 255 },
            { .name = "version",    .type = DataType::Int,      .defaultValue = "1" },
            { .name = "is_deleted", .type = DataType::Bool,     .defaultValue = "0" },
            { .name = "deleted_at", .type = DataType::DateTime, .nullable = true },
        };
        inline constexpr column_index_t SYS_DEFAULT_VALUES_KEY[] = {
            ColumnOf(SysDefaultValues::ColumnId)
        };

        // ---- sys_table_stats
        inline constexpr SystemColumn SYS_TABLE_STATS_COLUMNS[] = {
            { .name = "table_id",         .type = DataType::Int },
            { .name = "row_count",        .type = DataType::BigInt,   .defaultValue = "0" },
            { .name = "avg_record_size",  .type = DataType::Int,      .defaultValue = "0" },
            { .name = "page_count",       .type = DataType::Int,      .defaultValue = "0" },
            { .name = "last_modified_at", .type = DataType::DateTime },
        };
        inline constexpr column_index_t SYS_TABLE_STATS_KEY[] = {
            ColumnOf(SysTableStats::TableId)
        };

        // ---- sys_column_stats
        inline constexpr SystemColumn SYS_COLUMN_STATS_COLUMNS[] = {
            { .name = "column_id",      .type = DataType::Int },
            { .name = "distinct_count", .type = DataType::BigInt,   .defaultValue = "0" },
            { .name = "min_value",      .type = DataType::String,   .size = 200, .nullable = true },
            { .name = "max_value",      .type = DataType::String,   .size = 200, .nullable = true },
            { .name = "null_count",     .type = DataType::BigInt,   .nullable = true },
        };
        inline constexpr column_index_t SYS_COLUMN_STATS_KEY[] = {
            ColumnOf(SysColumnStats::ColumnId)
        };

        // ---- sys_column_histograms
        inline constexpr SystemColumn SYS_COLUMN_HISTOGRAMS_COLUMNS[] = {
            { .name = "column_id",      .type = DataType::Int },
            { .name = "histogram_id",   .type = DataType::Int,      .identity = true },
            { .name = "range_start",    .type = DataType::String,   .size = 200, .nullable = true },
            { .name = "range_end",      .type = DataType::String,   .size = 200, .nullable = true },
            { .name = "row_count",      .type = DataType::Int,      .defaultValue = "0" },
            { .name = "distinct_count", .type = DataType::BigInt,   .defaultValue = "0" },
        };
        inline constexpr column_index_t SYS_COLUMN_HISTOGRAMS_KEY[] = {
            ColumnOf(SysColumnHistograms::ColumnId),
            ColumnOf(SysColumnHistograms::HistogramId)
        };

        // ---- sys_roles
        inline constexpr SystemColumn SYS_ROLES_COLUMNS[] = {
            { .name = "role_id",          .type = DataType::Int,      .identity = true },
            { .name = "role_name",        .type = DataType::String,   .size = 100 },
            { .name = "permissions",      .type = DataType::Int },
            { .name = "is_system_role",   .type = DataType::Bool },
            { .name = "created_at",       .type = DataType::DateTime },
            { .name = "last_modified",    .type = DataType::DateTime },
            { .name = "last_modified_by", .type = DataType::String,   .size = 100 },
            { .name = "version",          .type = DataType::Int,      .defaultValue = "1" },
            { .name = "is_deleted",       .type = DataType::Bool,     .defaultValue = "0" },
            { .name = "deleted_at",       .type = DataType::DateTime, .nullable = true },
        };
        inline constexpr column_index_t SYS_ROLES_KEY[] = {
            ColumnOf(SysRoles::RoleId)
        };

        // ---- sys_users
        inline constexpr SystemColumn SYS_USERS_COLUMNS[] = {
            { .name = "user_id",          .type = DataType::Int,      .identity = true },
            { .name = "username",         .type = DataType::String,   .size = 100 },
            { .name = "password_hash",    .type = DataType::String,   .size = 200 },
            { .name = "role_id",          .type = DataType::Int },
            { .name = "is_active",        .type = DataType::Bool },
            { .name = "created_at",       .type = DataType::DateTime },
            { .name = "last_modified",    .type = DataType::DateTime },
            { .name = "last_modified_by", .type = DataType::String,   .size = 100 },
            { .name = "version",          .type = DataType::Int,      .defaultValue = "1" },
            { .name = "is_deleted",       .type = DataType::Bool,     .defaultValue = "0" },
            { .name = "deleted_at",       .type = DataType::DateTime, .nullable = true },
        };
        inline constexpr column_index_t SYS_USERS_KEY[] = {
            ColumnOf(SysUsers::UserId)
        };

        // ---- sys_index_stats
        inline constexpr SystemColumn SYS_INDEX_STATS_COLUMNS[] = {
            { .name = "table_id",          .type = DataType::Int },
            { .name = "index_id",          .type = DataType::Int },
            { .name = "leaf_pages",        .type = DataType::BigInt,   .defaultValue = "0" },
            { .name = "depth",             .type = DataType::TinyInt,  .defaultValue = "0" },
            { .name = "avg_fragmentation", .type = DataType::Decimal,  .size = 4 },
            { .name = "last_modified_at",  .type = DataType::DateTime },
        };
        inline constexpr column_index_t SYS_INDEX_STATS_KEY[] = {
            ColumnOf(SysIndexStats::TableId),
            ColumnOf(SysIndexStats::IndexId)
        };

        // The unscoped CatalogTables enumerators (SysDatabases, ...) hide the column enums of the same name
        // in ordinary lookup; `enum SysDatabases` is an elaborated type specifier and only looks up types.
        static_assert(std::size(SYS_DATABASES_COLUMNS)          == Reflection::EnumCount<enum SysDatabases>);
        static_assert(std::size(SYS_SCHEMAS_COLUMNS)            == Reflection::EnumCount<enum SysSchemas>);
        static_assert(std::size(SYS_TABLES_COLUMNS)             == Reflection::EnumCount<enum SysTables>);
        static_assert(std::size(SYS_COLUMNS_COLUMNS)            == Reflection::EnumCount<enum SysColumns>);
        static_assert(std::size(SYS_INDEXES_COLUMNS)            == Reflection::EnumCount<enum SysIndexes>);
        static_assert(std::size(SYS_IDENTITY_COLUMNS_COLUMNS)   == Reflection::EnumCount<enum SysIdentityColumns>);
        static_assert(std::size(SYS_INDEX_COLUMNS_COLUMNS)      == Reflection::EnumCount<enum SysIndexColumns>);
        static_assert(std::size(SYS_CONSTRAINTS_COLUMNS)        == Reflection::EnumCount<enum SysConstraints>);
        static_assert(std::size(SYS_CONSTRAINT_COLUMNS_COLUMNS) == Reflection::EnumCount<enum SysConstraintColumns>);
        static_assert(std::size(SYS_DEFAULT_VALUES_COLUMNS)     == Reflection::EnumCount<enum SysDefaultValues>);
        static_assert(std::size(SYS_TABLE_STATS_COLUMNS)        == Reflection::EnumCount<enum SysTableStats>);
        static_assert(std::size(SYS_COLUMN_STATS_COLUMNS)       == Reflection::EnumCount<enum SysColumnStats>);
        static_assert(std::size(SYS_COLUMN_HISTOGRAMS_COLUMNS)  == Reflection::EnumCount<enum SysColumnHistograms>);
        static_assert(std::size(SYS_ROLES_COLUMNS)              == Reflection::EnumCount<enum SysRoles>);
        static_assert(std::size(SYS_USERS_COLUMNS)              == Reflection::EnumCount<enum SysUsers>);
        static_assert(std::size(SYS_INDEX_STATS_COLUMNS)        == Reflection::EnumCount<enum SysIndexStats>);
    }

    // In CatalogTables order: SYSTEM_TABLES[ordinal] is that table.
    inline constexpr SystemTable SYSTEM_TABLES[] = {
        { CatalogTables::SysDatabases,         1,  "sys_databases",          SystemTableDefinitions::SYS_DATABASES_COLUMNS,          SystemTableDefinitions::SYS_DATABASES_KEY },
        { CatalogTables::SysSchemas,           2,  "sys_schemas",            SystemTableDefinitions::SYS_SCHEMAS_COLUMNS,            SystemTableDefinitions::SYS_SCHEMAS_KEY },
        { CatalogTables::SysTables,            3,  "sys_tables",             SystemTableDefinitions::SYS_TABLES_COLUMNS,             SystemTableDefinitions::SYS_TABLES_KEY },
        { CatalogTables::SysColumns,           4,  "sys_columns",            SystemTableDefinitions::SYS_COLUMNS_COLUMNS,            SystemTableDefinitions::SYS_COLUMNS_KEY },
        { CatalogTables::SysIndexes,           5,  "sys_indexes",            SystemTableDefinitions::SYS_INDEXES_COLUMNS,            SystemTableDefinitions::SYS_INDEXES_KEY },
        { CatalogTables::SysIdentityColumns,   6,  "sys_identity_columns",   SystemTableDefinitions::SYS_IDENTITY_COLUMNS_COLUMNS,   SystemTableDefinitions::SYS_IDENTITY_COLUMNS_KEY },
        { CatalogTables::SysIndexColumns,      7,  "sys_index_columns",      SystemTableDefinitions::SYS_INDEX_COLUMNS_COLUMNS,      SystemTableDefinitions::SYS_INDEX_COLUMNS_KEY },
        { CatalogTables::SysConstraints,       8,  "sys_constraints",        SystemTableDefinitions::SYS_CONSTRAINTS_COLUMNS,        SystemTableDefinitions::SYS_CONSTRAINTS_KEY },
        { CatalogTables::SysConstraintColumns, 9,  "sys_constraint_columns", SystemTableDefinitions::SYS_CONSTRAINT_COLUMNS_COLUMNS, SystemTableDefinitions::SYS_CONSTRAINT_COLUMNS_KEY },
        { CatalogTables::SysDefaultValues,     10, "sys_default_values",     SystemTableDefinitions::SYS_DEFAULT_VALUES_COLUMNS,     SystemTableDefinitions::SYS_DEFAULT_VALUES_KEY },
        { CatalogTables::SysTableStats,        11, "sys_table_stats",        SystemTableDefinitions::SYS_TABLE_STATS_COLUMNS,        SystemTableDefinitions::SYS_TABLE_STATS_KEY },
        { CatalogTables::SysColumnStats,       12, "sys_column_stats",       SystemTableDefinitions::SYS_COLUMN_STATS_COLUMNS,       SystemTableDefinitions::SYS_COLUMN_STATS_KEY },
        { CatalogTables::SysColumnHistograms,  13, "sys_column_histograms",  SystemTableDefinitions::SYS_COLUMN_HISTOGRAMS_COLUMNS,  SystemTableDefinitions::SYS_COLUMN_HISTOGRAMS_KEY },
        { CatalogTables::SysRoles,             14, "sys_roles",              SystemTableDefinitions::SYS_ROLES_COLUMNS,              SystemTableDefinitions::SYS_ROLES_KEY },
        { CatalogTables::SysUsers,             15, "sys_users",              SystemTableDefinitions::SYS_USERS_COLUMNS,              SystemTableDefinitions::SYS_USERS_KEY },
        { CatalogTables::SysIndexStats,        16, "sys_index_stats",        SystemTableDefinitions::SYS_INDEX_STATS_COLUMNS,        SystemTableDefinitions::SYS_INDEX_STATS_KEY },
    };

    static_assert(std::size(SYSTEM_TABLES) == Reflection::EnumCount<CatalogTables>, "one SystemTable per CatalogTables entry");

    // Structural checks the per-table static_asserts can't express
    consteval bool SystemTablesAreValid(){
        for (std::size_t i = 0; i < std::size(SYSTEM_TABLES); i++){
            const auto& table = SYSTEM_TABLES[i];
            if (table.ordinal != i)
                return false;   // SYSTEM_TABLES[ordinal] is that table
            if (table.name.Empty() || table.columns.empty())
                return false;
            if (table.primaryKey.empty())
                return false;   // every system table is clustered on its key
            if (table.columns.size() >= SYSTEM_COLUMN_ID_STRIDE)
                return false;   // SystemColumnId must not collide across tables
            if (SystemColumnId(table.id, static_cast<column_index_t>(table.columns.size() - 1)) >= FIRST_USER_OBJECT_ID)
                return false;   // system ids stay below the user range

            for (const auto key : table.primaryKey)
                if (key >= table.columns.size() || table.columns[key].nullable)
                    return false;                                    // key columns exist and are NOT NULL

            for (const auto& column : table.columns){
                if (column.name.Empty())                             return false;
                if (DataTypes::FixedColumnSize(column.type) == 0 && column.size == 0)
                    return false;                                    // variable-size columns need a size
                if (column.identity && column.type != DataType::Int && column.type != DataType::BigInt)
                    return false;
            }

            for (std::size_t j = 0; j < i; j++)
                if (SYSTEM_TABLES[j].id == table.id || SYSTEM_TABLES[j].name == table.name)
                    return false;                                    // ids and names are unique
        }
        return true;
    }
    static_assert(SystemTablesAreValid(), "SYSTEM_TABLES is inconsistent");

    [[nodiscard]] constexpr const SystemTable& SystemTableOf(const CatalogTables table){
        return SYSTEM_TABLES[table];
    }

    /** @} End of: System table definitions */

    // SystemTable -> the same TableDefinition user tables are built from (headers allocated in `scratch`)
    [[nodiscard]] Catalog::TableDefinition SystemTableDefinition(
        const ::Memory::IAllocator* allocator,
        const SystemTable& table
    );
}
