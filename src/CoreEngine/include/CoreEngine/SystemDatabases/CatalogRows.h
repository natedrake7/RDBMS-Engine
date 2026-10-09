#pragma once
#include <array>
#include <cstddef>
#include <meta>
#include <optional>
#include <string_view>

#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataTypes/DateTime.h>
#include <Systemic/DataTypes/Decimal.h>
#include <Systemic/DataTypes/String.h>
#include <Systemic/DataTypes/StringView.h>
#include <CoreEngine/Reflection/Rows.h>
#include <CoreEngine/SystemDatabases/CatalogSchema.h>

// One struct per system catalog table. Member order is column order, member names are the
// column names in camelCase (tableId -> table_id), std::optional<T> is a nullable column.
// The annotations carry what the C++ type can't: key, identity, size, default text, table identity.
namespace CoreEngine::Catalog{
    using Rows::TableInfo;
    using Rows::PrimaryKey;
    using Rows::Identity;
    using Rows::MaxSize;
    using Rows::Default;

    struct [[=TableInfo{ .name = "sys_databases", .id = 1, .ordinal = CatalogTables::SysDatabases }]] SysDatabaseRow{
        [[=PrimaryKey{}, =Identity{ Constants::SYSTEM_CATALOG_ID }]] Int databaseId;
        [[=MaxSize{ 100 }]]                                         DataTypes::String name;
        [[=MaxSize{ 100 }]]                                         DataTypes::String filepath;
                                                                    bool isSystem;
                                                                    DataTypes::DateTime createdAt;
                                                                    DataTypes::DateTime lastModified;
        [[=MaxSize{ 100 }]]                                         DataTypes::String lastModifiedBy;
        [[=Default{ "1" }]]                                         Int version = 1;
        [[=Default{ "0" }]]                                         bool isDeleted = false;
                                                                    std::optional<DataTypes::DateTime> deletedAt;
    };

    struct [[=TableInfo{ "sys_schemas", 2, CatalogTables::SysSchemas }]] SysSchemaRow{
        [[=PrimaryKey{}]]                                           Int databaseId;
        [[=PrimaryKey{}, =Identity{ FIRST_USER_OBJECT_ID }]]        Int schemaId;
        [[=MaxSize{ 100 }]]                                         DataTypes::String name;
                                                                    DataTypes::DateTime createdAt;
                                                                    DataTypes::DateTime lastModified;
        [[=MaxSize{ 100 }]]                                         DataTypes::String lastModifiedBy;
        [[=Default{ "1" }]]                                         Int version = 1;
        [[=Default{ "0" }]]                                         bool isDeleted = false;
                                                                    std::optional<DataTypes::DateTime> deletedAt;
    };

    struct [[=TableInfo{ "sys_tables", 3, CatalogTables::SysTables }]] SysTableRow{
        [[=PrimaryKey{}]]                                           Int databaseId;
        [[=PrimaryKey{}, =Identity{ FIRST_USER_OBJECT_ID }]]        Int tableId;
                                                                    Int schemaId;
        [[=MaxSize{ 100 }]]                                         DataTypes::String name;
                                                                    SmallInt ordinalPosition;
                                                                    bool isSystem;
                                                                    DataTypes::DateTime createdAt;
                                                                    DataTypes::DateTime lastModified;
        [[=MaxSize{ 100 }]]                                         DataTypes::String lastModifiedBy;
        [[=Default{ "1" }]]                                         Int version = 1;
        [[=Default{ "0" }]]                                         bool isDeleted = false;
                                                                    std::optional<DataTypes::DateTime> deletedAt;
    };

    struct [[=TableInfo{ "sys_columns", 4, CatalogTables::SysColumns }]] SysColumnRow{
        [[=PrimaryKey{}]]                                           Int tableId;
        [[=PrimaryKey{}, =Identity{ FIRST_USER_OBJECT_ID }]]        Int columnId;
        [[=MaxSize{ 100 }]]                                         DataTypes::String name;
                                                                    TinyInt dataType;
                                                                    Int recordSize;
                                                                    std::optional<TinyInt> precision;
                                                                    std::optional<TinyInt> scale;
                                                                    bool isNullable;
                                                                    SmallInt ordinalPosition;
                                                                    bool isSystem;
                                                                    DataTypes::DateTime createdAt;
                                                                    DataTypes::DateTime lastModified;
        [[=MaxSize{ 100 }]]                                         DataTypes::String lastModifiedBy;
        [[=Default{ "1" }]]                                         Int version = 1;
        [[=Default{ "0" }]]                                         bool isDeleted = false;
                                                                    std::optional<DataTypes::DateTime> deletedAt;
    };

    struct [[=TableInfo{ "sys_indexes", 5, CatalogTables::SysIndexes }]] SysIndexRow{
        [[=PrimaryKey{}]]                                           Int tableId;
        [[=PrimaryKey{}, =Identity{ FIRST_USER_OBJECT_ID }]]        Int indexId;
        [[=MaxSize{ 100 }]]                                         DataTypes::String name;
                                                                    bool isClustered;
                                                                    bool isDisabled;
                                                                    DataTypes::DateTime createdAt;
                                                                    DataTypes::DateTime lastModified;
        [[=MaxSize{ 100 }]]                                         DataTypes::String lastModifiedBy;
        [[=Default{ "1" }]]                                         Int version = 1;
        [[=Default{ "0" }]]                                         bool isDeleted = false;
                                                                    std::optional<DataTypes::DateTime> deletedAt;
    };

    struct [[=TableInfo{ "sys_identity_columns", 6, CatalogTables::SysIdentityColumns }]] SysIdentityColumnRow{
        [[=PrimaryKey{}]]                                           Int tableId;
        [[=PrimaryKey{}]]                                           Int columnId;
                                                                    Int seedValue;
                                                                    Int increment;
                                                                    BigInt lastValue;
                                                                    bool isCached;
                                                                    Int cacheBlock;
        [[=Default{ "1" }]]                                         Int version = 1;
        [[=Default{ "0" }]]                                         bool isDeleted = false;
                                                                    std::optional<DataTypes::DateTime> deletedAt;
    };

    struct [[=TableInfo{ "sys_index_columns", 7, CatalogTables::SysIndexColumns }]] SysIndexColumnRow{
        [[=PrimaryKey{}]]                                           Int indexId;
        [[=PrimaryKey{}]]                                           Int columnId;
                                                                    SmallInt ordinalPosition;
                                                                    bool isIncluded;
        [[=Default{ "1" }]]                                         Int version = 1;
        [[=Default{ "0" }]]                                         bool isDeleted = false;
                                                                    std::optional<DataTypes::DateTime> deletedAt;
    };

    struct [[=TableInfo{ "sys_constraints", 8, CatalogTables::SysConstraints }]] SysConstraintRow{
        [[=PrimaryKey{}]]                                           Int tableId;
        [[=PrimaryKey{}, =Identity{ FIRST_USER_OBJECT_ID }]]        Int constraintId;
        [[=MaxSize{ 100 }]]                                         DataTypes::String name;
                                                                    TinyInt constraintType;
                                                                    bool isDisabled;
                                                                    std::optional<Int> constraintIndexId;
                                                                    DataTypes::DateTime createdAt;
                                                                    DataTypes::DateTime lastModified;
        [[=MaxSize{ 100 }]]                                         DataTypes::String lastModifiedBy;
        [[=Default{ "1" }]]                                         Int version = 1;
        [[=Default{ "0" }]]                                         bool isDeleted = false;
                                                                    std::optional<DataTypes::DateTime> deletedAt;
    };

    struct [[=TableInfo{ "sys_constraint_columns", 9, CatalogTables::SysConstraintColumns }]] SysConstraintColumnRow{
        [[=PrimaryKey{}]]                                           Int constraintId;
        [[=PrimaryKey{}]]                                           Int columnId;
                                                                    SmallInt ordinalPosition;
        [[=Default{ "1" }]]                                         Int version = 1;
        [[=Default{ "0" }]]                                         bool isDeleted = false;
                                                                    std::optional<DataTypes::DateTime> deletedAt;
    };

    struct [[=TableInfo{ "sys_default_values", 10, CatalogTables::SysDefaultValues }]] SysDefaultValueRow{
        [[=PrimaryKey{}]]                                           Int columnId;
        [[=MaxSize{ 255 }]]                                         DataTypes::String value;
        [[=Default{ "1" }]]                                         Int version = 1;
        [[=Default{ "0" }]]                                         bool isDeleted = false;
                                                                    std::optional<DataTypes::DateTime> deletedAt;
    };

    struct [[=TableInfo{ "sys_table_stats", 11, CatalogTables::SysTableStats }]] SysTableStatsRow{
        [[=PrimaryKey{}]]                                           Int tableId;
        [[=Default{ "0" }]]                                         BigInt rowCount = 0;
        [[=Default{ "0" }]]                                         Int avgRecordSize = 0;
        [[=Default{ "0" }]]                                         Int pageCount = 0;
                                                                    DataTypes::DateTime lastModifiedAt;
    };

    struct [[=TableInfo{ "sys_column_stats", 12, CatalogTables::SysColumnStats }]] SysColumnStatsRow{
        [[=PrimaryKey{}]]                                           Int columnId;
        [[=Default{ "0" }]]                                         BigInt distinctCount = 0;
        [[=MaxSize{ 200 }]]                                         std::optional<DataTypes::String> minValue;
        [[=MaxSize{ 200 }]]                                         std::optional<DataTypes::String> maxValue;
                                                                    std::optional<BigInt> nullCount;
    };

    struct [[=TableInfo{ "sys_column_histograms", 13, CatalogTables::SysColumnHistograms }]] SysColumnHistogramRow{
        [[=PrimaryKey{}]]                                           Int columnId;
        [[=PrimaryKey{}, =Identity{}]]                              Int histogramId;
        [[=MaxSize{ 200 }]]                                         std::optional<DataTypes::String> rangeStart;
        [[=MaxSize{ 200 }]]                                         std::optional<DataTypes::String> rangeEnd;
        [[=Default{ "0" }]]                                         Int rowCount = 0;
        [[=Default{ "0" }]]                                         BigInt distinctCount = 0;
    };

    struct [[=TableInfo{ "sys_roles", 14, CatalogTables::SysRoles }]] SysRoleRow{
        [[=PrimaryKey{}, =Identity{}]]                              Int roleId;
        [[=MaxSize{ 100 }]]                                         DataTypes::String roleName;
                                                                    Int permissions;
                                                                    bool isSystemRole;
                                                                    DataTypes::DateTime createdAt;
                                                                    DataTypes::DateTime lastModified;
        [[=MaxSize{ 100 }]]                                         DataTypes::String lastModifiedBy;
        [[=Default{ "1" }]]                                         Int version = 1;
        [[=Default{ "0" }]]                                         bool isDeleted = false;
                                                                    std::optional<DataTypes::DateTime> deletedAt;
    };

    struct [[=TableInfo{ "sys_users", 15, CatalogTables::SysUsers }]] SysUserRow{
        [[=PrimaryKey{}, =Identity{}]]                              Int userId;
        [[=MaxSize{ 100 }]]                                         DataTypes::String username;
        [[=MaxSize{ 200 }]]                                         DataTypes::String passwordHash;
                                                                    Int roleId;
                                                                    bool isActive;
                                                                    DataTypes::DateTime createdAt;
                                                                    DataTypes::DateTime lastModified;
        [[=MaxSize{ 100 }]]                                         DataTypes::String lastModifiedBy;
        [[=Default{ "1" }]]                                         Int version = 1;
        [[=Default{ "0" }]]                                         bool isDeleted = false;
                                                                    std::optional<DataTypes::DateTime> deletedAt;
    };

    struct [[=TableInfo{ "sys_index_stats", 16, CatalogTables::SysIndexStats }]] SysIndexStatsRow{
        [[=PrimaryKey{}]]                                           Int tableId;
        [[=PrimaryKey{}]]                                           Int indexId;
        [[=Default{ "0" }]]                                         BigInt leafPages = 0;
        [[=Default{ "0" }]]                                         TinyInt depth = 0;
        [[=MaxSize{ 4 }]]                                           DataTypes::Decimal avgFragmentation;
                                                                    DataTypes::DateTime lastModifiedAt;
    };

    // In CatalogTables order
    using CatalogRowTypes = DataTypes::TypeList<
        SysDatabaseRow,
        SysSchemaRow,
        SysTableRow,
        SysColumnRow,
        SysIndexRow,
        SysIdentityColumnRow,
        SysIndexColumnRow,
        SysConstraintRow,
        SysConstraintColumnRow,
        SysDefaultValueRow,
        SysTableStatsRow,
        SysColumnStatsRow,
        SysColumnHistogramRow,
        SysRoleRow,
        SysUserRow,
        SysIndexStatsRow
    >;

    template <typename TRow>
    concept CatalogRow = CatalogRowTypes::IndexOf<TRow>() < CatalogRowTypes::SIZE;

    template <CatalogRow TRow>
    inline constexpr TableInfo TableInfoOf = Rows::AnnotationOf<TableInfo>(^^TRow);

    template <CatalogRow TRow>
    inline constexpr CatalogTables TableOrdinalOf = static_cast<CatalogTables>(TableInfoOf<TRow>.ordinal);

    /**
     * @name Generated column definitions
     * @{
     */

    // Row type -> its SystemColumn array
    template <typename TRow>
    [[nodiscard]] consteval auto ColumnsOf(){
        std::array<SystemColumn, Rows::ColumnCount<TRow>> columns{};
        std::size_t next = 0;

        template for (constexpr auto member : Rows::Members<TRow>()){
            using T = [:std::meta::type_of(member):];
            const auto name = Rows::ColumnNameOf(member);

            auto& column = columns[next++];
            column.name = DataTypes::StringView(name.data(), static_cast<data_size_t>(name.size()));
            column.type = DataTypes::DataTypeOf<typename Rows::Unwrap<T>::Type>();
            column.nullable = Rows::IsNullable<T>;

            if constexpr (::Reflection::HasAnnotation<MaxSize>(member))
                column.size = Rows::AnnotationOf<MaxSize>(member)._value;

            if constexpr (::Reflection::HasAnnotation<Identity>(member)){
                column.identity = true;
                column.identityStart = Rows::AnnotationOf<Identity>(member)._start;
            }

            if constexpr (::Reflection::HasAnnotation<Default>(member)){
                // keep the annotation alive: View() points into it. define_static_string returns const char*,
                // so the length comes from the annotation
                const auto annotation = Rows::AnnotationOf<Default>(member);
                const auto text = annotation._sql.View();
                const char* sql = std::define_static_string(text);
                column.defaultValue = DataTypes::StringView(sql, static_cast<data_size_t>(text.size()));
            }
        }
        return columns;
    }

    /** @} End of: Generated column definitions */

    /**
     * @name System tables
     * Generated from CatalogRowTypes: SYSTEM_TABLES[ordinal] describes the catalog table at that position.
     * @{
     */

    // Static storage the SystemTable spans point into
    template <CatalogRow TRow>
    inline constexpr auto COLUMNS_OF = ColumnsOf<TRow>();

    template <CatalogRow TRow>
    inline constexpr auto PRIMARY_KEY_OF = Rows::KeyColumnsOf<TRow>();

    namespace Detail{
        template <typename... TRows>
        [[nodiscard]] consteval std::array<SystemTable, sizeof...(TRows)> MakeSystemTables(DataTypes::TypeList<TRows...>){
            return std::array<SystemTable, sizeof...(TRows)>{
                SystemTable{
                    .ordinal = TableOrdinalOf<TRows>,
                    .id = static_cast<table_id_t>(TableInfoOf<TRows>.id),
                    .name = DataTypes::StringView(TableInfoOf<TRows>.name._data, TableInfoOf<TRows>.name._size),
                    .columns = COLUMNS_OF<TRows>,
                    .primaryKey = PRIMARY_KEY_OF<TRows>
                }...
            };
        }

        // StringView::operator== isn't usable in constant evaluation: compare as std::string_view
        [[nodiscard]] consteval bool SameText(const DataTypes::StringView lhs, const DataTypes::StringView rhs){
            return std::string_view(lhs.Data(), lhs.Size()) == std::string_view(rhs.Data(), rhs.Size());
        }
    }

    inline constexpr auto SYSTEM_TABLES = Detail::MakeSystemTables(CatalogRowTypes{});

    static_assert(SYSTEM_TABLES.size() == ::Reflection::EnumCount<CatalogTables>, "one row type per catalog table");

    // Structural checks a row struct can't express by itself. Throws the reason, so a failing
    // static_assert names what is wrong.
    consteval bool SystemTablesAreValid(){
        for (std::size_t i = 0; i < SYSTEM_TABLES.size(); i++){
            const auto& table = SYSTEM_TABLES[i];
            if (table.ordinal != i)
                throw "SYSTEM_TABLES: a row type's TableInfo ordinal differs from its position in CatalogRowTypes";
            if (table.name.Empty() || table.columns.empty())
                throw "SYSTEM_TABLES: a table has no name or no columns";
            if (table.primaryKey.empty())
                throw "SYSTEM_TABLES: every system table needs a primary key (it is clustered on it)";
            if (table.columns.size() >= SYSTEM_COLUMN_ID_STRIDE)
                throw "SYSTEM_TABLES: too many columns, SystemColumnId would collide across tables";
            if (SystemColumnId(table.id, static_cast<column_index_t>(table.columns.size() - 1)) >= FIRST_USER_OBJECT_ID)
                throw "SYSTEM_TABLES: system column ids reach the user id range";

            for (const auto key : table.primaryKey)
                if (key >= table.columns.size() || table.columns[key].nullable)
                    throw "SYSTEM_TABLES: primary key columns must exist and be NOT NULL";

            for (const auto& column : table.columns){
                if (column.name.Empty())
                    throw "SYSTEM_TABLES: a column has no name";
                if (DataTypes::FixedColumnSize(column.type) == 0 && column.size == 0)
                    throw "SYSTEM_TABLES: a variable-size column has no MaxSize";
                if (column.identity && column.type != DataType::Int && column.type != DataType::BigInt)
                    throw "SYSTEM_TABLES: identity columns must be Int or BigInt";
            }

            for (std::size_t j = 0; j < i; j++)
                if (SYSTEM_TABLES[j].id == table.id || Detail::SameText(SYSTEM_TABLES[j].name, table.name))
                    throw "SYSTEM_TABLES: table ids and names must be unique";
        }
        return true;
    }
    static_assert(SystemTablesAreValid());

    // The column position enums of CatalogSchema.h index these rows: their sizes must match.
    // `enum SysX` is an elaborated type specifier: the unscoped CatalogTables enumerators hide the enum names.
    static_assert(Rows::ColumnCount<SysDatabaseRow>         == ::Reflection::EnumCount<enum SysDatabases>);
    static_assert(Rows::ColumnCount<SysSchemaRow>           == ::Reflection::EnumCount<enum SysSchemas>);
    static_assert(Rows::ColumnCount<SysTableRow>            == ::Reflection::EnumCount<enum SysTables>);
    static_assert(Rows::ColumnCount<SysColumnRow>           == ::Reflection::EnumCount<enum SysColumns>);
    static_assert(Rows::ColumnCount<SysIndexRow>            == ::Reflection::EnumCount<enum SysIndexes>);
    static_assert(Rows::ColumnCount<SysIdentityColumnRow>   == ::Reflection::EnumCount<enum SysIdentityColumns>);
    static_assert(Rows::ColumnCount<SysIndexColumnRow>      == ::Reflection::EnumCount<enum SysIndexColumns>);
    static_assert(Rows::ColumnCount<SysConstraintRow>       == ::Reflection::EnumCount<enum SysConstraints>);
    static_assert(Rows::ColumnCount<SysConstraintColumnRow> == ::Reflection::EnumCount<enum SysConstraintColumns>);
    static_assert(Rows::ColumnCount<SysDefaultValueRow>     == ::Reflection::EnumCount<enum SysDefaultValues>);
    static_assert(Rows::ColumnCount<SysTableStatsRow>       == ::Reflection::EnumCount<enum SysTableStats>);
    static_assert(Rows::ColumnCount<SysColumnStatsRow>      == ::Reflection::EnumCount<enum SysColumnStats>);
    static_assert(Rows::ColumnCount<SysColumnHistogramRow>  == ::Reflection::EnumCount<enum SysColumnHistograms>);
    static_assert(Rows::ColumnCount<SysRoleRow>             == ::Reflection::EnumCount<enum SysRoles>);
    static_assert(Rows::ColumnCount<SysUserRow>             == ::Reflection::EnumCount<enum SysUsers>);
    static_assert(Rows::ColumnCount<SysIndexStatsRow>       == ::Reflection::EnumCount<enum SysIndexStats>);

    [[nodiscard]] constexpr const SystemTable& SystemTableOf(const CatalogTables table){
        return SYSTEM_TABLES[table];
    }

    /** @} End of: System tables */
}
