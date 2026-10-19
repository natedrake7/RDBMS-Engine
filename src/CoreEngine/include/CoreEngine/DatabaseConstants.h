#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataTypes/PackedWord.h>
#include <Server/Security/Security.h>
#include <Systemic/DataTypes/StringView.h>

namespace Pages
{
    class HeaderPageView;
    class PageFreeSpaceView;
    class GlobalAllocationPageView;
}

namespace Constants{
    inline constexpr UnsignedBigInt KB = 1024;
    inline constexpr UnsignedBigInt MB = 1024 * KB;
    inline constexpr UnsignedBigInt GB = 1024 * MB;

    inline constexpr auto WRITE_AHEAD_LOG_FILE = DataTypes::StringView("wal.log");
    inline constexpr auto UNDO_LOG_FILE = DataTypes::StringView("undo.log");
    inline constexpr auto DATA_FILE_EXTENSION = DataTypes::StringView(".data");
    inline constexpr auto SYS_EXTENSION = DataTypes::StringView("_sys");
    inline constexpr auto DEFAULT_SCHEMA_NAME = DataTypes::StringView("dbo");

    inline constexpr Int DEFAULT_IDENTITY_SEED = 1;
    inline constexpr Int DEFAULT_IDENTITY_INCREMENT = 1;
    inline constexpr Int DEFAULT_IDENTITY_VALUE = 1;
    inline constexpr Int DEFAULT_IDENTITY_CACHE_BLOCK = 10000;

    inline constexpr size_t LOG_BATCH_SIZE = MB; // 1 MB

    inline constexpr size_t DEFAULT_BATCH_SIZE = 2048;

    enum class AlterTableType: UnsignedTinyInt {
        AddColumn = 0,
        AlterColumn = 1,
        DropColumn = 2,
        RenameColumn = 3,
    };

    enum OrderType : UnsignedTinyInt {
        ASCENDING = 0,
        DESCENDING = 1,
    };

    enum AggregateFunction: UnsignedTinyInt {
        NONE = 0,
        SUM = 1,
        AVERAGE = 2,
        COUNT = 3,
        MIN = 4,
        MAX = 5
    };

    enum class TableType : UnsignedTinyInt{
        HEAP = 0,
        CLUSTERED = 1,
    };

    enum TreeType : UnsignedTinyInt {
        Clustered = 0,
        NonClustered = 1
    };

    inline constexpr Int VECTORIZATION_THRESHOLD = 128;

    inline constexpr size_t MAX_QUERY_JOINS = 10;
    inline constexpr size_t MAX_TABLE_COLUMNS = 128;

    static_assert(
         MAX_TABLE_COLUMNS <= std::numeric_limits<column_index_t>::max(),
         "MAX_TABLE_COLUMNS must fit in column_index_t"
    );

    // Page and file layout constants live in <CoreEngine/BufferPool/StorageLayout.h>

    inline constexpr Int TEMPORARY_DATABASE_ID = 0;
    inline constexpr Int VERSION_DATABASE_ID = 1;
    inline constexpr Int SYSTEM_CATALOG_ID = 2;

    inline constexpr Int SYSTEM_SCHEMA_ID = 1;

    enum class FunctionType : UnsignedTinyInt {
        // -----------------------
        // Date/Time Functions
        // -----------------------
        GetDate = 0,         // SQL Server style
        DateAdd = 1,
        DateDiff = 2,
        DatePart = 3,
        Year = 4,
        Month = 5,
        Day = 6,

        // -----------------------
        // GUID/Identifier Functions
        // -----------------------
        NewGuid = 20,

        // -----------------------
        // String Functions
        // -----------------------
        Concat = 40,
        Length = 41,
        AsciiValue = 42,
        Char = 43,
        CharIndex = 44,
        Instr = 45,          // alias for CharIndex
        Lower = 46,
        Upper = 47,
        Trim = 48,
        TrimLeft = 49,
        TrimRight = 50,
        Replace = 51,
        Substr = 52,
        Left = 53,
        Right = 54,
        Reverse = 55,
        Repeat = 56,
        Space = 57,
        Soundex = 58,

        // -----------------------
        // Mathematical Functions
        // -----------------------
        Abs = 80,
        Ceil = 81,
        Floor = 82,
        Round = 83,
        Power = 84,
        Sqrt = 85,
        Exp = 86,
        Log = 87,
        Log10 = 88,
        Rand = 89,

        // -----------------------
        // Conversion Functions
        // -----------------------
        Cast = 100,
        Convert = 101,

        // -----------------------
        // Aggregate Functions
        // -----------------------
        Count = 120,
        Sum = 121,
        Avg = 122,
        Min = 123,
        Max = 124,

        // -----------------------
        // Null Checking Functions
        // -----------------------
        Coalesce = 150,
        NullIf = 151,


        //Sentinel (Plugins Lookup)
        Plugin = 200,
    };

    inline constexpr auto ADMIN_NAME = DataTypes::StringView("admin");
    inline constexpr auto DB_OWNER_NAME = DataTypes::StringView("db_owner");
    inline constexpr auto DB_WRITER_NAME = DataTypes::StringView("db_writer");
    inline constexpr auto DB_READER_NAME = DataTypes::StringView("db_reader");
    inline constexpr auto GUEST_NAME = DataTypes::StringView("guest");

    inline constexpr auto  ADMIN_PERMISSIONS = Security::Permission::ALL;

    inline constexpr auto  GUEST_PERMISSIONS =
        Security::Permission::NONE;

    inline constexpr Security::Permission DB_READER_PERMISSIONS =
        Security::Permission::SELECT
        | GUEST_PERMISSIONS;

    inline constexpr Security::Permission DB_WRITER_PERMISSIONS =
        Security::Permission::INSERT
        | Security::Permission::UPDATE
        | Security::Permission::DELETE_PERMISSION
        | DB_READER_PERMISSIONS;

    inline constexpr Security::Permission DB_OWNER_PERMISSIONS =
        DB_WRITER_PERMISSIONS
        | Security::Permission::CREATE
        | Security::Permission::DROP
        | Security::Permission::ALTER;
}
