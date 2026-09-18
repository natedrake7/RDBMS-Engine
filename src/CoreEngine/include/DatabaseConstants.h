#pragma once
#include "../../Systemic/include/DataTypes/DataTypes.h"
#include "../../Systemic/include/DataTypes/PackedWord.h"
#include "../../Server/include/Security/Security.h"
#include "../../Systemic/include/DataTypes/StringView.h"

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

    enum class PageType : UnsignedSmallInt{
        DATA = 0,
        ALLOCATION = 1,
        LOB_ROOT = 2,
        INDEXED = 3,
        HEADER = 4,
        GLOBAL_ALLOCATION = 5,
        FREE_SPACE = 6,
        OVERFLOW_TYPE = 7,
        LOB_INDEX = 8,
        LOB_DATA = 9
    };

    enum class PagePriority : UnsignedTinyInt{
        LOW = 0,
        MEDIUM = 1,
        HIGH = 2,
        SYSTEM = 3,
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

    enum class ExecutionMode : UnsignedTinyInt{
        Row = 0,
        Vectorized = 1
    };

    inline constexpr size_t MAX_QUERY_JOINS = 10;
    inline constexpr size_t MAX_TABLE_COLUMNS = 128;

    static_assert(
         MAX_TABLE_COLUMNS <= std::numeric_limits<column_index_t>::max(),
         "MAX_TABLE_COLUMNS must fit in column_index_t"
    );

    inline constexpr UnsignedSmallInt OBJECT_METADATA_SIZE_T = sizeof(page_size_t) + sizeof(page_id_t) + sizeof(large_page_index_t);
    inline constexpr UnsignedSmallInt PAGE_HEADER_SIZE = sizeof(page_id_t) + 2 * sizeof(page_size_t);
    inline constexpr UnsignedSmallInt ALLOCATION_PAGE_ADDITIONAL_HEADER_SIZE = sizeof(extent_id_t) + 2 * sizeof(page_id_t);
    inline constexpr UnsignedSmallInt OVERFLOW_POINTER_SIZE = sizeof(page_offset_t) + sizeof(page_id_t);

    inline constexpr size_t PAGE_SIZE = 8 * 1024;
    inline constexpr size_t MAX_NUMBER_OF_PAGES = 100000;
    inline constexpr size_t MAX_NUMBER_SYSTEM_PAGES = 1000000;
    inline constexpr size_t EXTENT_SIZE = 8;
    inline constexpr size_t EXTENT_BYTE_SIZE = EXTENT_SIZE * PAGE_SIZE;
    inline constexpr size_t EXTENT_BIT_MAP_SIZE = 64000;
    inline constexpr size_t GAM_HEADER_SIZE = PAGE_SIZE - (EXTENT_BIT_MAP_SIZE / 8) - PAGE_HEADER_SIZE;
    inline constexpr size_t GAM_HEADER_RESERVED_SPACE = PAGE_SIZE - (EXTENT_BIT_MAP_SIZE / 8) - PAGE_HEADER_SIZE - 2 * sizeof(extent_id_t) - sizeof(UnsignedInt);
    inline constexpr size_t GAM_METADATA_SIZE = PAGE_HEADER_SIZE + GAM_HEADER_SIZE;

    inline constexpr UnsignedSmallInt PAGE_FREE_SPACE_SIZE = PAGE_SIZE - PAGE_HEADER_SIZE;
    inline constexpr UnsignedSmallInt NEXT_PAGE_FREE_SPACE = PAGE_FREE_SPACE_SIZE + 1;
    inline constexpr page_size_t PAGE_SIZE_WITHOUT_HEADER = PAGE_SIZE - PAGE_HEADER_SIZE;
    inline constexpr page_size_t INDEX_PAGE_SIBLINGS_SIZE = 2 * sizeof(page_id_t);
    inline constexpr UnsignedSmallInt LARGE_OBJECT_PAGE_SIZE = PAGE_SIZE_WITHOUT_HEADER - 2 * sizeof(page_id_t);

    inline constexpr size_t LOB_ROOT_HEADER_SIZE =
            sizeof(UnsignedBigInt) + sizeof(page_id_t)
            + 2 * sizeof(UnsignedInt) + 2 * sizeof(UnsignedTinyInt)
            + sizeof(UnsignedSmallInt);

    inline constexpr size_t LOB_INDEX_HEADER_SIZE = sizeof(page_id_t) + sizeof(UnsignedInt);
    inline constexpr size_t LOB_DATA_HEADER_SIZE = sizeof(page_id_t) + sizeof(UnsignedInt);

    inline constexpr page_size_t INDEX_PAGE_ADDITIONAL_HEADER_SIZE = INDEX_PAGE_SIBLINGS_SIZE + PackedByte::SIZE + 3; //3 stands for alignment and reserved space
    inline constexpr page_size_t INDEX_PAGE_DEFAULT_SIZE = PAGE_SIZE_WITHOUT_HEADER - INDEX_PAGE_ADDITIONAL_HEADER_SIZE;

    inline constexpr UnsignedSmallInt GAM_PAGE_SIZE = 64000;
    inline constexpr UnsignedInt GAM_NUMBER_OF_PAGES = 64000 * 8;
    inline constexpr page_id_t HEADER_PAGE_ID = 0;

    inline constexpr Int TEMPORARY_DATABASE_ID = 0;
    inline constexpr Int VERSION_DATABASE_ID = 1;
    inline constexpr Int SYSTEM_CATALOG_ID = 2;

    inline constexpr page_id_t NEXT_GAM_PAGE_ID_OFFSET = (GAM_NUMBER_OF_PAGES + PAGE_FREE_SPACE_SIZE - 1) / PAGE_FREE_SPACE_SIZE + 1;

    inline constexpr Int LARGE_OBJECT_POINTER_SIZE = sizeof(page_id_t);
    inline constexpr Int LARGE_OBJECT_METADATA_SIZE = PAGE_HEADER_SIZE + sizeof(page_size_t) + sizeof(page_id_t) + 2; //2 is for alignment
    inline constexpr Int OVERFLOW_POINTER_TOTAL_SIZE = sizeof(page_id_t) + sizeof(page_offset_t);

    inline constexpr size_t LARGE_DATA_OBJECT_SIZE = PAGE_SIZE - LARGE_OBJECT_METADATA_SIZE;;
    inline constexpr size_t LARGE_OBJECT_THRESHOLD_SIZE = PAGE_SIZE_WITHOUT_HEADER;
    inline constexpr size_t LARGE_DATA_MAX_SIZE = 2 * 1024 * 1024; //2 * Byte -> KB -> MB -> GB;

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
