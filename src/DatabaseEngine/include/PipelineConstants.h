#pragma once

#include <cstdint>
#include <cstddef>
#include <limits>
#include "../../Systemic/include/DataTypes/DataTypes.h"
#include "../../Systemic/include/Security/Security.h"

namespace Constants{
    constexpr std::string_view WRITE_AHEAD_LOG_FILE = "wal.log";

    constexpr size_t PAGE_SIZE = 8 * 1024;
    constexpr size_t MAX_NUMBER_OF_PAGES = 10000;
    constexpr size_t MAX_NUMBER_SYSTEM_PAGES = 1000000;
    constexpr size_t EXTENT_SIZE = 8;
    constexpr size_t EXTENT_BYTE_SIZE = EXTENT_SIZE * PAGE_SIZE;
    constexpr size_t EXTENT_BIT_MAP_SIZE = 64000;
    constexpr size_t LARGE_DATA_OBJECT_SIZE = 8060;
    constexpr size_t LARGE_DATA_MAX_SIZE = 2147483648;
    constexpr size_t LOG_BATCH_SIZE = 1024 * 1024; // 1 MB

    enum class AlterTableType: uint8_t {
        AddColumn = 0,
        AlterColumn = 1,
        DropColumn = 2,
        RenameColumn = 3,
    };

    enum OrderType : uint8_t {
        ASCENDING = 0,
        DESCENDING = 1,
    };

    enum AggregateFunction: uint8_t {
        NONE = 0,
        SUM = 1,
        AVERAGE = 2,
        COUNT = 3,
        MIN = 4,
        MAX = 5
    };

    enum class PageType : uint8_t{
        DATA = 0,
        IAM = 1,
        LOB = 2,
        INDEX = 3,
        METADATA = 4,
        GAM = 5,
        FREESPACE = 6,
        OVERFLOWTYPE = 7,
        UNDO = 8
    };

    enum class PagePriority : uint8_t{
        LOW = 0,
        MEDIUM = 1,
        HIGH = 2,
        SYSTEM = 3,
    };

    enum class TableType : uint8_t{
        HEAP = 0,
        CLUSTERED = 1,
    };

    enum TreeType : uint8_t {
        Clustered = 0,
        NonClustered = 1
    };

    constexpr uint16_t OBJECT_METADATA_SIZE_T = sizeof(page_size_t) + sizeof(page_id_t) + sizeof(large_page_index_t);
    constexpr uint16_t PAGE_HEADER_SIZE = sizeof(page_id_t) + 2 * sizeof(page_size_t) + sizeof(PageType);
    constexpr uint16_t OVERFLOW_POINTER_SIZE = sizeof(page_offset_t) + sizeof(page_id_t);

    constexpr uint16_t PAGE_FREE_SPACE_SIZE = PAGE_SIZE - PAGE_HEADER_SIZE - 7;
    constexpr uint16_t NEXT_PAGE_FREE_SPACE = PAGE_FREE_SPACE_SIZE + 1;
    constexpr page_size_t PAGE_SIZE_WITHOUT_HEADER = PAGE_SIZE - PAGE_HEADER_SIZE;
    constexpr page_size_t INDEX_PAGE_ADDITIONAL_HEADER_SIZE = sizeof(page_id_t) + sizeof(TreeType) + sizeof(uint8_t) + 3 * sizeof(bool) + sizeof(uint16_t);
    constexpr page_size_t INDEX_PAGE_DEFAULT_SIZE = PAGE_SIZE_WITHOUT_HEADER - INDEX_PAGE_ADDITIONAL_HEADER_SIZE;

    constexpr uint16_t GAM_PAGE_SIZE = 64000;
    constexpr uint32_t GAM_NUMBER_OF_PAGES = 64000 * 8;
    constexpr page_id_t HEADER_PAGE_ID = 0;

    constexpr int32_t CATALOG_ID = 1;

    constexpr page_id_t NEXT_GAM_PAGE_ID_OFFSET = (GAM_NUMBER_OF_PAGES + PAGE_FREE_SPACE_SIZE - 1) / PAGE_FREE_SPACE_SIZE + 1;

    constexpr row_header_size_t ROW_VERSION_HEADER_SIZE = 2 * sizeof(transaction_id_t) + sizeof(page_id_t) + sizeof(page_offset_t);

    enum class FunctionType : uint8_t {
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
        NullIf = 121
    };

    static constexpr std::string_view DEFAULT_SCHEMA_NAME = "dbo";
    static constexpr std::string_view ADMIN_NAME = "admin";
    static constexpr std::string_view DB_OWNER_NAME = "db_owner";
    static constexpr std::string_view DB_WRITER_NAME = "db_writer";
    static constexpr std::string_view DB_READER_NAME = "db_reader";
    static constexpr std::string_view GUEST_NAME = "guest";

    static constexpr auto  ADMIN_PERMISSIONS = Security::Permission::ALL;

    static constexpr auto  GUEST_PERMISSIONS =
      Security::Permission::NONE;

    static constexpr Security::Permission DB_READER_PERMISSIONS =
        Security::Permission::SELECT
        | GUEST_PERMISSIONS;

    static constexpr Security::Permission DB_WRITER_PERMISSIONS =
        Security::Permission::INSERT
        | Security::Permission::UPDATE
        | Security::Permission::DELETE_PERMISSION
        | DB_READER_PERMISSIONS;

    static constexpr Security::Permission DB_OWNER_PERMISSIONS =
        DB_WRITER_PERMISSIONS
        | Security::Permission::CREATE
        | Security::Permission::DROP
        | Security::Permission::ALTER;
}
