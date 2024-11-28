#pragma once

constexpr size_t PAGE_SIZE = 8 * 1024;
constexpr size_t MAX_NUMBER_OF_PAGES = 100;
constexpr size_t MAX_NUMBER_SYSTEM_PAGES = 20;
constexpr size_t EXTENT_SIZE = 8;
constexpr size_t EXTENT_BYTE_SIZE = EXTENT_SIZE * PAGE_SIZE;
constexpr size_t EXTENT_BIT_MAP_SIZE = 65000;
constexpr size_t LARGE_DATA_OBJECT_SIZE = 1024;

//table types
typedef uint16_t table_id_t;

//data types
typedef unsigned char object_t;

//extent types
typedef uint32_t extent_id_t;
typedef uint32_t extent_num_t;

//Page types
typedef uint32_t page_id_t;
typedef uint16_t page_size_t;
typedef uint16_t page_offset_t;
typedef uint16_t large_page_index_t;

enum class PageType : uint8_t {
    DATA = 0,
    INDEX = 1,
    LOB = 2,
    METADATA = 3,
    GAM = 4,
};

//block types
typedef uint16_t block_size_t;

//column types
typedef uint16_t column_index_t;

//record size
typedef uint32_t row_size_t;

//header literal size
typedef uint16_t header_literal_t;
typedef uint16_t row_header_size_t;

//number of column - table
typedef uint16_t column_number_t;
typedef uint16_t table_number_t;

//bit map constants
typedef uint16_t bit_map_size_t;
typedef uint16_t bit_map_pos_t;
typedef uint8_t byte;

constexpr uint16_t OBJECT_METADATA_SIZE_T = sizeof(page_size_t) + sizeof(page_id_t) + sizeof(large_page_index_t);