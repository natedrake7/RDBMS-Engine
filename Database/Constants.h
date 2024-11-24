#pragma once

constexpr size_t PAGE_SIZE = 8 * 1024;
constexpr size_t MAX_NUMBER_OF_PAGES = 100;
constexpr size_t EXTENT_SIZE = 8;
constexpr size_t EXTENT_BYTE_SIZE = EXTENT_SIZE * PAGE_SIZE;
constexpr size_t LARGE_DATA_OBJECT_SIZE = 1024;

//data types
typedef unsigned char object_t;

//Page types
typedef uint32_t page_id_t;
typedef uint16_t page_size_t;
typedef uint16_t page_offset_t;
typedef uint16_t large_page_index_t;

enum class PageType : uint8_t {
    DATA = 0,
    INDEX = 1,
    LOB = 2,
    METADATA = 3
};

//block types
typedef uint16_t block_size_t;

//column types
typedef uint16_t column_index_t;

//record size
typedef uint32_t record_size_t;

//metadata literal size
typedef uint16_t metadata_literal_t;

//number of column - table
typedef uint16_t column_number_t;
typedef uint16_t table_number_t;

//bit map constants
typedef uint16_t bit_map_size_t;
typedef uint16_t bit_map_pos_t;
typedef uint8_t byte;