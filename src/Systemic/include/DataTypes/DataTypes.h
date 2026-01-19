#pragma once
#include "DateTime.h"
#include <cstdint>
#include <string>
#include "../DataStructures/Dictionary.h"

typedef uint8_t UnsignedTinyInt;
typedef uint16_t UnsignedSmallInt;
typedef uint32_t UnsignedInt;
typedef uint64_t UnsignedBigInt;
typedef int8_t TinyInt;
typedef int16_t SmallInt;
typedef int32_t Int;
typedef int64_t BigInt;

typedef uint8_t byte_t;

// block types
typedef uint16_t block_size_t;

// column types
typedef uint8_t column_index_t;

// record size
typedef uint16_t row_size_t;

// header literal size
typedef uint16_t header_literal_t;
typedef uint16_t row_header_size_t;

// number of column - table
typedef uint16_t column_number_t;
typedef uint16_t table_number_t;

// bit map constants
typedef uint16_t bit_map_size_t;
typedef uint16_t bit_map_pos_t;
typedef uint16_t byte_map_size_t;
typedef uint16_t byte_map_pos_t;

// decimal constants
typedef uint8_t fraction_index_t;

// B-tree constants
typedef uint16_t key_size_t;

// table types
typedef uint16_t table_id_t;
typedef int32_t column_id_t;

// data types
typedef unsigned char object_t;

// extent types
typedef uint32_t extent_id_t;
typedef uint32_t extent_num_t;

// Page types
typedef uint32_t page_id_t;
typedef int16_t page_size_t;
typedef uint16_t page_offset_t;
typedef uint16_t large_page_index_t;

typedef uint32_t log_sequence_number_t;
typedef uint64_t transaction_id_t;

enum class DataType : uint8_t{
  TinyInt = 0,
  SmallInt = 1,
  Int = 2,
  BigInt = 3,
  Decimal = 4,
  String = 5,
  UnicodeString = 6,
  Bool = 7,
  DateTime = 8,
  Guid = 9,
  RowIdentifier = 10,
  Unknown = 11
};

static Dictionary<std::string, block_size_t> ColumnTypeSizes = {
  {"tinyint", sizeof(int8_t)},
  {"smallint", sizeof(int16_t)},
  {"int", sizeof(int32_t)},
  {"bigint", sizeof(int64_t)},
  {"datetime", DataTypes::DateTime::Size()},
  {"bool", sizeof(bool)},
  {"string", 0},
  {"decimal", 0},
  {"unicodestring", 0},
  {"guid", 16}
  //all other types must have their size defined since it is not constant (e.g. string, decimal dont have fixed sizes)
};

static Dictionary<std::string, DataType> ColumnTypesDictionary = {
  {"tinyint", DataType::TinyInt},
  {"smallint", DataType::SmallInt},
  {"int", DataType::Int},
  {"bigint", DataType::BigInt},
  {"datetime", DataType::DateTime},
  {"bool", DataType::Bool},
  {"string", DataType::String},
  {"decimal", DataType::Decimal},
  {"unicodestring", DataType::UnicodeString},
  {"guid", DataType::Guid}
  //all other types must have their size defined since it is not constant (e.g. string, decimal dont have fixed sizes)
};

static Dictionary<DataType, std::string> ColumnTypesToStringDictionary = {
  {DataType::TinyInt, "TinyInt"},
  {DataType::SmallInt, "SmallInt"},
  {DataType::Int, "Int"},
  {DataType::BigInt, "BigInt"},
  {DataType::DateTime, "DateTime"},
  {DataType::Bool, "Bool"},
  {DataType::String, "String"},
  {DataType::Decimal, "Decimal"},
  {DataType::UnicodeString, "Unicodestring"},
  {DataType::Guid, "Guid"},
  {DataType::Unknown, "Invalid"}
  //all other types must have their size defined since it is not constant (e.g. string, decimal dont have fixed sizes)
};