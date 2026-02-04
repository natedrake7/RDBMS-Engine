#pragma once
#include "DataTypes/DataTypes.h"
#include <limits>

constexpr Int NUMBER_OF_HISTOGRAM_BUCKETS = 100;

constexpr Int INVALID_DATABASE_ID = -1;
constexpr Int INVALID_TABLE_ID = -1;
constexpr Int INVALID_COLUMN_ID = -1;
constexpr Int INVALID_HISTOGRAM_ID = -1;
constexpr Int INVALID_SCHEMA_ID = -1;
constexpr SmallInt INVALID_ORDINAL_POS = -1;
constexpr Int INVALID_CONSTRAINT_ID = -1;
constexpr Int INVALID_INDEX_ID = -1;

constexpr transaction_id_t INVALID_LOG_SEQUENCE_NUMBER = std::numeric_limits<log_sequence_number_t>::max();
constexpr table_id_t INVALID_TABLE_ORDINAL_POS = std::numeric_limits<table_id_t>::max();
constexpr transaction_id_t INVALID_TRANSACTION_ID = std::numeric_limits<transaction_id_t>::max();
constexpr page_id_t INVALID_PAGE_ID = std::numeric_limits<page_id_t>::max();
constexpr extent_id_t INVALID_EXTENT_ID = std::numeric_limits<extent_id_t>::max();
constexpr Int INVALID_PAGE_INDEX_ID = -1;
constexpr size_t ROW_ID_SIZE = sizeof(page_id_t) + sizeof(Int);

constexpr transaction_id_t FIRST_TRANSACTION_ID = 0;
constexpr Int DEFAULT_BATCH_SIZE = 10000;

constexpr std::string_view WILDCARD = "*";

constexpr TinyInt INVALID_DECIMAL_PRECISION = -1;
constexpr TinyInt INVALID_DECIMAL_SCALE = -1;

constexpr Int MAX_DECIMAL_PRECISION = 38;
constexpr Int MAX_DECIMAL_SCALE = 38;

constexpr BigInt INVALID_TOP = -1;

enum class JoinType : UnsignedTinyInt {
    Inner = 0,
    Left = 1,
    Right = 2,
    Full = 3
};