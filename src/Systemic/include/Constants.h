#pragma once
#include "DataTypes/DataTypes.h"
#include <limits>
#include <string_view>

#include "DataTypes/DateTime.h"


static constexpr Int ITOS_BUFFER_SIZE = 32;
static constexpr Int NUMBER_OF_HISTOGRAM_BUCKETS = 100;

static constexpr Int DEFAULT_SLOT_INDEX = 0;

static constexpr Int INVALID_DATABASE_ID = -1;
static constexpr Int INVALID_TABLE_ID = -1;
static constexpr Int INVALID_COLUMN_ID = -1;
static constexpr Int INVALID_HISTOGRAM_ID = -1;
static constexpr Int INVALID_SCHEMA_ID = -1;
static constexpr SmallInt INVALID_ORDINAL_POS = -1;
static constexpr Int INVALID_CONSTRAINT_ID = -1;
static constexpr Int INVALID_INDEX_ID = -1;

static constexpr transaction_id_t INVALID_LOG_SEQUENCE_NUMBER = std::numeric_limits<log_sequence_number_t>::max();
static constexpr table_id_t INVALID_TABLE_ORDINAL_POS = std::numeric_limits<table_id_t>::max();
static constexpr transaction_id_t INVALID_TRANSACTION_ID = std::numeric_limits<transaction_id_t>::max();
static constexpr page_id_t INVALID_PAGE_ID = std::numeric_limits<page_id_t>::max();
static constexpr extent_id_t INVALID_EXTENT_ID = std::numeric_limits<extent_id_t>::max();
static constexpr UnsignedSmallInt INVALID_PAGE_INDEX_ID = std::numeric_limits<UnsignedSmallInt>::max();
static constexpr size_t ROW_ID_SIZE = sizeof(page_id_t) + sizeof(Int);

static constexpr transaction_id_t FIRST_TRANSACTION_ID = 0;
static constexpr Int DEFAULT_BATCH_SIZE = 10000;

static constexpr DataTypes::StringView WILDCARD = "*";

static constexpr TinyInt INVALID_DECIMAL_PRECISION = -1;
static constexpr TinyInt INVALID_DECIMAL_SCALE = -1;

static constexpr Int MAX_DECIMAL_PRECISION = 38;
static constexpr Int MAX_DECIMAL_SCALE = 38;

static constexpr BigInt INVALID_TOP = -1;

enum class JoinType : UnsignedTinyInt {
    Inner = 0,
    Left = 1,
    Right = 2,
    Full = 3
};