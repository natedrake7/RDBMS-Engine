#pragma once
#include "../DataStructures/ConstexprDictionary.h"
#include "DataTypes.h"
#include "StringView.h"
#include "Decimal.h"
#include "JsonBinary.h"
#include "DateTime.h"
#include "Guid.h"

static constexpr DataTypes::StringView TrueStrings[] = {
    DataTypes::StringView("true"),
    DataTypes::StringView("1")
};

static constexpr DataTypes::StringView FalseStrings[] = {
    DataTypes::StringView("false"),
    DataTypes::StringView("0")
};

static constexpr ConstexprDictionary<DataTypes::StringView, block_size_t, 10, CaseInsensitiveHash<DataTypes::StringView>> COLUMN_SIZES_BY_TYPENAME = std::initializer_list{
    Pair(DataTypes::StringView("tinyint"), static_cast<block_size_t>(sizeof(TinyInt))),
    Pair(DataTypes::StringView("smallint"), static_cast<block_size_t>(sizeof(SmallInt))),
    Pair(DataTypes::StringView("int"), static_cast<block_size_t>(sizeof(Int))),
    Pair(DataTypes::StringView("bigint"), static_cast<block_size_t>(sizeof(BigInt))),
    Pair(DataTypes::StringView("datetime"), static_cast<block_size_t>(DATETIME_SIZE)),
    Pair(DataTypes::StringView("bool"), static_cast<block_size_t>(sizeof(bool))),
    Pair(DataTypes::StringView("string"), static_cast<block_size_t>(0)),
    Pair(DataTypes::StringView("decimal"), static_cast<block_size_t>(0)),
    Pair(DataTypes::StringView("json"), static_cast<block_size_t>(0)),
    Pair(DataTypes::StringView("guid"), static_cast<block_size_t>(16))
};

static block_size_t COLUMN_SIZES_BY_DATATYPE[DATATYPE_COUNT] = {
    sizeof(DataTypes::String),
    sizeof(bool),
    sizeof(TinyInt),
    sizeof(SmallInt),
    sizeof(Int),
    sizeof(BigInt),
    sizeof(DataTypes::Decimal),
    sizeof(DataTypes::DateTime),
    sizeof(DataTypes::Guid),
    sizeof(DataTypes::JsonBinary),
};


static constexpr ConstexprDictionary<DataTypes::StringView, DataType, 10, CaseInsensitiveHash<DataTypes::StringView>> COLUMN_TYPENAMES_TO_ENUMS{
    Pair(DataTypes::StringView("tinyint"), DataType::TinyInt),
    Pair(DataTypes::StringView("smallint"), DataType::SmallInt),
    Pair(DataTypes::StringView("int"), DataType::Int),
    Pair(DataTypes::StringView("bigint"), DataType::BigInt),
    Pair(DataTypes::StringView("datetime"), DataType::DateTime),
    Pair(DataTypes::StringView("bool"), DataType::Bool),
    Pair(DataTypes::StringView("string"), DataType::String),
    Pair(DataTypes::StringView("decimal"), DataType::Decimal),
    Pair(DataTypes::StringView("json"), DataType::Json),
    Pair(DataTypes::StringView("guid"), DataType::Guid)
};

static constexpr DataTypes::StringView SQL_TYPES_NAMES[] = {
    DataTypes::StringView(""),         // 0  - Unknown
    DataTypes::StringView("String"),   // 1
    DataTypes::StringView("Bool"),     // 2
    DataTypes::StringView("TinyInt"),  // 3
    DataTypes::StringView("SmallInt"), // 4
    DataTypes::StringView("Int"),      // 5
    DataTypes::StringView("BigInt"),   // 6
    DataTypes::StringView("Decimal"),  // 7
    DataTypes::StringView("DateTime"), // 8
    DataTypes::StringView("Guid"),     // 9
    DataTypes::StringView("Json"),     // 10
    DataTypes::StringView("Null"),     // 11
    DataTypes::StringView(""),         // 12 - RowIdentifier (not displayable)
};

static constexpr DataType JSON_TYPES_NAMES[] = {
    DataType::Null, //NULL
    DataType::Bool,
    DataType::Decimal,
    DataType::String,
    DataType::String, //Array
    DataType::String, //Object
};