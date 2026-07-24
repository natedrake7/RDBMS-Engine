#pragma once
#include "../DataStructures/ConstexprDictionary.h"
#include "DataTypes.h"
#include "StringView.h"
#include "Decimal.h"
#include "JsonBinary.h"
#include "DateTime.h"
#include "Guid.h"
#include "StringValue.h"

static constexpr auto TRUE_STRING = DataTypes::StringView("TRUE");
static constexpr auto FALSE_STRING = DataTypes::StringView("FALSE");
static constexpr auto NULL_STRING = DataTypes::StringView("NULL");

static constexpr DataTypes::StringView TrueStrings[]{
    DataTypes::StringView("true"),
    DataTypes::StringView("1")
};

static constexpr DataTypes::StringView FalseStrings[]{
    DataTypes::StringView("false"),
    DataTypes::StringView("0")
};

static constexpr ConstexprDictionary COLUMN_SIZES_BY_TYPENAME{
    Pair(DataTypes::StringView("tinyint"), static_cast<block_size_t>(sizeof(TinyInt))),
    Pair(DataTypes::StringView("smallint"), static_cast<block_size_t>(sizeof(SmallInt))),
    Pair(DataTypes::StringView("int"), static_cast<block_size_t>(sizeof(Int))),
    Pair(DataTypes::StringView("bigint"), static_cast<block_size_t>(sizeof(BigInt))),
    Pair(DataTypes::StringView("datetime"), static_cast<block_size_t>(sizeof(DataTypes::DateTime))),
    Pair(DataTypes::StringView("bool"), static_cast<block_size_t>(sizeof(bool))),
    Pair(DataTypes::StringView("string"), static_cast<block_size_t>(0)),
    Pair(DataTypes::StringView("decimal"), static_cast<block_size_t>(0)),
    Pair(DataTypes::StringView("json"), static_cast<block_size_t>(0)),
    Pair(DataTypes::StringView("guid"), static_cast<block_size_t>(16))
};

static constexpr block_size_t VECTOR_COLUMN_SIZES_BY_DATATYPE[DATATYPE_COUNT]{
    sizeof(DataTypes::StringValue),
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

static constexpr ConstexprDictionary COLUMN_TYPENAMES_TO_ENUMS{
    Pair(DataTypes::StringView("string"), DataType::String),
    Pair(DataTypes::StringView("bool"), DataType::Bool),
    Pair(DataTypes::StringView("tinyint"), DataType::TinyInt),
    Pair(DataTypes::StringView("smallint"), DataType::SmallInt),
    Pair(DataTypes::StringView("int"), DataType::Int),
    Pair(DataTypes::StringView("bigint"), DataType::BigInt),
    Pair(DataTypes::StringView("decimal"), DataType::Decimal),
    Pair(DataTypes::StringView("datetime"), DataType::DateTime),
    Pair(DataTypes::StringView("json"), DataType::Json),
    Pair(DataTypes::StringView("guid"), DataType::Guid)
};

static constexpr DataTypes::StringView SQL_TYPES_NAMES[]{
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
};

static constexpr DataType JSON_TYPES_NAMES[]{
    DataType::Null, //NULL
    DataType::Bool,
    DataType::Decimal,
    DataType::String,
    DataType::String, //Array
    DataType::String, //Object
};