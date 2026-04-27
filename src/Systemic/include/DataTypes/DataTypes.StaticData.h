#pragma once
#include "../DataStructures/ConstexprDictionary.h"
#include "DataTypes.h"
#include "StringView.h"

static constexpr ConstexprDictionary ColumnTypeRank{
    Pair(DataType::String, 1),
    Pair(DataType::Bool, 3),
    Pair(DataType::TinyInt, 4),
    Pair(DataType::SmallInt, 5),
    Pair(DataType::Int, 6),
    Pair(DataType::BigInt, 7),
    Pair(DataType::Decimal, 8),
    Pair(DataType::DateTime, 9),
};

static constexpr DataTypes::StringView TrueStrings[] = {
    DataTypes::StringView("true"),
    DataTypes::StringView("1")
};

static constexpr DataTypes::StringView FalseStrings[] = {
    DataTypes::StringView("false"),
    DataTypes::StringView("0")
};

static constexpr ConstexprDictionary<DataTypes::StringView, block_size_t, 10, CaseInsensitiveHash<DataTypes::StringView>> ColumnTypeSizes = std::initializer_list{
    Pair(DataTypes::StringView("tinyint"), static_cast<block_size_t>(sizeof(TinyInt))),
    Pair(DataTypes::StringView("smallint"), static_cast<block_size_t>(sizeof(SmallInt))),
    Pair(DataTypes::StringView("int"), static_cast<block_size_t>(sizeof(Int))),
    Pair(DataTypes::StringView("bigint"), static_cast<block_size_t>(sizeof(BigInt))),
    Pair(DataTypes::StringView("datetime"), static_cast<block_size_t>(DATETIME_SIZE)),
    Pair(DataTypes::StringView("bool"), static_cast<block_size_t>(sizeof(bool))),
    Pair(DataTypes::StringView("string"), static_cast<block_size_t>(0)),
    Pair(DataTypes::StringView("decimal"), static_cast<block_size_t>(0)),
    Pair(DataTypes::StringView("unicodestring"), static_cast<block_size_t>(0)),
    Pair(DataTypes::StringView("guid"), static_cast<block_size_t>(16))
};

static constexpr ConstexprDictionary<DataTypes::StringView, DataType, 10, CaseInsensitiveHash<DataTypes::StringView>> ColumnTypesDictionary{
    Pair(DataTypes::StringView("tinyint"), DataType::TinyInt),
    Pair(DataTypes::StringView("smallint"), DataType::SmallInt),
    Pair(DataTypes::StringView("int"), DataType::Int),
    Pair(DataTypes::StringView("bigint"), DataType::BigInt),
    Pair(DataTypes::StringView("datetime"), DataType::DateTime),
    Pair(DataTypes::StringView("bool"), DataType::Bool),
    Pair(DataTypes::StringView("string"), DataType::String),
    Pair(DataTypes::StringView("decimal"), DataType::Decimal),
    Pair(DataTypes::StringView("guid"), DataType::Guid)
};

static constexpr ConstexprDictionary DataTypeToStringDictionary{
    Pair(DataType::TinyInt, DataTypes::StringView("TinyInt")),
    Pair(DataType::SmallInt, DataTypes::StringView("SmallInt")),
    Pair(DataType::Int, DataTypes::StringView("Int")),
    Pair(DataType::BigInt, DataTypes::StringView("BigInt")),
    Pair(DataType::DateTime, DataTypes::StringView("DateTime")),
    Pair(DataType::Bool, DataTypes::StringView("Bool")),
    Pair(DataType::String, DataTypes::StringView("String")),
    Pair(DataType::Decimal, DataTypes::StringView("Decimal")),
    Pair(DataType::Guid, DataTypes::StringView("Guid")),
    Pair(DataType::Unknown, DataTypes::StringView("Invalid"))
    //all other types must have their size defined since it is not constant (e.g. string, decimal dont have fixed sizes)
};