#pragma once
#include "../../DatabaseEngine/include/DatabaseConstants.h"
#include "../../Systemic/include/Constants.h"
#include "../../Systemic/include/DataStructures/ConstexprDictionary.h"
#include "../../Systemic/include/DataStructures/ConstexprHashSet.h"

namespace QueryPipeline::PipelineConstants {
    typedef uint16_t cursor_id_t;

    constexpr Int SMALL_TABLE = 1000;
    constexpr Int MEDIUM_TABLE = 10000;
    constexpr Int LARGE_TABLE = 100000;
    constexpr Int HASH_JOIN_THRESHOLD = 10000;

    static constexpr double RANDOM_PAGE_COST = 4.0;
    static constexpr double SEQUENTIAL_PAGE_COST = 1.0;
    static constexpr double CPU_COST_PER_ROW = 0.01;
    static constexpr double LOOKUP_COST_PER_ROW = RANDOM_PAGE_COST;
    static constexpr double INDEX_SEEK_COST = 0.1;

    static constexpr double CPU_COST_PER_COMPARISON = 0.0001;

    enum class JoinAlgorithm : UnsignedTinyInt{
        NestedLoopJoin = 0,
        HashJoin = 1,
        MergeJoin = 2,
    };

    static constexpr ConstexprDictionary<DataTypes::StringView, JoinType, 4> JoinTypeDictionary{
        Pair(DataTypes::StringView("inner"), JoinType::Inner),
        Pair(DataTypes::StringView("left"), JoinType::Left),
        Pair(DataTypes::StringView("right"), JoinType::Right),
        Pair(DataTypes::StringView("full"), JoinType::Full)
    };

    static constexpr ConstexprHashSet<DataType, 4> ValidTableIntegerConversions{
        DataType::TinyInt,
        DataType::SmallInt,
        DataType::Int,
        DataType::BigInt,
    };

    static constexpr ConstexprHashSet<DataType, 2> ValidTableStringConversions{
        DataType::String,
        DataType::UnicodeString
    };
}
