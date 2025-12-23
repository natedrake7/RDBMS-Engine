#pragma once
#include "../../Systemic/include/DataStructures/Dictionary.h"
#include "../../Systemic/include/DataStructures/HashSet.h"
#include "../../DatabaseEngine/include/PipelineConstants.h"
#include "../../Systemic/include/Constants.h"

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

  static Dictionary<std::string, JoinType> JoinTypeDictionary{
    {"inner", JoinType::Inner},
    {"left", JoinType::Left},
    {"right", JoinType::Right},
    {"full", JoinType::Full},
  };

  static HashSet ValidTableIntegerConversions{
    DataType::TinyInt,
    DataType::SmallInt,
    DataType::Int,
    DataType::BigInt,
  };

  static HashSet ValidTableStringConversions{
    DataType::String,
    DataType::UnicodeString
  };
}
