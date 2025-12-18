#pragma once
#include "../../Systemic/include/DataStructures/Dictionary.h"
#include "../../Systemic/include/DataStructures/HashSet.h"
#include "../../DatabaseEngine/include/PipelineConstants.h"
#include "../../Systemic/include/Constants.h"

namespace QueryPipeline::PipelineConstants {
  typedef uint16_t cursor_id_t;

  constexpr int32_t SMALL_TABLE = 1000;
  constexpr int32_t MEDIUM_TABLE = 10000;
  constexpr int32_t LARGE_TABLE = 100000;

  static constexpr double RANDOM_PAGE_COST = 4.0;
  static constexpr double SEQUENTIAL_PAGE_COST = 1.0;
  static constexpr double CPU_COST = 0.01;

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
