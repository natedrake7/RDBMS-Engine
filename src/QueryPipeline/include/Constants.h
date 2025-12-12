#pragma once
#include "../../Systemic/include/DataStructures/Dictionary.h"
#include "../../Systemic/include/DataStructures/HashSet.h"
#include "../../DatabaseEngine/include/Constants.h"

namespace QueryPipeline::PipelineConstants {
  typedef uint16_t cursor_id_t;

  static Dictionary<std::string, Constants::JoinType> JoinTypeDictionary{
    {"inner", Constants::JoinType::Inner},
    {"left", Constants::JoinType::Left},
    {"right", Constants::JoinType::Right},
    {"full", Constants::JoinType::Full},
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