#pragma once
#include "../AdditionalLibraries/Dictionary/Dictionary.h"
#include "../AdditionalLibraries/HashSet/HashSet.h"
#include "../Database/Constants.h"

namespace QueryPipeline::PipelineConstants {
  typedef uint16_t cursor_id_t;

  static Dictionary<std::string, Constants::JoinType> JoinTypeDictionary{
    {"inner", Constants::JoinType::Inner},
    {"left", Constants::JoinType::Left},
    {"right", Constants::JoinType::Right},
    {"full", Constants::JoinType::Full},
  };

  static HashSet<Constants::DataType> ValidIntegerConversions{
    Constants::DataType::TinyInt,
    Constants::DataType::SmallInt,
    Constants::DataType::Int,
    Constants::DataType::BigInt,
  };

  static HashSet<Constants::DataType> ValidStringConversions{
    Constants::DataType::String,
    Constants::DataType::UnicodeString
  };
}