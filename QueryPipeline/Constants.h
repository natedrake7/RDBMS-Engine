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

  static HashSet<Constants::ColumnType> ValidIntegerConversions{
    Constants::ColumnType::TinyInt,
    Constants::ColumnType::SmallInt,
    Constants::ColumnType::Int,
    Constants::ColumnType::BigInt,
  };

  static HashSet<Constants::ColumnType> ValidStringConversions{
    Constants::ColumnType::String,
    Constants::ColumnType::UnicodeString
  };
}