#pragma once
#include "../AdditionalLibraries/Dictionary/Dictionary.h"
#include "../Database/Constants.h"

namespace QueryPipeline::Constants {
  static Dictionary<std::string, Constants::JoinType> JoinTypeDictionary{
    {"inner", Constants::JoinType::Inner},
    {"left", Constants::JoinType::Left},
    {"right", Constants::JoinType::Right},
    {"full", Constants::JoinType::Full},
  };

}