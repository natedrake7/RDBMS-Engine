#pragma once
#include <string>
#include "../../Dictionary/Dictionary.h"
#include "../../../Database/Constants.h"
#define UNLIMITED_ARGS (-1)

namespace Expressions {
  enum class ExpressionType {
    And = 0,
    Or = 1,
    Invalid = 2
  };

  enum class ExpressionOperator {
    Equal = 0,
    NotEqual = 1,
    Greater = 2,
    GreaterEqual = 3,
    Less = 4,
    LessEqual = 5,
    Add = 6,
    Subtract = 7,
    Multiply = 8,
    Divide = 9,
    Modulo = 10,
  };

  static Dictionary<std::string, ExpressionOperator> ExpressionOperatorsDictionary{
    { "=", ExpressionOperator::Equal },
    { "!=", ExpressionOperator::NotEqual },
    { "<>", ExpressionOperator::NotEqual },
    { ">", ExpressionOperator::Greater },
    { ">=", ExpressionOperator::GreaterEqual },
    { "<", ExpressionOperator::Less },
    { "<=", ExpressionOperator::LessEqual },
    { "+", ExpressionOperator::Add },
    { "-", ExpressionOperator::Subtract },
    { "*", ExpressionOperator::Multiply },
    { "/", ExpressionOperator::Divide },
    { "%", ExpressionOperator::Modulo },
  };

static Dictionary<std::string, Constants::FunctionType> FunctionTypeDictionary{
      {"getdate",   Constants::FunctionType::GetDate},
      {"newid",     Constants::FunctionType::NewGuid},
      {"concat",    Constants::FunctionType::Concat},
      {"length",    Constants::FunctionType::Length},
      {"ascii",     Constants::FunctionType::AsciiValue},
      {"char",      Constants::FunctionType::Char},
      {"charindex", Constants::FunctionType::CharIndex},
      {"lower",     Constants::FunctionType::Lower},
      {"upper",     Constants::FunctionType::Upper},
      {"trim",      Constants::FunctionType::Trim},
      {"trimleft",  Constants::FunctionType::TrimLeft},
      {"trimright", Constants::FunctionType::TrimRight},
      {"replace",   Constants::FunctionType::Replace},
      {"substr",    Constants::FunctionType::Substr},
      {"left",      Constants::FunctionType::Left},
      {"right",     Constants::FunctionType::Right},
      {"reverse",   Constants::FunctionType::Reverse},
      {"space",     Constants::FunctionType::Space}
};

struct FunctionInfo {
  std::string name;
  int minArgs;
  int maxArgs;
  std::vector<Constants::DataType> expectedTypes;
  Constants::DataType returnType;
};

static Dictionary<Constants::FunctionType, FunctionInfo> FunctionInfoDictionary{
    { Constants::FunctionType::GetDate,
        {"GETDATE", 0, 0, {}, Constants::DataType::DateTime } },

    { Constants::FunctionType::NewGuid,
        {"NEWID", 0, 0, {}, Constants::DataType::Guid } },

    { Constants::FunctionType::Concat,
        {"CONCAT", 2, UNLIMITED_ARGS, {Constants::DataType::String}, Constants::DataType::String } },

    { Constants::FunctionType::Length,
        {"LENGTH", 1, 1, {Constants::DataType::String}, Constants::DataType::Int } },

    { Constants::FunctionType::AsciiValue,
        {"ASCII", 1, 1, {Constants::DataType::String}, Constants::DataType::Int } },

    { Constants::FunctionType::Char,
        {"CHAR", 1, 1, {Constants::DataType::Int}, Constants::DataType::String } },

    { Constants::FunctionType::CharIndex,
        {"CHARINDEX", 2, 3,
            {Constants::DataType::String, Constants::DataType::String, Constants::DataType::Int},
            Constants::DataType::Int } },

    { Constants::FunctionType::Lower,
        {"LOWER", 1, 1, {Constants::DataType::String}, Constants::DataType::String } },

    { Constants::FunctionType::Upper,
        {"UPPER", 1, 1, {Constants::DataType::String}, Constants::DataType::String } },

    { Constants::FunctionType::Trim,
        {"TRIM", 1, 1, {Constants::DataType::String}, Constants::DataType::String } },

    { Constants::FunctionType::TrimLeft,
        {"TRIMLEFT", 1, 1, {Constants::DataType::String}, Constants::DataType::String } },

    { Constants::FunctionType::TrimRight,
        {"TRIMRIGHT", 1, 1, {Constants::DataType::String}, Constants::DataType::String } },

    { Constants::FunctionType::Replace,
        {"REPLACE", 3, 3,
            {Constants::DataType::String, Constants::DataType::String, Constants::DataType::String},
            Constants::DataType::String } },

    { Constants::FunctionType::Substr,
        {"SUBSTR", 2, 3,
            {Constants::DataType::String, Constants::DataType::Int, Constants::DataType::Int},
            Constants::DataType::String } },

    { Constants::FunctionType::Left,
        {"LEFT", 2, 2,
            {Constants::DataType::String, Constants::DataType::Int},
            Constants::DataType::String } },

    { Constants::FunctionType::Right,
        {"RIGHT", 2, 2,
            {Constants::DataType::String, Constants::DataType::Int},
            Constants::DataType::String } },

    { Constants::FunctionType::Reverse,
        {"REVERSE", 1, 1, {Constants::DataType::String}, Constants::DataType::String } },

    { Constants::FunctionType::Space,
        {"SPACE", 1, 1, {Constants::DataType::Int}, Constants::DataType::String } },
};

}