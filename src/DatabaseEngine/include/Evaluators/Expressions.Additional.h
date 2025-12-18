#pragma once
#include <string>
#include "../../../Systemic/include/DataStructures/Dictionary.h"
#include "../PipelineConstants.h"

#include <functional>
#define UNLIMITED_ARGS (-1)

namespace Expressions {
    class Expression;

    enum class LogicalType {
        And = 0,
        Or = 1,
        Invalid = 2
      };

    enum class BinaryOperator {
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
        EqualIgnoreOrdinalCase = 11
    };

    enum class BranchType {
        Switch = 0,
        Ternary = 1,
    };

    enum class ExpressionType : uint8_t {
        Expression = 0,
        Column = 1,
        Constant = 2,
        Binary = 3,
        Logical = 4,
        Variable = 5,
        Branch = 6,
        Function = 7
    };

    static Dictionary<std::string, BinaryOperator> ExpressionOperatorsDictionary{
    { "=", BinaryOperator::Equal },
    { "!=", BinaryOperator::NotEqual },
    { "<>", BinaryOperator::NotEqual },
    { ">", BinaryOperator::Greater },
    { ">=", BinaryOperator::GreaterEqual },
    { "<", BinaryOperator::Less },
    { "<=", BinaryOperator::LessEqual },
    { "+", BinaryOperator::Add },
    { "-", BinaryOperator::Subtract },
    { "*", BinaryOperator::Multiply },
    { "/", BinaryOperator::Divide },
    { "%", BinaryOperator::Modulo },
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
          {"space",     Constants::FunctionType::Space},
          {"nullif",    Constants::FunctionType::NullIf},
          {"coalesce",  Constants::FunctionType::Coalesce}
    };

    struct FunctionInfo {
        std::string name;
        int minArgs;
        int maxArgs;
        std::vector<DataType> expectedTypes;
        DataType returnType;
        bool allowImplicitCast;
        bool additionalValidations;
    };

    static Dictionary<Constants::FunctionType, FunctionInfo> FunctionInfoDictionary{
        { Constants::FunctionType::GetDate,
            {"GETDATE", 0, 0, {}, DataType::DateTime, false, false } },

        { Constants::FunctionType::NewGuid,
            {"NEWID", 0, 0, {}, DataType::Guid, false, false } },

        { Constants::FunctionType::Concat,
            {"CONCAT", 2, UNLIMITED_ARGS, {DataType::String}, DataType::String, true, false } },

        { Constants::FunctionType::Length,
            {"LENGTH", 1, 1, {DataType::String}, DataType::Int, false, false } },

        { Constants::FunctionType::AsciiValue,
            {"ASCII", 1, 1, {DataType::String}, DataType::Int, false, false } },

        { Constants::FunctionType::Char,
            {"CHAR", 1, 1, {DataType::Int}, DataType::String, false, false } },

        { Constants::FunctionType::CharIndex,
            {"CHARINDEX", 2, 3,
                {DataType::String, DataType::String, DataType::Int},
                DataType::Int, false, false } },

        { Constants::FunctionType::Lower,
            {"LOWER", 1, 1, {DataType::String}, DataType::String, false, false } },

        { Constants::FunctionType::Upper,
            {"UPPER", 1, 1, {DataType::String}, DataType::String, false, false } },

        { Constants::FunctionType::Trim,
            {"TRIM", 1, 1, {DataType::String}, DataType::String, false, false } },

        { Constants::FunctionType::TrimLeft,
            {"TRIMLEFT", 1, 1, {DataType::String}, DataType::String, false, false } },

        { Constants::FunctionType::TrimRight,
            {"TRIMRIGHT", 1, 1, {DataType::String}, DataType::String, false, false } },

        { Constants::FunctionType::Replace,
            {"REPLACE", 3, 3,
                {DataType::String, DataType::String, DataType::String},
                DataType::String, false, false } },

        { Constants::FunctionType::Substr,
            {"SUBSTR", 2, 3,
                {DataType::String, DataType::Int, DataType::Int},
                DataType::String, false, false } },

        { Constants::FunctionType::Left,
            {"LEFT", 2, 2,
                {DataType::String, DataType::Int},
                DataType::String, false, false } },

        { Constants::FunctionType::Right,
            {"RIGHT", 2, 2,
                {DataType::String, DataType::Int},
                DataType::String, false, false } },

        { Constants::FunctionType::Reverse,
            {"REVERSE", 1, 1, {DataType::String}, DataType::String, false, false } },

        { Constants::FunctionType::Space,
            {"SPACE", 1, 1, {DataType::Int}, DataType::String, false, false } },

        { Constants::FunctionType::NullIf,
        {"NULLIF", 2, 2, {DataType::String, DataType::String}, DataType::String, false, true } },

        { Constants::FunctionType::Coalesce,
        {"COALESCE", 2, UNLIMITED_ARGS, {DataType::String}, DataType::String, false, true } },
    };

}