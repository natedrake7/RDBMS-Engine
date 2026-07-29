#pragma once
#include "../DatabaseConstants.h"

#include <utility>

#include "../../../Systemic/include/DataStructures/ConstexprDictionary.h"
#include "../../../Systemic/include/DataStructures/StaticArray.h"
#define UNLIMITED_ARGS (-1)

namespace Expressions {
    class Expression;

    enum class LogicalType {
        And = 0,
        Or = 1,
        Not = 2,
        Invalid = 3
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
        Function = 7,
        Json = 8,
        Cast = 9
    };

    static constexpr ConstexprDictionary EXPRESSION_OPERATORS_DICT{
        Pair(DataTypes::StringView("="), BinaryOperator::Equal),
        Pair(DataTypes::StringView("!="), BinaryOperator::NotEqual),
        Pair(DataTypes::StringView("<>"), BinaryOperator::NotEqual),
        Pair(DataTypes::StringView(">"), BinaryOperator::Greater),
        Pair(DataTypes::StringView(">="), BinaryOperator::GreaterEqual),
        Pair(DataTypes::StringView("<"), BinaryOperator::Less),
        Pair(DataTypes::StringView("<="), BinaryOperator::LessEqual),
        Pair(DataTypes::StringView("+"), BinaryOperator::Add),
        Pair(DataTypes::StringView("-"), BinaryOperator::Subtract),
        Pair(DataTypes::StringView("*"), BinaryOperator::Multiply),
        Pair(DataTypes::StringView("/"), BinaryOperator::Divide),
        Pair(DataTypes::StringView("%"), BinaryOperator::Modulo),
    };

    static constexpr ConstexprDictionary FUNCTION_TYPE_DICT{
        Pair(DataTypes::StringView("getdate"),   Constants::FunctionType::GetDate),
        Pair(DataTypes::StringView("newid"),     Constants::FunctionType::NewGuid),
        Pair(DataTypes::StringView("concat"),    Constants::FunctionType::Concat),
        Pair(DataTypes::StringView("length"),    Constants::FunctionType::Length),
        Pair(DataTypes::StringView("ascii"),     Constants::FunctionType::AsciiValue),
        Pair(DataTypes::StringView("char"),      Constants::FunctionType::Char),
        Pair(DataTypes::StringView("charindex"), Constants::FunctionType::CharIndex),
        Pair(DataTypes::StringView("lower"),     Constants::FunctionType::Lower),
        Pair(DataTypes::StringView("upper"),     Constants::FunctionType::Upper),
        Pair(DataTypes::StringView("trim"),      Constants::FunctionType::Trim),
        Pair(DataTypes::StringView("trimleft"),  Constants::FunctionType::TrimLeft),
        Pair(DataTypes::StringView("trimright"), Constants::FunctionType::TrimRight),
        Pair(DataTypes::StringView("replace"),   Constants::FunctionType::Replace),
        Pair(DataTypes::StringView("substr"),    Constants::FunctionType::Substr),
        Pair(DataTypes::StringView("left"),      Constants::FunctionType::Left),
        Pair(DataTypes::StringView("right"),     Constants::FunctionType::Right),
        Pair(DataTypes::StringView("reverse"),   Constants::FunctionType::Reverse),
        Pair(DataTypes::StringView("space"),     Constants::FunctionType::Space),
        Pair(DataTypes::StringView("nullif"),    Constants::FunctionType::NullIf),
        Pair(DataTypes::StringView("coalesce"),  Constants::FunctionType::Coalesce),
    };

    using ExpectedTypesArray = DataStructures::StaticArray<DataType, 5>;

    struct FunctionInfo {
        DataTypes::StringView name;
        Int minArgs;
        Int maxArgs;
        ExpectedTypesArray expectedTypes;
        DataType returnType;
        bool allowImplicitCast;
        bool additionalValidations;

        constexpr FunctionInfo(
            DataTypes::StringView&&  name,
            const Int minArgs,
            const Int maxArgs,
            const ExpectedTypesArray& expectedTypes,
            const DataType returnType,
            const bool allowImplicitCast,
            const bool additionalValidations
        ) : name(std::move(name)),
            minArgs(minArgs),
            maxArgs(maxArgs),
            expectedTypes(expectedTypes),
            returnType(returnType),
            allowImplicitCast(allowImplicitCast),
            additionalValidations(additionalValidations)
         {}

        constexpr FunctionInfo()
            : name(DataTypes::StringView()),
            minArgs(0),
            maxArgs(0),
            expectedTypes(ExpectedTypesArray()),
            returnType(DataType::Null),
            allowImplicitCast(false),
            additionalValidations(false){}
        constexpr ~FunctionInfo() = default;
    };

    static constexpr ConstexprDictionary FunctionInfoDictionary{
        Pair(
            Constants::FunctionType::GetDate,
            FunctionInfo(
                DataTypes::StringView("GETDATE"),
                0, 0, ExpectedTypesArray(),
                DataType::DateTime, false,
                false)
        ),
        Pair(
            Constants::FunctionType::NewGuid,
            FunctionInfo(
                DataTypes::StringView("NEWID"),
                0, 0, ExpectedTypesArray(),
                DataType::Guid, false,
                false
                )
        ),
        Pair(
            Constants::FunctionType::Concat,
            FunctionInfo(
                DataTypes::StringView("CONCAT"), 2,
                UNLIMITED_ARGS, {DataType::String},
                DataType::String, true,
                false
            )
        ),
        Pair(
            Constants::FunctionType::Length,
            FunctionInfo(
                DataTypes::StringView("LENGTH"), 1, 1,
                {DataType::String}, DataType::Int, false,
                false
            )
        ),
        Pair(
            Constants::FunctionType::AsciiValue,
            FunctionInfo(
                DataTypes::StringView("ASCII"), 1, 1,
                {DataType::String}, DataType::Int, false,
                false
            )
        ),
        Pair(Constants::FunctionType::Char,
            FunctionInfo(DataTypes::StringView("CHAR"), 1, 1, {DataType::Int}, DataType::String, false, false)),

        Pair(Constants::FunctionType::CharIndex,
            FunctionInfo(DataTypes::StringView("CHARINDEX"), 2, 3,
                {DataType::String, DataType::String, DataType::Int},
                DataType::Int, false, false)),

        Pair(Constants::FunctionType::Lower,
            FunctionInfo(DataTypes::StringView("LOWER"), 1, 1, {DataType::String}, DataType::String, false, false)),

        Pair(Constants::FunctionType::Upper,
            FunctionInfo(DataTypes::StringView("UPPER"), 1, 1, {DataType::String}, DataType::String, false, false)),

        Pair(Constants::FunctionType::Trim,
            FunctionInfo(DataTypes::StringView("TRIM"), 1, 1, {DataType::String}, DataType::String, false, false)),

        Pair(Constants::FunctionType::TrimLeft,
            FunctionInfo(DataTypes::StringView("TRIMLEFT"), 1, 1, {DataType::String}, DataType::String, false, false)),

        Pair(Constants::FunctionType::TrimRight,
            FunctionInfo(DataTypes::StringView("TRIMRIGHT"), 1, 1, {DataType::String}, DataType::String, false, false)),

        Pair(Constants::FunctionType::Replace,
            FunctionInfo(DataTypes::StringView("REPLACE"), 3, 3,
                {DataType::String, DataType::String, DataType::String},
                DataType::String, false, false)),

        Pair(Constants::FunctionType::Substr,
            FunctionInfo(DataTypes::StringView("SUBSTR"), 2, 3,
                {DataType::String, DataType::Int, DataType::Int},
                DataType::String, false, false)),

        Pair(Constants::FunctionType::Left,
            FunctionInfo(DataTypes::StringView("LEFT"), 2, 2,
                {DataType::String, DataType::Int},
                DataType::String, false, false)),

        Pair(Constants::FunctionType::Right,
            FunctionInfo(DataTypes::StringView("RIGHT"), 2, 2,
                {DataType::String, DataType::Int},
                DataType::String, false, false)),

        Pair(Constants::FunctionType::Reverse,
            FunctionInfo(DataTypes::StringView("REVERSE"), 1, 1, {DataType::String}, DataType::String, false, false)),

        Pair(Constants::FunctionType::Space,
            FunctionInfo(DataTypes::StringView("SPACE"), 1, 1, {DataType::Int}, DataType::String, false, false)),

        Pair(Constants::FunctionType::NullIf,
        FunctionInfo(DataTypes::StringView("NULLIF"), 2, 2, {DataType::String, DataType::String}, DataType::String, false, true)),

        Pair(Constants::FunctionType::Coalesce,
        FunctionInfo(DataTypes::StringView("COALESCE"), 2, UNLIMITED_ARGS, {DataType::String}, DataType::String, false, true)),
    };
}