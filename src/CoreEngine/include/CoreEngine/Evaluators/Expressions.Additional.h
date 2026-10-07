#pragma once
#include <CoreEngine/DatabaseConstants.h>

#include <utility>

#include <Systemic/DataStructures/ConstexprDictionary.h>
#include <Systemic/DataStructures/StaticArray.h>
#define UNLIMITED_ARGS (-1)

namespace Expressions {
    class Expression;

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

    static inline constexpr Int EXPRESSION_TYPE_COUNT = Reflection::EnumCount<ExpressionType>;

    enum class LogicalType {
        And = 0,
        Or = 1,
        Not = 2
    };
    static inline constexpr Int LOGICAL_TYPE_COUNT = Reflection::EnumCount<LogicalType>;

    enum class BinaryOperator : UnsignedTinyInt{
        Equal                  [[= DataTypes::Traits::Comparison{}]] = 0,
        NotEqual               [[= DataTypes::Traits::Comparison{}]] = 1,
        Greater                [[= DataTypes::Traits::Comparison{}]] = 2,
        GreaterEqual           [[= DataTypes::Traits::Comparison{}]] = 3,
        Less                   [[= DataTypes::Traits::Comparison{}]] = 4,
        LessEqual              [[= DataTypes::Traits::Comparison{}]] = 5,
        Add                    [[= DataTypes::Traits::Arithmetic{}, = DataTypes::Traits::Concatenation{}]] = 6,
        Subtract               [[= DataTypes::Traits::Arithmetic{}]] = 7,
        Multiply               [[= DataTypes::Traits::Arithmetic{}]] = 8,
        Divide                 [[= DataTypes::Traits::Arithmetic{}]] = 9,
        Modulo                 [[= DataTypes::Traits::Arithmetic{}]] = 10,
        EqualIgnoreOrdinalCase [[= DataTypes::Traits::CaseInsensitiveComparison{}]] = 11
    };

    static inline constexpr Int BINARY_OPERATORS_COUNT = Reflection::EnumCount<BinaryOperator>;

    struct DivideTag{};
    struct ModuloTag{};

    using BinaryOperatorFunctors = DataTypes::TypeList<
        std::equal_to<>,
        std::not_equal_to<>,
        std::greater<>,
        std::greater_equal<>,
        std::less<>,
        std::less_equal<>,
        std::plus<>,
        std::minus<>,
        std::multiplies<>,
        DivideTag,
        ModuloTag,
        DataTypes::StringEqualsIgnoreCase
    >;

    static_assert(
        BinaryOperatorFunctors::SIZE == BINARY_OPERATORS_COUNT,
        "Invalid Count of binary operator functors"
    );

    enum class BranchType {
        Switch = 0,
        Ternary = 1,
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