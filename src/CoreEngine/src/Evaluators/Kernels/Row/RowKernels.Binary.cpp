#include "../../../../include/Evaluators/Kernels/Row/RowKernels.h"
#include "Evaluators/Kernels/Row/RowKernels.Binary.h"

namespace CoreEngine::RowKernels{
    constexpr void RegisterBool(BinaryKernelTable& table){
        constexpr auto CAST_BOOL_TYPE = static_cast<Int>(DataType::Bool);
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Equal)][CAST_BOOL_TYPE] = &BinaryComparisonKernel<bool, std::equal_to<bool>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::NotEqual)][CAST_BOOL_TYPE] = &BinaryComparisonKernel<bool, std::not_equal_to<bool>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Greater)][CAST_BOOL_TYPE] = &BinaryComparisonKernel<bool, std::greater<bool>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::GreaterEqual)][CAST_BOOL_TYPE] = &BinaryComparisonKernel<bool, std::greater_equal<bool>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Less)][CAST_BOOL_TYPE] = &BinaryComparisonKernel<bool, std::less<bool>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::LessEqual)][CAST_BOOL_TYPE] = &BinaryComparisonKernel<bool, std::less_equal<bool>>;
    }

    // Case-insensitive string equality, shaped like std::equal_to so it plugs straight
    // into BinaryComparisonKernel. Routes through String::Compare -- the same public
    // entry the Value path uses for EqualsIgnoreOrdinalCase.

    constexpr void RegisterString(BinaryKernelTable& table){
        constexpr auto CAST_STR_TYPE = static_cast<Int>(DataType::String);
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Add)][CAST_STR_TYPE] = &BinaryArithmeticKernel<DataTypes::String, std::plus<DataTypes::String>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Equal)][CAST_STR_TYPE] = &BinaryComparisonKernel<DataTypes::String, std::equal_to<DataTypes::String>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::NotEqual)][CAST_STR_TYPE] = &BinaryComparisonKernel<DataTypes::String, std::not_equal_to<DataTypes::String>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Greater)][CAST_STR_TYPE] = &BinaryComparisonKernel<DataTypes::String, std::greater<DataTypes::String>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::GreaterEqual)][CAST_STR_TYPE] = &BinaryComparisonKernel<DataTypes::String, std::greater_equal<DataTypes::String>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Less)][CAST_STR_TYPE] = &BinaryComparisonKernel<DataTypes::String, std::less<DataTypes::String>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::LessEqual)][CAST_STR_TYPE] = &BinaryComparisonKernel<DataTypes::String, std::less_equal<DataTypes::String>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::EqualIgnoreOrdinalCase)][CAST_STR_TYPE] = &BinaryComparisonKernel<DataTypes::String, DataTypes::StringEqualsIgnoreCase>;

        //TODO add starts with / ends with / contains kernels
    }

    constexpr void RegisterDateTime(BinaryKernelTable& table){
        constexpr auto CAST_DATETIME_TYPE = static_cast<Int>(DataType::DateTime);

        table.cells[static_cast<Int>(Expressions::BinaryOperator::Equal)][CAST_DATETIME_TYPE] = &BinaryComparisonKernel<DataTypes::DateTime, std::equal_to<DataTypes::DateTime>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::NotEqual)][CAST_DATETIME_TYPE] = &BinaryComparisonKernel<DataTypes::DateTime, std::not_equal_to<DataTypes::DateTime>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Greater)][CAST_DATETIME_TYPE] = &BinaryComparisonKernel<DataTypes::DateTime, std::greater<DataTypes::DateTime>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::GreaterEqual)][CAST_DATETIME_TYPE] = &BinaryComparisonKernel<DataTypes::DateTime, std::greater_equal<DataTypes::DateTime>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Less)][CAST_DATETIME_TYPE] = &BinaryComparisonKernel<DataTypes::DateTime, std::less<DataTypes::DateTime>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::LessEqual)][CAST_DATETIME_TYPE] = &BinaryComparisonKernel<DataTypes::DateTime, std::less_equal<DataTypes::DateTime>>;
    }

    constexpr void RegisterDecimal(BinaryKernelTable& table){
        constexpr auto CAST_DECIMAL_TYPE = static_cast<Int>(DataType::Decimal);

        table.cells[static_cast<Int>(Expressions::BinaryOperator::Add)][CAST_DECIMAL_TYPE] = &BinaryArithmeticKernel<DataTypes::Decimal, std::plus<DataTypes::Decimal>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Subtract)][CAST_DECIMAL_TYPE] = &BinaryArithmeticKernel<DataTypes::Decimal, std::minus<DataTypes::Decimal>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Multiply)][CAST_DECIMAL_TYPE] = &BinaryArithmeticKernel<DataTypes::Decimal, std::multiplies<DataTypes::Decimal>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Divide)][CAST_DECIMAL_TYPE] = &BinaryDivideKernel<DataTypes::Decimal>;
        // table.cells[static_cast<Int>(Expressions::BinaryOperator::Modulo)][CAST_DECIMAL_TYPE] = &BinaryModuloKernel<DataTypes::Decimal>;

        table.cells[static_cast<Int>(Expressions::BinaryOperator::Equal)][CAST_DECIMAL_TYPE] = &BinaryComparisonKernel<DataTypes::Decimal, std::equal_to<DataTypes::Decimal>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::NotEqual)][CAST_DECIMAL_TYPE] = &BinaryComparisonKernel<DataTypes::Decimal, std::not_equal_to<DataTypes::Decimal>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Greater)][CAST_DECIMAL_TYPE] = &BinaryComparisonKernel<DataTypes::Decimal, std::greater<DataTypes::Decimal>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::GreaterEqual)][CAST_DECIMAL_TYPE] = &BinaryComparisonKernel<DataTypes::Decimal, std::greater_equal<DataTypes::Decimal>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Less)][CAST_DECIMAL_TYPE] = &BinaryComparisonKernel<DataTypes::Decimal, std::less<DataTypes::Decimal>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::LessEqual)][CAST_DECIMAL_TYPE] = &BinaryComparisonKernel<DataTypes::Decimal, std::less_equal<DataTypes::Decimal>>;
    }

    constexpr void RegisterJson(BinaryKernelTable& table){

    }

    constexpr void RegisterGuid(BinaryKernelTable& table){
        constexpr auto CAST_GUID_TYPE = static_cast<Int>(DataType::Guid);

        table.cells[static_cast<Int>(Expressions::BinaryOperator::Equal)][CAST_GUID_TYPE] = &BinaryComparisonKernel<DataTypes::Guid, std::equal_to<DataTypes::Guid>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::NotEqual)][CAST_GUID_TYPE] = &BinaryComparisonKernel<DataTypes::Guid, std::not_equal_to<DataTypes::Guid>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Greater)][CAST_GUID_TYPE] = &BinaryComparisonKernel<DataTypes::Guid, std::greater<DataTypes::Guid>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::GreaterEqual)][CAST_GUID_TYPE] = &BinaryComparisonKernel<DataTypes::Guid, std::greater_equal<DataTypes::Guid>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Less)][CAST_GUID_TYPE] = &BinaryComparisonKernel<DataTypes::Guid, std::less<DataTypes::Guid>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::LessEqual)][CAST_GUID_TYPE] = &BinaryComparisonKernel<DataTypes::Guid, std::less_equal<DataTypes::Guid>>;
    }

    template<DataTypes::IsInteger T>
    constexpr void RegisterIntegral(BinaryKernelTable& table, const DataType dataType){
        const auto castType = static_cast<Int>(dataType);

        table.cells[static_cast<Int>(Expressions::BinaryOperator::Add)][castType] = &BinaryArithmeticKernel<T, std::plus<T>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Subtract)][castType] = &BinaryArithmeticKernel<T, std::minus<T>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Multiply)][castType] = &BinaryArithmeticKernel<T, std::multiplies<T>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Divide)][castType] = &BinaryDivideKernel<T>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Modulo)][castType] = &BinaryModuloKernel<T>;

        table.cells[static_cast<Int>(Expressions::BinaryOperator::Equal)][castType] = &BinaryComparisonKernel<T, std::equal_to<T>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::NotEqual)][castType] = &BinaryComparisonKernel<T, std::not_equal_to<T>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Greater)][castType] = &BinaryComparisonKernel<T, std::greater<T>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::GreaterEqual)][castType] = &BinaryComparisonKernel<T, std::greater_equal<T>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::Less)][castType] = &BinaryComparisonKernel<T, std::less<T>>;
        table.cells[static_cast<Int>(Expressions::BinaryOperator::LessEqual)][castType] = &BinaryComparisonKernel<T, std::less_equal<T>>;
    }


    constexpr BinaryKernelTable RegisterBinaryKernels(){
        BinaryKernelTable table {};
        RegisterBool(table);
        RegisterIntegral<TinyInt>(table, DataType::TinyInt);
        RegisterIntegral<SmallInt>(table, DataType::SmallInt);
        RegisterIntegral<Int>(table, DataType::Int);
        RegisterIntegral<BigInt>(table, DataType::BigInt);
        RegisterDecimal(table);
        RegisterJson(table);
        RegisterString(table);
        RegisterGuid(table);
        RegisterDateTime(table);
        return table;
    }

    inline constexpr auto BINARY_KERNEL_TABLE = RegisterBinaryKernels();

    Expressions::RowKernelFunction LookupBinaryKernel(Expressions::BinaryOperator op, DataType operandType){
        return BINARY_KERNEL_TABLE.cells[static_cast<Int>(op)][static_cast<Int>(operandType)];
    }
}