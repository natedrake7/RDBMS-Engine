#include "../../../../include/Evaluators/Kernels/Row/RowKernels.h"
#include "Evaluators/Kernels/Row/RowKernels.Binary.h"

namespace CoreEngine::RowKernels{
        void BinaryStringAdditionKernel(
        const Expressions::Expression* self,
        const Expressions::EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        const auto* binaryExpr = self->AsBinary();
        bool leftNull = false, rightNull = false;
        DataTypes::String left, right;

        binaryExpr->left->rowKernel(binaryExpr->left, context, &left, &leftNull);
        binaryExpr->right->rowKernel(binaryExpr->right, context, &right, &rightNull);

        if (leftNull || rightNull){
            *outNull = true;
            return;
        }   // strict propagation

        new (outVal) DataTypes::String(DataTypes::String::Concat(context.allocator, left, right));
        *outNull = false;
    }

    // Single definition for the extern table declared in RowKernels.h.
    Expressions::RowKernelFunction RowBinaryKernelTable[BINARY_OPERATIONS_COUNT][DATATYPE_COUNT] = {};

    void RegisterBool(){
        constexpr auto CAST_BOOL_TYPE = static_cast<Int>(DataType::Bool);
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Equal)][CAST_BOOL_TYPE] = &BinaryComparisonKernel<bool, std::equal_to<bool>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::NotEqual)][CAST_BOOL_TYPE] = &BinaryComparisonKernel<bool, std::not_equal_to<bool>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Greater)][CAST_BOOL_TYPE] = &BinaryComparisonKernel<bool, std::greater<bool>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::GreaterEqual)][CAST_BOOL_TYPE] = &BinaryComparisonKernel<bool, std::greater_equal<bool>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Less)][CAST_BOOL_TYPE] = &BinaryComparisonKernel<bool, std::less<bool>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::LessEqual)][CAST_BOOL_TYPE] = &BinaryComparisonKernel<bool, std::less_equal<bool>>;
    }

    // Register the full arithmetic + comparison row for one integral type.


    void RegisterString(){
        constexpr auto CAST_STR_TYPE = static_cast<Int>(DataType::String);
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Add)][CAST_STR_TYPE] = &BinaryStringAdditionKernel;

        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Equal)][CAST_STR_TYPE] = &BinaryComparisonKernel<DataTypes::String, std::equal_to<DataTypes::String>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::NotEqual)][CAST_STR_TYPE] = &BinaryComparisonKernel<DataTypes::String, std::not_equal_to<DataTypes::String>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Greater)][CAST_STR_TYPE] = &BinaryComparisonKernel<DataTypes::String, std::greater<DataTypes::String>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::GreaterEqual)][CAST_STR_TYPE] = &BinaryComparisonKernel<DataTypes::String, std::greater_equal<DataTypes::String>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Less)][CAST_STR_TYPE] = &BinaryComparisonKernel<DataTypes::String, std::less<DataTypes::String>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::LessEqual)][CAST_STR_TYPE] = &BinaryComparisonKernel<DataTypes::String, std::less_equal<DataTypes::String>>;

        //TODO add ignore ordinal case, starts with etc kernel
    }

    void RegisterDateTime(){
        constexpr auto CAST_DATETIME_TYPE = static_cast<Int>(DataType::DateTime);

        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Equal)][CAST_DATETIME_TYPE] = &BinaryComparisonKernel<DataTypes::DateTime, std::equal_to<DataTypes::DateTime>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::NotEqual)][CAST_DATETIME_TYPE] = &BinaryComparisonKernel<DataTypes::DateTime, std::not_equal_to<DataTypes::DateTime>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Greater)][CAST_DATETIME_TYPE] = &BinaryComparisonKernel<DataTypes::DateTime, std::greater<DataTypes::DateTime>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::GreaterEqual)][CAST_DATETIME_TYPE] = &BinaryComparisonKernel<DataTypes::DateTime, std::greater_equal<DataTypes::DateTime>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Less)][CAST_DATETIME_TYPE] = &BinaryComparisonKernel<DataTypes::DateTime, std::less<DataTypes::DateTime>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::LessEqual)][CAST_DATETIME_TYPE] = &BinaryComparisonKernel<DataTypes::DateTime, std::less_equal<DataTypes::DateTime>>;
    }

    void RegisterDecimal(){

    }

    void RegisterJson(){

    }

    void RegisterGuid(){
        constexpr auto CAST_GUID_TYPE = static_cast<Int>(DataType::Guid);

        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Equal)][CAST_GUID_TYPE] = &BinaryComparisonKernel<DataTypes::Guid, std::equal_to<DataTypes::Guid>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::NotEqual)][CAST_GUID_TYPE] = &BinaryComparisonKernel<DataTypes::Guid, std::not_equal_to<DataTypes::Guid>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Greater)][CAST_GUID_TYPE] = &BinaryComparisonKernel<DataTypes::Guid, std::greater<DataTypes::Guid>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::GreaterEqual)][CAST_GUID_TYPE] = &BinaryComparisonKernel<DataTypes::Guid, std::greater_equal<DataTypes::Guid>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::Less)][CAST_GUID_TYPE] = &BinaryComparisonKernel<DataTypes::Guid, std::less<DataTypes::Guid>>;
        RowBinaryKernelTable[static_cast<Int>(Expressions::BinaryOperator::LessEqual)][CAST_GUID_TYPE] = &BinaryComparisonKernel<DataTypes::Guid, std::less_equal<DataTypes::Guid>>;
    }

    // Call once at engine startup, before any expression is bound.
    void RegisterBinaryKernels(){
        RegisterBool();
        RegisterIntegral<TinyInt>(DataType::TinyInt);
        RegisterIntegral<SmallInt>(DataType::SmallInt);
        RegisterIntegral<Int>(DataType::Int);
        RegisterIntegral<BigInt>(DataType::BigInt);
        RegisterDecimal();
        RegisterJson();
        RegisterString();
        RegisterGuid();
        RegisterDateTime();

        // TODO: Decimal (no operator% -- arithmetic + comparison only, via its own
        //       methods), DateTime/Guid (comparison only), String (comparison only).
    }

}