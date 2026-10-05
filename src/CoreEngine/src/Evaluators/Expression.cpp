#include "../../include/Evaluators/Expression.h"

#include "../../../Systemic/include/Coercions/Coercions.h"
#include "../../../Systemic/include/DataTypes/Value.h"
#include "../../../Systemic/include/MaterializedRow.h"
#include "../../../Systemic/include/Functions/StringFunctions.h"
#include "../../include/DataStorage/Row/Row.h"
#include "Plugin.h"
#include "Contexts/ExecutionContext.h"
#include "DataStructures/PolymorphicArray.h"
#include "DataTypes/DateTime.h"

#include "../../../Systemic/include/DataTypes/Variable.h"
#include "DataStorage/Table.h"
#include "DataTypes/DataTypes.StaticData.h"
#include "../../include/Evaluators/Kernels/Row/RowKernels.h"
#include "../../include/Evaluators/Kernels/Vectorized/VectorizedKernels.h"
#include "Contexts/OutputSchema.h"
#include "Evaluators/Kernels/Row/RowKernels.Binary.h"
#include "Evaluators/Kernels/Row/RowKernels.Cast.h"
#include "Evaluators/Kernels/Vectorized/Vectorized.JumpTables.h"
#include "Vectorization/Vectorization.h"

namespace Expressions{
    static constexpr ConstexprDictionary FunctionDictionary{
        // Date Functions
        Pair(Constants::FunctionType::GetDate,    &FunctionExpression::GetDate),
        Pair(Constants::FunctionType::DateAdd,    &FunctionExpression::GetDate),
        Pair(Constants::FunctionType::DateDiff,   &FunctionExpression::GetDate),
        Pair(Constants::FunctionType::DatePart,   &FunctionExpression::GetDate),
        Pair(Constants::FunctionType::Year,       &FunctionExpression::GetDate),
        Pair(Constants::FunctionType::Month,      &FunctionExpression::GetDate),
        Pair(Constants::FunctionType::Day,        &FunctionExpression::GetDate),
        // Guid
        Pair(Constants::FunctionType::NewGuid,    &FunctionExpression::NewGuid),
        // String
        Pair(Constants::FunctionType::Concat,     &FunctionExpression::Concat),
        Pair(Constants::FunctionType::Length,     &FunctionExpression::Length),
        Pair(Constants::FunctionType::AsciiValue, &FunctionExpression::AsciiValue),
        Pair(Constants::FunctionType::Char,       &FunctionExpression::Char),
        Pair(Constants::FunctionType::CharIndex,  &FunctionExpression::CharIndex),
        Pair(Constants::FunctionType::Lower,      &FunctionExpression::Lower),
        Pair(Constants::FunctionType::Upper,      &FunctionExpression::Upper),
        Pair(Constants::FunctionType::Trim,       &FunctionExpression::Trim),
        Pair(Constants::FunctionType::TrimLeft,   &FunctionExpression::TrimLeft),
        Pair(Constants::FunctionType::TrimRight,  &FunctionExpression::TrimRight),
        Pair(Constants::FunctionType::Replace,    &FunctionExpression::Replace),
        Pair(Constants::FunctionType::Substr,     &FunctionExpression::Substr),
        Pair(Constants::FunctionType::Left,       &FunctionExpression::Left),
        Pair(Constants::FunctionType::Right,      &FunctionExpression::Right),
        Pair(Constants::FunctionType::Reverse,    &FunctionExpression::Reverse),
        Pair(Constants::FunctionType::Space,      &FunctionExpression::Space),
        // Null Handling
        Pair(Constants::FunctionType::NullIf,     &FunctionExpression::NullIf),
        Pair(Constants::FunctionType::Coalesce,   &FunctionExpression::Coalesce),
    };

    // 2 entries → N=2
    static constexpr ConstexprDictionary FunctionAdditionalValidationsDictionary{
        Pair(Constants::FunctionType::NullIf,   &FunctionExpression::ValidateNullIf),
        Pair(Constants::FunctionType::Coalesce, &FunctionExpression::ValidateCoalesce),
    };

    EvaluationContext::EvaluationContext(
        const EvaluationContextType type,
        const ::Memory::IAllocator* allocator
    ):  _rids(nullptr), _pages(nullptr),
        _executionContext(nullptr),
        _allocator(allocator),
        _type(type) {}

    EvaluationContext::EvaluationContext(
        const EvaluationContextType type,
        const CoreEngine::ExecutionContext* executionContext
    ):  _rids(nullptr), _pages(nullptr),
        _executionContext(executionContext),
        _allocator(executionContext->GetAllocator()),
        _type(type){}

    EvaluationContext::EvaluationContext(
        const CoreEngine::StorageTypes::RID* row,
        const CoreEngine::ExecutionContext* executionContext
    ) : _rids(row), _pages(nullptr),
        _executionContext(executionContext), _allocator(executionContext->GetAllocator()),
        _type(EvaluationContextType::SingleRow){}

    void Expression::SetIndex(const column_index_t index){ this->ordinalPosition = index; }

    void ColumnExpression::ResolveReference(ColumnExpression* self, const CoreEngine::OutputSchema* schema){
        const auto identity = CoreEngine::ColumnIdentity::Base(self->_slotIndex, self->ordinalPosition);
        self->_boundReference._childIndex = 0;
        self->_boundReference._position = schema->IndexOf(identity);
    }

    ColumnExpression::ColumnExpression(const DataTypes::String& name, const DataTypes::String& tableAlias){
        this->alias = name;
        this->tableAlias = tableAlias;

        this->_slotIndex = DEFAULT_SLOT_INDEX;
        this->tableId = INVALID_TABLE_ID;
        this->columnId = INVALID_COLUMN_ID;
        this->ordinalPosition = 0;
        this->size = 0;
        this->returnType = DataType::Null;
        this->expressionType = ColumnExpression::TYPE;
    }

    ColumnExpression::ColumnExpression(DataTypes::String&& name, DataTypes::String&& tableAlias)
        :   alias(std::move(name)), tableAlias(std::move(tableAlias)),
            tableId(INVALID_TABLE_ID), columnId(INVALID_COLUMN_ID),
            returnType(DataType::Null), size(0) {
        this->_slotIndex = DEFAULT_SLOT_INDEX;
        this->ordinalPosition = 0;
        this->expressionType = ColumnExpression::TYPE;
    }

    ColumnExpression::ColumnExpression(const column_index_t index, const DataType dataType){
        this->_slotIndex = DEFAULT_SLOT_INDEX;
        this->ordinalPosition = index;
        this->size = 0;
        this->returnType = dataType;
        this->tableId = INVALID_TABLE_ID;
        this->columnId = INVALID_COLUMN_ID;
        this->expressionType = ColumnExpression::TYPE;
    }

    void ColumnExpression::BindVectorizedKernel(Expression* self, const CoreEngine::OutputSchema* schema){
        auto* columnExpression = self->As<ColumnExpression>();
        columnExpression->vectorizedKernel = CoreEngine::VectorizedKernels::ColumnScanKernel;
        ColumnExpression::ResolveReference(columnExpression, schema);
    }

    void ColumnExpression::BindRowKernel(Expression* self){
        auto* columnExpression = self->As<ColumnExpression>();
        switch (columnExpression->returnType){
        case DataType::String:
            columnExpression->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<DataTypes::StringValue>;
            break;
        case DataType::Bool:
            columnExpression->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<bool>;
            break;
        case DataType::TinyInt:
            columnExpression->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<TinyInt>;
            break;
        case DataType::SmallInt:
            columnExpression->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<SmallInt>;
            break;
        case DataType::Int:
            columnExpression->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<Int>;
            break;
        case DataType::BigInt:
            columnExpression->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<BigInt>;
            break;
        case DataType::Decimal:
            columnExpression->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<DataTypes::Decimal>;
            break;
        case DataType::DateTime:
            columnExpression->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<DataTypes::DateTime>;
            break;
        case DataType::Guid:
            columnExpression->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<DataTypes::Guid>;
            break;
        case DataType::Json:
            columnExpression->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<DataTypes::JsonBinary>;
            break;
        case DataType::Null:
            break;
        }
    }

    DataType ColumnExpression::GetReturnType() const{ return this->returnType; }

    bool ColumnExpression::HasTableAlias() const { return !this->tableAlias.Empty();}

    ConstantExpression::ConstantExpression(const Value &value)
        : value(value){
        this->expressionType = ConstantExpression::TYPE;
    }

    ConstantExpression::ConstantExpression(Value &value)
        : value(std::move(value)){
        this->expressionType = ConstantExpression::TYPE;
    }

    ConstantExpression::ConstantExpression(Value&& value)
        : value(std::move(value)){
        this->expressionType = ConstantExpression::TYPE;
    }

    void ConstantExpression::BindVectorizedKernel(Expression* self, const CoreEngine::OutputSchema* schema){
        auto* constantExpression = self->As<ConstantExpression>();
        constantExpression->vectorizedKernel = CoreEngine::VectorizedKernels::JumpTables::GetConstantKernel(constantExpression->value.GetType());
    }

    void ConstantExpression::BindRowKernel(Expression* self){
        auto* constantExpression = self->As<ConstantExpression>();
        switch (constantExpression->value.GetType()){
        case DataType::Bool:
            constantExpression->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<bool>;
            break;
        case DataType::TinyInt:
            constantExpression->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<TinyInt>;
            break;
        case DataType::SmallInt:
            constantExpression->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<SmallInt>;
            break;
        case DataType::Int:
            constantExpression->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<Int>;
            break;
        case DataType::BigInt:
            constantExpression->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<BigInt>;
            break;
        case DataType::DateTime:
            constantExpression->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<DataTypes::DateTime>;
            break;
        case DataType::Guid:
            constantExpression->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<DataTypes::Guid>;
            break;
        case DataType::Decimal:
            constantExpression->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<DataTypes::Decimal>;
            break;
        case DataType::String:
            constantExpression->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<DataTypes::StringValue>;
            break;
        case DataType::Json:
            constantExpression->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<DataTypes::JsonBinary>;
            break;
        case DataType::Null:
            constantExpression->rowKernel = &CoreEngine::RowKernels::ConstantScanNullKernel;
            break;
        default:
            break;
        }
    }

    DataType ConstantExpression::GetReturnType() const{ return this->value.GetType(); }

    LogicalExpression::LogicalExpression(
        Expression *leftExpression,
        Expression *RightExpression,
        const LogicalType logicalType
    ){
        this->logicalType = logicalType;
        this->left = leftExpression;
        this->right = RightExpression;
        this->expressionType = LogicalExpression::TYPE;
    }

    bool LogicalExpression::IsOr() const{ return this->logicalType == LogicalType::Or; }


    bool LogicalExpression::IsAnd() const{ return this->logicalType == LogicalType::And; }

    bool LogicalExpression::IsNot() const{ return this->logicalType == LogicalType::Not; }

    bool LogicalExpression::HasAtLeastOneConstant() const{ return this->left->Is<ConstantExpression>() || this->right->Is<ConstantExpression>(); }

    void LogicalExpression::BindRowKernel(Expression* self){
        auto* logicalExpression = self->As<LogicalExpression>();
        Expressions::BindExpressionRowKernel(logicalExpression->left);
        Expressions::BindExpressionRowKernel(logicalExpression->right);

        switch (logicalExpression->logicalType) {
        case LogicalType::And:
            logicalExpression->rowKernel = &CoreEngine::RowKernels::LogicalAndKernel;
            break;
        case LogicalType::Or:
            logicalExpression->rowKernel = &CoreEngine::RowKernels::LogicalOrKernel;
            break;
        case LogicalType::Not:
            logicalExpression->rowKernel = &CoreEngine::RowKernels::LogicalNotKernel;
            break;
        }
    }

    void LogicalExpression::BindVectorizedKernel(Expression* self, const CoreEngine::OutputSchema* schema){
        auto* logicalExpression = self->As<LogicalExpression>();
        Expressions::BindAndResolveExpressionKernel(logicalExpression->left, schema);
        Expressions::BindAndResolveExpressionKernel(logicalExpression->right, schema);
        logicalExpression->vectorizedKernel = CoreEngine::VectorizedKernels::JumpTables::GetLogicalKernel(logicalExpression->logicalType);
    }

    constexpr DataType LogicalExpression::GetReturnType() { return DataType::Bool; }

    bool BinaryExpression::ValidateAddition()const{
        const auto leftType = GetExpressionReturnType(this->left);
        const auto rightType = GetExpressionReturnType(this->right);

        switch (PromoteType(leftType, rightType)) {
        case DataType::TinyInt:
        case DataType::SmallInt:
        case DataType::Int:
        case DataType::BigInt:
        case DataType::Decimal:
        case DataType::String:
        case DataType::Bool:
            return true;
        case DataType::DateTime:
        case DataType::Guid:
        case DataType::Null:
        default:
            return false;
        }
    }

    bool BinaryExpression::ValidateSubtraction() const{
        const auto leftType = GetExpressionReturnType(this->left);
        const auto rightType = GetExpressionReturnType(this->right);

        switch (PromoteType(leftType, rightType)) {
        case DataType::TinyInt:
        case DataType::SmallInt:
        case DataType::Int:
        case DataType::BigInt:
        case DataType::Decimal:
        case DataType::Bool:
            return true;
        case DataType::String:
        case DataType::DateTime:
        case DataType::Guid:
        case DataType::Null:
        default:
            return false;
        }
    }

    bool BinaryExpression::ValidateMultiplication() const{
        const auto leftType = GetExpressionReturnType(this->left);
        const auto rightType = GetExpressionReturnType(this->right);

        switch (PromoteType(leftType, rightType)) {
        case DataType::TinyInt:
        case DataType::SmallInt:
        case DataType::Int:
        case DataType::BigInt:
        case DataType::Decimal:
        case DataType::Bool:
            return true;
        case DataType::String:
        case DataType::DateTime:
        case DataType::Guid:
        case DataType::Null:
        default:
            return false;
        }
    }

    bool BinaryExpression::ValidateDivision() const{
        const auto leftType = GetExpressionReturnType(this->left);
        const auto rightType = GetExpressionReturnType(this->right);

        switch (PromoteType(leftType, rightType)) {
        case DataType::TinyInt:
        case DataType::SmallInt:
        case DataType::Int:
        case DataType::BigInt:
        case DataType::Decimal:
        case DataType::Bool:
            return true;
        case DataType::String:
        case DataType::DateTime:
        case DataType::Guid:
        case DataType::Null:
        default:
            return false;
        }
    }

    bool BinaryExpression::ValidateModulo() const{
        const auto leftType = GetExpressionReturnType(this->left);
        const auto rightType = GetExpressionReturnType(this->right);

        switch (PromoteType(leftType, rightType)) {
        case DataType::TinyInt:
        case DataType::SmallInt:
        case DataType::Int:
        case DataType::BigInt:
        case DataType::Bool:
        case DataType::Decimal:
            return true;
        case DataType::String:
        case DataType::DateTime:
        case DataType::Guid:
        case DataType::Null:
        default:
            return false;
        }
    }

    BinaryExpression::BinaryExpression(Expression *left, Expression *right, const BinaryOperator operation){
        this->left = left;
        this->right = right;
        this->operation = operation;
        this->expressionType = BinaryExpression::TYPE;
    }

    bool BinaryExpression::ValidateOperation() const {
        switch (this->operation) {
        case BinaryOperator::Add:
            return this->ValidateAddition();
        case BinaryOperator::Subtract:
            return this->ValidateSubtraction();
        case BinaryOperator::Multiply:
            return this->ValidateMultiplication();
        case BinaryOperator::Divide:
            return this->ValidateDivision();
        case BinaryOperator::Modulo:
            return this->ValidateModulo();
        case BinaryOperator::Equal:
        case BinaryOperator::EqualIgnoreOrdinalCase:
        case BinaryOperator::NotEqual:
        case BinaryOperator::Greater:
        case BinaryOperator::GreaterEqual:
        case BinaryOperator::Less:
        case BinaryOperator::LessEqual:
            return true;
        default:
            throw std::runtime_error("BinaryExpression::ValidateOperation: Unknown operator" + std::to_string(static_cast<Int>(this->operation)));
        }
    }

    void BinaryExpression::BindRowKernel(Expression* self){
        auto* binaryExpression = self->As<BinaryExpression>();

        Expressions::BindExpressionRowKernel(binaryExpression->left);
        Expressions::BindExpressionRowKernel(binaryExpression->right);

        const auto operandType = GetExpressionReturnType(binaryExpression->left);
        binaryExpression->rowKernel = CoreEngine::RowKernels::LookupBinaryKernel(
            binaryExpression->operation, operandType
        );
    }

    void BinaryExpression::BindVectorizedKernel(Expression* self, const CoreEngine::OutputSchema* schema){
        auto* binaryExpression = self->As<BinaryExpression>();
        Expressions::BindAndResolveExpressionKernel(binaryExpression->left, schema);
        Expressions::BindAndResolveExpressionKernel(binaryExpression->right, schema);

        const auto operandType = PromoteType(
            GetExpressionReturnType(binaryExpression->left),
            GetExpressionReturnType(binaryExpression->right)
        );

        binaryExpression->vectorizedKernel = CoreEngine::VectorizedKernels::JumpTables::GetBinaryKernel(
            binaryExpression->operation, operandType
        );
    }

    //TODO Implement field logical operations.
    DataType BinaryExpression::GetReturnType() const{
        switch (this->operation) {
        case BinaryOperator::Add:
        case BinaryOperator::Subtract:
        case BinaryOperator::Multiply:
        case BinaryOperator::Divide:
        case BinaryOperator::Modulo: {
            const auto leftType = GetExpressionReturnType(this->left);
            const auto rightType = GetExpressionReturnType(this->right);
            return PromoteType(leftType, rightType);
        }
        case BinaryOperator::Equal:
        case BinaryOperator::EqualIgnoreOrdinalCase:
        case BinaryOperator::NotEqual:
        case BinaryOperator::Greater:
        case BinaryOperator::GreaterEqual:
        case BinaryOperator::Less:
        case BinaryOperator::LessEqual:
            return DataType::Bool;
        default:
            throw std::runtime_error("BinaryExpression::GetReturnType: Unknown operator" + std::to_string(static_cast<int>(this->operation)));
        }
    }

    bool FunctionExpression::ValidateUnlimitedArgumentTypes(const FunctionInfo& info, DataTypes::String& errorMessage)const{
        const auto& expectedType = info.expectedTypes.First();

        for (int i = 0;i < this->arguments.Size(); i++)
            if (!FunctionExpression::ValidateReturnType(
                info, errorMessage, expectedType,
                GetExpressionReturnType(this->arguments[i]), i
            ))
                return false;

        return true;
    }

    bool FunctionExpression::ValidateArgumentTypes(const FunctionInfo &info, DataTypes::String& errorMessage) const{
        for (int i = 0;i < this->arguments.Size(); i++)
            if (!FunctionExpression::ValidateReturnType(info, errorMessage, info.expectedTypes[i], GetExpressionReturnType(this->arguments[i]), i))
                return false;

        return true;
    }

    bool FunctionExpression::ValidateReturnType(
        const FunctionInfo &info,
        DataTypes::String& errorMessage,
        const DataType expectedType,
        const DataType returnType,
        const Int index
    ) {
        if (returnType == DataType::Null) {
            errorMessage = errorMessage.ConcatInPlace(
                "Function: ",
                info.name,
                " has an argument at position ",
                std::to_string(index + 1),
                " with invalid type"
            );

            return false;
        }

        if (!DataTypes::Coercions::IsCoercionAllowed(returnType, expectedType, info.allowImplicitCast)) {
            errorMessage = errorMessage.ConcatInPlace(
                "Function: ",
                info.name,
                " expects argument ",
                std::to_string(index + 1),
                " to be of type: ",
                SQL_TYPES_NAMES[static_cast<int>(expectedType)],
                ", but got type: ",
                SQL_TYPES_NAMES[static_cast<int>(returnType)]
            );
            return false;
        }

        return true;
    }

    void FunctionExpression::ConstructInvalidCastMessage(DataTypes::String& errorMessage, const DataType fromType, const DataType toType) {
        errorMessage = errorMessage.ConcatInPlace(
            "Cannot cast safely type: ",
            SQL_TYPES_NAMES[static_cast<int>(fromType)],
            " to type: ",
            SQL_TYPES_NAMES[static_cast<int>(toType)]
        );
    }

    bool FunctionExpression::PerformAdditionalValidations(DataTypes::String& errorMessage)const {
        return FunctionAdditionalValidationsDictionary.Get(this->functionType)(this->arguments, errorMessage);
    }

    FunctionExpression::FunctionExpression(
        const Constants::FunctionType functionType,
        DataStructures::PolymorphicArray<Expression*>& arguments
    ) {
        this->functionType = functionType;
        this->arguments = std::move(arguments);
        this->expressionType = FunctionExpression::TYPE;
    }

    Value FunctionExpression::Evaluate(const EvaluationContext& context) const {
        // DataStructures::PolymorphicArray<Value> evaluatedArguments(context.allocator, this->arguments.Size());
        // for (const auto& arg : this->arguments)
        //     evaluatedArguments.Push(std::move(EvaluateExpression(arg, context)));

        // if (!this->IsPlugin())
        // return FunctionDictionary.Get(this->functionType)(context, evaluatedArguments);

        // External::registry.Get(this->name.ToView())(context, evaluatedArguments);
        // else {
        //     const auto& plugin = PluginDictionary.Get(this->functionType);
        //     return plugin->Evaluate(context, evaluatedArguments);
        // }

    }

    Value FunctionExpression::Concat(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        // Value value( DataTypes::StringValue::Empty(), context._allocator, 0);
        //
        // // for (const auto& argument : arguments)
        // //     value += Value(argument.AsString(), context.allocator, 0);
        //
        // return value;
    }

    Value FunctionExpression::Length(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        // const auto& field = arguments[0];
        // return Value(DataTypes::String::Length(field.AsStringView()));
    }

    Value FunctionExpression::TrimLeft(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        // const auto& field = arguments[0];
        // return Value(DataTypes::String::TrimLeft(field.AsStringView()), context._allocator, 0);
    }

    Value FunctionExpression::TrimRight(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        // const auto& field = arguments[0];
        // return Value(DataTypes::String::TrimRight(field.AsStringView()), context._allocator, 0);
    }

    Value FunctionExpression::Trim(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        // const auto& field = arguments[0];
        // return Value(DataTypes::String::Trim(field.AsStringView()), context._allocator, 0);
    }

    Value FunctionExpression::AsciiValue(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        // const auto& field = arguments[0];
        // return Value(DataTypes::String::Ascii(field.AsStringView()));
    }

    Value FunctionExpression::Char(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        // const auto& field = arguments[0];
        // return Value(DataTypes::String::Char(field.AsInt(), context._allocator), context._allocator, 0);
    }

    Value FunctionExpression::CharIndex(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        // const auto& subStr = arguments[0].AsStringView();
        //
        // const auto& str = arguments[1].AsStringView();
        //
        // const int pos = (arguments.Size() > 2)
        //                     ? arguments[2].AsInt()
        //                     : 0;
        //
        // return Value(DataTypes::String::CharIndex(subStr, str, pos));
    }

    Value FunctionExpression::Lower(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        // const auto& field = arguments[0];
        // return Value(DataTypes::String::Lower(field.AsStringView(), context._allocator), context._allocator, 0);
    }

    Value FunctionExpression::Upper(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        // const auto& field = arguments[0];
        // return Value(DataTypes::String::Upper(field.AsStringView(), context._allocator), context._allocator, 0);
    }

    Value FunctionExpression::Replace(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        // const auto& str = arguments[0].AsStringView();
        // const auto& subStr = arguments[1].AsStringView();
        // const auto& replaceStr = arguments[2].AsStringView();
        //
        // return Value(DataTypes::String::Replace(str, subStr, replaceStr, context._allocator), context._allocator, 0);
    }

    Value FunctionExpression::Substr(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        // const auto& field = arguments[0].AsString(context._allocator);
        // const auto& startPos = arguments[1].AsInt();
        // const auto& endPos = arguments[2].AsInt();
        //
        // return Value(DataTypes::String::SubString(field, startPos, endPos), context._allocator, 0);
    }

    Value FunctionExpression::Left(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        // const auto& field = arguments[0].AsString(context._allocator);
        // const auto& startPos = arguments[1].AsInt();
        //
        // return Value(DataTypes::String::Left(field, startPos), context._allocator, 0);
    }

    Value FunctionExpression::Right(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        // const auto& field = arguments[0].AsString(context._allocator);
        // const auto& startPos = arguments[1].AsInt();
        //
        // return Value(DataTypes::String::Right(field, startPos), context._allocator, 0);
    }

    Value FunctionExpression::Reverse(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        // const auto str = arguments[0].AsString(context._allocator);
        // return Value(DataTypes::String::Reverse(str, str.GetAllocator()), context._allocator, 0);
    }

    Value FunctionExpression::Space(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        // const auto& size = arguments[0].AsInt();
        //
        // return Value(DataTypes::String::Space(size, context._allocator), context._allocator, 0);
    }

    Value FunctionExpression::GetDate(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        // return Value(DataTypes::DateTime::Now());
    }

    Value FunctionExpression::NewGuid(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        // return Value(DataTypes::Guid::NewGuid());
    }

    Value FunctionExpression::NullIf(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments) {
        // auto& firstArg = arguments[0];
        // const auto& secondArg = arguments[1];
        //
        // return firstArg == secondArg
        //            ? Value::Null()
        //            : firstArg;
    }

    bool FunctionExpression::ValidateNullIf(
        const DataStructures::PolymorphicArray<Expression*>& arguments,
        DataTypes::String& errorMessage
    ) {
        const auto firstArgumentType = GetExpressionReturnType(arguments[0]);
        const auto secondArgumentType = GetExpressionReturnType(arguments[1]);

        const auto promotedType = PromoteType(
            firstArgumentType,
            secondArgumentType
        );

        if (!DataTypes::Coercions::IsCoercionAllowed(firstArgumentType, promotedType)) {
            FunctionExpression::ConstructInvalidCastMessage(errorMessage, firstArgumentType, promotedType);
            return false;
        }

        if (!DataTypes::Coercions::IsCoercionAllowed(secondArgumentType, promotedType)) {
            FunctionExpression::ConstructInvalidCastMessage(errorMessage, secondArgumentType, promotedType);
            return false;
        }

        return true;
    }

    Value FunctionExpression::Coalesce(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments) {
        for (auto& argument : arguments) {
            if (!argument.IsNull())
                return argument;
        }

        return arguments[0];
    }

    bool FunctionExpression::ValidateCoalesce(
        const DataStructures::PolymorphicArray<Expression*>& arguments,
        DataTypes::String& errorMessage
    ){
        auto promotedType = DataType::String;

        DataStructures::PolymorphicArray<DataType> argTypes(arguments.GetAllocator(), arguments.Size());
        for (const auto& argument : arguments) {
            const auto argType = GetExpressionReturnType(argument);
            argTypes.Push(argType);
            promotedType = PromoteType(promotedType, argType);
        }

        for (const auto type : argTypes) {
            if (!DataTypes::Coercions::IsCoercionAllowed(type, promotedType)){
                FunctionExpression::ConstructInvalidCastMessage(errorMessage, type, promotedType);
                return false;
            }
        }

        return true;
    }

    bool FunctionExpression::ValidateNumberOfArguments(DataTypes::String& errorMessage)const {
        const auto& info = FunctionInfoDictionary.Get(this->functionType);
        const auto argSize = this->arguments.Size();
        if (argSize < info.minArgs || (argSize > info.maxArgs && info.maxArgs != UNLIMITED_ARGS)) {
            errorMessage = errorMessage.ConcatInPlace(
                "Function: ",
                info.name,
                " expects number of arguments from: ",
                std::to_string(info.minArgs),
                "to "
                ,(info.maxArgs == UNLIMITED_ARGS ? "unlimited" : std::to_string(info.maxArgs))
                , " but " , std::to_string(argSize) , " were given"
            );
            return false;
        }

        const auto argResult =
            (info.maxArgs == UNLIMITED_ARGS)
                ? this->ValidateUnlimitedArgumentTypes(info, errorMessage)
                : this->ValidateArgumentTypes(info, errorMessage);

        if (!argResult)
            return false;

        if (info.additionalValidations)
            return PerformAdditionalValidations(errorMessage);

        return true;
    }

    DataType FunctionExpression::GetReturnType() const{
        return FunctionInfoDictionary.Get(this->functionType).returnType;
    }

    bool FunctionExpression::IsPlugin() const{
        return this->functionType == Constants::FunctionType::Plugin;
    }

    BranchExpression::BranchExpression(const BranchType type, const ::Memory::IAllocator* allocator)
        :   branchType(type), branches(allocator),
            results(allocator), arguments(allocator),
            baseCase(nullptr) {
        this->expressionType = BranchExpression::TYPE;
    }

    DataType BranchExpression::GetReturnType() const {
        auto returnType = DataType::String;
        for (const auto& result : this->results)
            returnType = PromoteType(GetExpressionReturnType(result), returnType);

        if (this->HasBaseCase())
            returnType = PromoteType(GetExpressionReturnType(this->baseCase), returnType);

        return returnType;
    }

    bool BranchExpression::HasBaseCase() const {
        return this->branchType == BranchType::Switch;
    }

    bool BranchExpression::ValidateNumberOfArguments() const {
        switch (this->branchType) {
        case BranchType::Switch:
            return !this->branches.Empty() && this->branches.Size() == this->results.Size() && this->baseCase != nullptr;
        case BranchType::Ternary:
            return this->branches.Size() == 1 && this->results.Size() == 2 && this->baseCase == nullptr;
        default:
            throw std::runtime_error("BranchExpression::ValidateNumberOfArguments: Unknown expression branching type");
        }
    }

    VariableExpression::VariableExpression(const DataTypes::String& name, const ::Memory::IAllocator* allocator) {
        this->name = name;
        this->normalizedName = this->name.ToLower();
        this->dataType = DataType::Null;
        this->expressionType = VariableExpression::TYPE;
    }

    void VariableExpression::BindRowKernel(Expression* self){
        auto* variableExpression = self->As<VariableExpression>();
        switch (variableExpression->dataType){
        case DataType::String:
            variableExpression->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<DataTypes::StringValue>;
            break;
        case DataType::Bool:
            variableExpression->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<bool>;
            break;
        case DataType::TinyInt:
            variableExpression->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<TinyInt>;
            break;
        case DataType::SmallInt:
            variableExpression->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<SmallInt>;
            break;
        case DataType::Int:
            variableExpression->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<Int>;
            break;
        case DataType::BigInt:
            variableExpression->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<BigInt>;
            break;
        case DataType::Decimal:
            variableExpression->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<DataTypes::Decimal>;
            break;
        case DataType::DateTime:
            variableExpression->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<BigInt>;
            break;
        case DataType::Guid:
            variableExpression->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<DataTypes::Guid>;
            break;
        case DataType::Json:
            variableExpression->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<DataTypes::JsonBinary>;
            break;
        default:
            break;
        }
    }

    void VariableExpression::BindVectorizedKernel(Expression* self, const CoreEngine::OutputSchema* schema){
        auto* variableExpression = self->As<VariableExpression>();
        variableExpression->vectorizedKernel = CoreEngine::VectorizedKernels::JumpTables::GetVariableKernel(variableExpression->dataType);
    }

    Value VariableExpression::Evaluate(const EvaluationContext &context) const {
        return context._executionContext->GetVariable(DataTypes::StringView::ViewOf(this->normalizedName))->GetValue();
    }

    DataType VariableExpression::GetReturnType() const { return this->dataType; }

    Value JsonExpression::EvaluateJsonPath(const EvaluationContext &context, const Value& columnValue) const{
        const auto jsonBinary = columnValue.AsJson(context._allocator);
        auto jsonValue = jsonBinary.Navigate(this->pathSegments);

        // const auto jsonType = jsonValue.Type();
        // if (jsonType == Serialization::JsonType::Array
        //     || jsonType == Serialization::JsonType::Object
        // ){
        //     const auto str = DataTypes::JsonBinary::JsonObjectToString(jsonValue, context._allocator);
        //     return Value(str, context._allocator);
        // }
        //
        // return Value(jsonValue, context._allocator);
    }

    JsonExpression::JsonExpression(ColumnExpression* columnPtr, const Memory::IAllocator* allocator)
        :columnPtr(columnPtr), pathSegments(allocator), type(DataType::Null){
        this->expressionType = JsonExpression::TYPE;
    }

    DataType JsonExpression::GetReturnType() const{
        return this->type;
    }

    CastExpression::CastExpression(
        Expression* expression,
        const DataType targetType,
        const bool isTryCast
    ): childExpr(expression), targetType(targetType), isTryCast(isTryCast) {
        this->expressionType = CastExpression::TYPE;
    }

    void CastExpression::BindRowKernel(Expression* self){
        auto* castExpression = self->As<CastExpression>();
        Expressions::BindExpressionRowKernel(castExpression->childExpr);
        const auto childExprType = GetExpressionReturnType(castExpression->childExpr);
        castExpression->rowKernel = CoreEngine::RowKernels::LookupCastKernel(childExprType, castExpression->targetType);
    }

    void CastExpression::BindVectorizedKernel(Expression* self, const CoreEngine::OutputSchema* schema){
        auto* castExpression = self->As<CastExpression>();
        Expressions::BindAndResolveExpressionKernel(castExpression->childExpr, schema);
        const auto childExprType = GetExpressionReturnType(castExpression->childExpr);
        castExpression->vectorizedKernel = CoreEngine::VectorizedKernels::JumpTables::GetCastKernel(childExprType, castExpression->targetType);
    }

    DataType CastExpression::GetReturnType() const{ return this->targetType; }

    CoreEngine::DataVector* EvaluateExpression(
        const Expression* expression,
        const CoreEngine::ExecutionContext* context,
        const CoreEngine::DataChunk* chunk
    ){
        return expression->vectorizedKernel(expression, context, chunk);
    }

    void EvaluateExpression(
        const Expression* expression,
        const EvaluationContext& context,
        void* outVal,
        bool* outNull
    ){
        expression->rowKernel(expression, context, outVal, outNull);
    }

    Value EvaluateExpression(const Expression* expression, const EvaluationContext& context){
        switch (GetExpressionReturnType(expression)) {
        case DataType::String:
            return CoreEngine::RowKernels::KernelToValue<DataTypes::StringValue>(expression, context);
        case DataType::Bool:
            return CoreEngine::RowKernels::KernelToValue<bool>(expression, context);
        case DataType::TinyInt:
            return CoreEngine::RowKernels::KernelToValue<TinyInt>(expression, context);
        case DataType::SmallInt:
            return CoreEngine::RowKernels::KernelToValue<SmallInt>(expression, context);
        case DataType::Int:
            return CoreEngine::RowKernels::KernelToValue<Int>(expression, context);
        case DataType::BigInt:
            return CoreEngine::RowKernels::KernelToValue<BigInt>(expression, context);
        case DataType::Decimal:
            return CoreEngine::RowKernels::KernelToValue<DataTypes::Decimal>(expression, context);
        case DataType::DateTime:
            return CoreEngine::RowKernels::KernelToValue<BigInt>(expression, context);
        case DataType::Guid:
            return CoreEngine::RowKernels::KernelToValue<DataTypes::Guid>(expression, context);
        case DataType::Json:
            return CoreEngine::RowKernels::KernelToValue<DataTypes::JsonBinary>(expression, context);
        default:
            return Value::Null();
        }
    }

    bool RowModeFilter(
        const Expression* expression,
        const EvaluationContext& context
    ){
        auto keep = false, isNull = false;
        expression->rowKernel(expression, context, &keep, &isNull);
        return !isNull && keep;
    }

    DataType GetExpressionReturnType(const Expression* expression){
        return VisitNode(expression->expressionType, [&]<typename TNode>() -> DataType{
            return expression->As<TNode>()->GetReturnType();
        });
    }

    void BindExpressionRowKernel(Expression* expression){
        if (expression == nullptr)
            return;

        switch (expression->expressionType){
        case ExpressionType::Column:
            ColumnExpression::BindRowKernel(expression);
            break;
        case ExpressionType::Constant:
            ConstantExpression::BindRowKernel(expression);
            break;
        case ExpressionType::Binary:
            BinaryExpression::BindRowKernel(expression);
            break;
        case ExpressionType::Logical:
            LogicalExpression::BindRowKernel(expression);
            break;
        case ExpressionType::Variable:
            VariableExpression::BindRowKernel(expression);
            break;
        case ExpressionType::Branch:
            break;
        case ExpressionType::Function:
            break;
        case ExpressionType::Json:
            break;
        case ExpressionType::Cast:
            CastExpression::BindRowKernel(expression);
            break;
        default:
            break;
        }
    }

    void BindAndResolveExpressionKernel(Expression* expression, const CoreEngine::OutputSchema* schema){
        if (expression == nullptr)
            return;

        VisitNode(expression->expressionType, [&]<typename TNode>(){
            if constexpr (VectorizedNode<TNode>)
                TNode::BindVectorizedKernel(expression, schema);
            else
                throw std::runtime_error(std::format(
                        "Expression '{}' is not supported in vectorized execution",
                        Reflection::EnumIdentifier(TNode::TYPE))
                );
        });
    }
}
