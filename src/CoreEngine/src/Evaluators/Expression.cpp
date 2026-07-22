#include "../../include/Evaluators/Expression.h"

#include "../../../Systemic/include/Coercions/Coercions.h"
#include "../../../Systemic/include/DataTypes/Value.h"
#include "../../../Systemic/include/QueryResult.h"
#include "../../../Systemic/include/Functions/StringFunctions.h"
#include "../../include/DataStorage/Row.h"
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

    // EvaluationContext::EvaluationContext(
    //     const QueryResult &row,
    //     const CoreEngine::ExecutionContext* executionContext
    // ) {
    //     this->type = EvaluationContextType::MaterializedRow;
    //     this->allocator = executionContext.GetAllocator();
    //     this->table = executionContext.GetTable(0);
    //     this->variables = executionContext.GetVariables();
    //     this->materializedRow = row;
    //     this->row = nullptr;
    //     this->joinRow = nullptr;
    // }
    //
    // EvaluationContext::EvaluationContext(
    //     const CoreEngine::StorageTypes::RID* row,
    //     const CoreEngine::StorageTypes::RID* joinRow,
    //     const CoreEngine::ExecutionContext& executionContext
    // ) :     row(row), joinRow(joinRow),
    //         allocator(executionContext.GetAllocator()),
    //         table(executionContext.GetTable(0)),
    //         variables(executionContext.GetVariables()),
    //         type(EvaluationContextType::Join){}
    //
    // EvaluationContext EvaluationContext::CreateJoinContext(
    //     const CoreEngine::StorageTypes::RID* outerRow,
    //     const CoreEngine::StorageTypes::RID* innerRow,
    //     const CoreEngine::ExecutionContext& executionContext
    // ){
    //     return EvaluationContext(outerRow, innerRow, executionContext);
    // }

    Value Expression::EvaluateJoin(const EvaluationContext& context) const{
        // auto previousColumns = context.row->numberOfColumns;
        // if (this->columnIndex < previousColumns)
        //     return context.row->PartialMaterialize(context.allocator, this->columnIndex);
        //
        // for (const auto& joinedRow : context.row->logicalState->joinedRows) {
        //     const auto joinRowColumns = joinedRow->numberOfColumns;
        //
        //     if (this->columnIndex < previousColumns + joinRowColumns)
        //         return joinedRow->PartialMaterialize(context.allocator, this->columnIndex - previousColumns);
        //
        //     previousColumns += joinRowColumns;
        // }
        //
        // if (this->columnIndex < previousColumns + context.joinRow->numberOfColumns)
        //     return context.joinRow->PartialMaterialize(context.allocator, this->columnIndex - previousColumns);
        //
        // return Value::Null();
    }

    Expression::Expression()
        : ordinalPosition(0), expressionType(ExpressionType::Expression) {}

    bool Expression::IsBinary() const{ return this->expressionType == ExpressionType::Binary; }
    bool Expression::IsLogical() const{ return this->expressionType == ExpressionType::Logical; }
    bool Expression::IsConstant() const{ return this->expressionType == ExpressionType::Constant; }
    bool Expression::IsVariable() const{ return this->expressionType == ExpressionType::Variable; }
    bool Expression::IsColumn() const{ return this->expressionType == ExpressionType::Column; }
    bool Expression::IsFunction() const{ return this->expressionType == ExpressionType::Function; }
    bool Expression::IsBranch() const{ return this->expressionType == ExpressionType::Branch; }
    bool Expression::IsJson() const{ return this->expressionType == ExpressionType::Json; }

    BinaryExpression * Expression::AsBinary(){ return static_cast<BinaryExpression*>(this); }
    LogicalExpression * Expression::AsLogical(){ return static_cast<LogicalExpression*>(this); }
    ColumnExpression * Expression::AsColumn(){ return static_cast<ColumnExpression*>(this); }
    VariableExpression * Expression::AsVariable(){ return static_cast<VariableExpression*>(this); }
    ConstantExpression * Expression::AsConstant(){ return static_cast<ConstantExpression*>(this); }
    BranchExpression * Expression::AsBranch(){ return static_cast<BranchExpression*>(this); }
    FunctionExpression * Expression::AsFunction(){ return static_cast<FunctionExpression*>(this); }
    JsonExpression* Expression::AsJson(){ return static_cast<JsonExpression*>(this); }
    CastExpression* Expression::AsCast(){return static_cast<CastExpression*>(this);}

    const BinaryExpression * Expression::AsBinary() const{ return static_cast<const BinaryExpression*>(this); }
    const LogicalExpression * Expression::AsLogical() const{ return static_cast<const LogicalExpression*>(this); }
    const ColumnExpression * Expression::AsColumn() const{ return static_cast<const ColumnExpression*>(this); }
    const VariableExpression * Expression::AsVariable() const{ return static_cast<const VariableExpression*>(this); }
    const ConstantExpression * Expression::AsConstant() const{ return static_cast<const ConstantExpression*>(this); }
    const BranchExpression * Expression::AsBranch() const{ return static_cast<const BranchExpression*>(this); }
    const FunctionExpression * Expression::AsFunction() const{ return static_cast<const FunctionExpression*>(this); }
    const JsonExpression* Expression::AsJson() const{ return static_cast<const JsonExpression*>(this); }
    const CastExpression* Expression::AsCast() const{ return static_cast<const CastExpression*>(this); }

    bool Expression::IsColumnType() const{
        return this->expressionType == ExpressionType::Column
            || this->expressionType == ExpressionType::Json;
    }

    void Expression::SetIndex(const column_index_t index){ this->ordinalPosition = index; }

    // Value ColumnExpression::EvaluateSingleRow(const EvaluationContext& context) const{
    //     return context.table->MaterializeColumn(context.allocator, context.row, this->columnIndex);
    // }

    void ColumnExpression::BindVectorizedKernel(){
        this->vectorizedKernel = &CoreEngine::VectorizedKernels::ColumnScanKernel;
    }

    void ColumnExpression::BindRowKernel(){
        switch (this->returnType){
        case DataType::String:
            this->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<DataTypes::String>;
            break;
        case DataType::Bool:
            this->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<bool>;
            break;
        case DataType::TinyInt:
            this->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<TinyInt>;
            break;
        case DataType::SmallInt:
            this->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<SmallInt>;
            break;
        case DataType::Int:
            this->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<Int>;
            break;
        case DataType::BigInt:
            this->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<BigInt>;
            break;
        case DataType::Decimal:
            this->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<DataTypes::Decimal>;
            break;
        case DataType::DateTime:
            this->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<DataTypes::DateTime>;
            break;
        case DataType::Guid:
            this->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<DataTypes::Guid>;
            break;
        case DataType::Json:
            this->rowKernel = &CoreEngine::RowKernels::ColumnScanKernel<DataTypes::JsonBinary>;
            break;
        case DataType::Null:
        case DataType::RowIdentifier:
            break;
        }
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
        this->expressionType = ExpressionType::Column;
    }

    ColumnExpression::ColumnExpression(DataTypes::String&& name, DataTypes::String&& tableAlias)
        :   alias(std::move(name)), tableAlias(std::move(tableAlias)),
            tableId(INVALID_TABLE_ID), columnId(INVALID_COLUMN_ID),
            returnType(DataType::Null), size(0) {
        this->_slotIndex = DEFAULT_SLOT_INDEX;
        this->ordinalPosition = 0;
        this->expressionType = ExpressionType::Column;
    }

    ColumnExpression::ColumnExpression(const column_index_t index, const DataType dataType){
        this->_slotIndex = DEFAULT_SLOT_INDEX;
        this->ordinalPosition = index;
        this->size = 0;
        this->returnType = dataType;
        this->tableId = INVALID_TABLE_ID;
        this->columnId = INVALID_COLUMN_ID;
        this->expressionType = ExpressionType::Column;
    }

    void ColumnExpression::BindExpressionKernel(
        ColumnExpression* expression,
        const Constants::ExecutionMode mode
    ){
        switch (mode){
        case Constants::ExecutionMode::Row:
            expression->BindRowKernel();
            break;
        case Constants::ExecutionMode::Vectorized:
            expression->BindVectorizedKernel();
            break;
        }
    }

    void ColumnExpression::ResolveReference(ColumnExpression* expression, const CoreEngine::OutputSchema* schema){
        const auto identity = CoreEngine::ColumnIdentity::Base(expression->_slotIndex, expression->ordinalPosition);
        expression->_boundReference._childIndex = 0;
        expression->_boundReference._position = schema->IndexOf(identity);
    }

    DataType ColumnExpression::GetReturnType() const{ return this->returnType; }

    bool ColumnExpression::HasTableAlias() const { return !this->tableAlias.Empty();}

    void ConstantExpression::BindVectorizedKernel(){
    }

    void ConstantExpression::BindRowKernel(){
        switch (this->value.GetType()){
        case DataType::Bool:
            this->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<bool>;
            break;
        case DataType::TinyInt:
            this->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<TinyInt>;
            break;
        case DataType::SmallInt:
            this->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<SmallInt>;
            break;
        case DataType::Int:
            this->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<Int>;
            break;
        case DataType::BigInt:
            this->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<BigInt>;
            break;
        case DataType::DateTime:
            this->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<DataTypes::DateTime>;
            break;
        case DataType::Guid:
            this->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<DataTypes::Guid>;
            break;
        case DataType::Decimal:
            this->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<DataTypes::Decimal>;
            break;
        case DataType::String:
            this->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<DataTypes::String>;
            break;
        case DataType::Json:
            this->rowKernel = &CoreEngine::RowKernels::ConstantScanKernel<DataTypes::JsonBinary>;
            break;
        case DataType::Null:
            this->rowKernel = &CoreEngine::RowKernels::ConstantScanNullKernel;
            break;
        default:
            break;
        }
    }

    ConstantExpression::ConstantExpression(const Value &value)
        : value(value){
        this->expressionType = ExpressionType::Constant;
    }

    ConstantExpression::ConstantExpression(Value &value)
        : value(std::move(value)){
        this->expressionType = ExpressionType::Constant;
    }

    ConstantExpression::ConstantExpression(Value&& value)
        : value(std::move(value)){
        this->expressionType = ExpressionType::Constant;
    }

    void ConstantExpression::BindExpressionKernel(ConstantExpression* expression, const Constants::ExecutionMode mode){
        switch (mode){
        case Constants::ExecutionMode::Row:
            expression->BindRowKernel();
            break;
        case Constants::ExecutionMode::Vectorized:
            expression->BindVectorizedKernel();
            break;
        }
    }

    DataType ConstantExpression::GetReturnType() const{ return this->value.GetType(); }

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
        case DataType::RowIdentifier:
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
        case DataType::RowIdentifier:
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
        case DataType::RowIdentifier:
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
        case DataType::RowIdentifier:
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
        case DataType::RowIdentifier:
        case DataType::Null:
        default:
            return false;
        }
    }

    void BinaryExpression::BindVectorizedKernel(){
    }

    void BinaryExpression::BindRowKernel(){
        const auto operandType = GetExpressionReturnType(this->left);
        this->rowKernel = CoreEngine::RowKernels::LookupBinaryKernel(
            this->operation, operandType
        );
    }

    BinaryExpression::BinaryExpression(Expression *left, Expression *right, const BinaryOperator operation){
        this->left = left;
        this->right = right;
        this->operation = operation;
        this->expressionType = ExpressionType::Binary;
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

    void BinaryExpression::BindExpressionKernel(
        BinaryExpression* expression,
        const Constants::ExecutionMode mode
    ){
        Expressions::BindExpressionKernel(expression->left, mode);
        Expressions::BindExpressionKernel(expression->right, mode);

        switch (mode){
        case Constants::ExecutionMode::Row:
            expression->BindRowKernel();
            break;
        case Constants::ExecutionMode::Vectorized:
            expression->BindVectorizedKernel();
            break;
        }
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
        this->expressionType = ExpressionType::Function;
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
        Value value(DataTypes::String::Null(), context._allocator, 0);

        // for (const auto& argument : arguments)
        //     value += Value(argument.AsString(), context.allocator, 0);

        return value;
    }

    Value FunctionExpression::Length(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0];
        return Value(DataTypes::String::Length(field.AsStringView()), context._allocator, 0);
    }

    Value FunctionExpression::TrimLeft(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0];
        return Value(DataTypes::String::TrimLeft(field.AsStringView()), context._allocator, 0);
    }

    Value FunctionExpression::TrimRight(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0];
        return Value(DataTypes::String::TrimRight(field.AsStringView()), context._allocator, 0);
    }

    Value FunctionExpression::Trim(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0];
        return Value(DataTypes::String::Trim(field.AsStringView()), context._allocator, 0);
    }

    Value FunctionExpression::AsciiValue(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0];
        return Value(DataTypes::String::Ascii(field.AsStringView()), context._allocator, 0);
    }

    Value FunctionExpression::Char(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0];
        return Value(DataTypes::String::Char(field.AsInt(), context._allocator), context._allocator, 0);
    }

    Value FunctionExpression::CharIndex(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& subStr = arguments[0].AsStringView();

        const auto& str = arguments[1].AsStringView();

        const int pos = (arguments.Size() > 2)
                            ? arguments[2].AsInt()
                            : 0;

        return Value(DataTypes::String::CharIndex(subStr, str, pos), context._allocator, 0);
    }

    Value FunctionExpression::Lower(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0];
        return Value(DataTypes::String::Lower(field.AsStringView(), context._allocator), context._allocator, 0);
    }

    Value FunctionExpression::Upper(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0];
        return Value(DataTypes::String::Upper(field.AsStringView(), context._allocator), context._allocator, 0);
    }

    Value FunctionExpression::Replace(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& str = arguments[0].AsStringView();
        const auto& subStr = arguments[1].AsStringView();
        const auto& replaceStr = arguments[2].AsStringView();

        return Value(DataTypes::String::Replace(str, subStr, replaceStr, context._allocator), context._allocator, 0);
    }

    Value FunctionExpression::Substr(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0].AsString();
        const auto& startPos = arguments[1].AsInt();
        const auto& endPos = arguments[2].AsInt();

        return Value(DataTypes::String::SubString(field, startPos, endPos), context._allocator, 0);
    }

    Value FunctionExpression::Left(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0].AsString();
        const auto& startPos = arguments[1].AsInt();

        return Value(DataTypes::String::Left(field, startPos), context._allocator, 0);
    }

    Value FunctionExpression::Right(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0].AsString();
        const auto& startPos = arguments[1].AsInt();

        return Value(DataTypes::String::Right(field, startPos), context._allocator, 0);
    }

    Value FunctionExpression::Reverse(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& str = arguments[0].AsString();
        return Value(DataTypes::String::Reverse(str), context._allocator, 0);
    }

    Value FunctionExpression::Space(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& size = arguments[0].AsInt();

        return Value(DataTypes::String::Space(size, context._allocator), context._allocator, 0);
    }

    Value FunctionExpression::GetDate(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        return Value(DataTypes::DateTime::Now(), context._allocator, 0);
    }

    Value FunctionExpression::NewGuid(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        return Value(DataTypes::Guid::NewGuid(), context._allocator, 0);
    }

    Value FunctionExpression::NullIf(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments) {
        auto& firstArg = arguments[0];
        const auto& secondArg = arguments[1];

        return firstArg == secondArg
                   ? Value::Null(nullptr)
                   : firstArg;
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

    void LogicalExpression::BindRowKernel(){
        switch (logicalType) {
        case LogicalType::And:
            this->rowKernel = &CoreEngine::RowKernels::LogicalAndKernel;
            break;
        case LogicalType::Or:
            this->rowKernel = &CoreEngine::RowKernels::LogicalOrKernel;
            break;
        case LogicalType::Invalid:
            break;
        }

    }

    void LogicalExpression::BindVectorizedKernel(){
    }

    LogicalExpression::LogicalExpression(
        Expression *leftExpression,
        Expression *RightExpression,
        const LogicalType logicalType
    ){
        this->logicalType = logicalType;
        this->left = leftExpression;
        this->right = RightExpression;
        this->expressionType = ExpressionType::Logical;
    }

    LogicalExpression::LogicalExpression(){
        this->logicalType = LogicalType::Invalid;
        this->left = nullptr;
        this->right = nullptr;
        this->expressionType = ExpressionType::Logical;
    }

    bool LogicalExpression::IsOr() const{ return this->logicalType == LogicalType::Or; }

    bool LogicalExpression::IsAnd() const{ return this->logicalType == LogicalType::And; }

    bool LogicalExpression::HasAtLeastOneConstant() const{ return this->left->IsConstant() || this->right->IsConstant(); }

    void LogicalExpression::BindExpressionKernel(
        LogicalExpression* expression,
        const Constants::ExecutionMode mode
    ){
        Expressions::BindExpressionKernel(expression->left, mode);
        Expressions::BindExpressionKernel(expression->right, mode);

        switch (mode){
        case Constants::ExecutionMode::Row:
            expression->BindRowKernel();
            break;
        case Constants::ExecutionMode::Vectorized:
            expression->BindVectorizedKernel();
            break;
        }
    }

    constexpr DataType LogicalExpression::GetReturnType() { return DataType::Bool; }

    BranchExpression::BranchExpression(const BranchType type, const ::Memory::IAllocator* allocator)
        :   branchType(type), branches(allocator),
            results(allocator), arguments(allocator),
            baseCase(nullptr) {
        this->expressionType = ExpressionType::Branch;
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

    void VariableExpression::BindVectorizedKernel(){
    }

    void VariableExpression::BindRowKernel(){
        switch (this->dataType){
        case DataType::String:
            this->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<DataTypes::String>;
            break;
        case DataType::Bool:
            this->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<bool>;
            break;
        case DataType::TinyInt:
            this->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<TinyInt>;
            break;
        case DataType::SmallInt:
            this->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<SmallInt>;
            break;
        case DataType::Int:
            this->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<Int>;
            break;
        case DataType::BigInt:
            this->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<BigInt>;
            break;
        case DataType::Decimal:
            this->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<DataTypes::Decimal>;
            break;
        case DataType::DateTime:
            this->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<BigInt>;
            break;
        case DataType::Guid:
            this->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<DataTypes::Guid>;
            break;
        case DataType::Json:
            this->rowKernel = &CoreEngine::RowKernels::VariableScanKernel<DataTypes::JsonBinary>;
            break;
        default:
            break;
        }
    }

    VariableExpression::VariableExpression(const DataTypes::String& name, const ::Memory::IAllocator* allocator) {
        this->name = name;
        this->normalizedName = this->name.ToLower();
        this->dataType = DataType::Null;
        this->expressionType = ExpressionType::Variable;
    }

    void VariableExpression::BindExpressionKernel(
        VariableExpression* expression,
        const Constants::ExecutionMode mode
    ){
        switch (mode){
        case Constants::ExecutionMode::Row:
            expression->BindRowKernel();
            break;
        case Constants::ExecutionMode::Vectorized:
            expression->BindVectorizedKernel();
            break;
        }
    }

    Value VariableExpression::Evaluate(const EvaluationContext &context) const {
        return context._executionContext->GetVariables()->Get(this->normalizedName).GetValue();
    }

    DataType VariableExpression::GetReturnType() const { return this->dataType; }

    Value JsonExpression::EvaluateJsonPath(const EvaluationContext &context, const Value& columnValue) const{
        const auto jsonBinary = columnValue.AsJson();
        auto jsonValue = jsonBinary.Navigate(this->pathSegments);

        const auto jsonType = jsonValue.Type();
        if (jsonType == Serialization::JsonType::Array
            || jsonType == Serialization::JsonType::Object
        ){
            const auto str = DataTypes::JsonBinary::JsonObjectToString(jsonValue, context._allocator);
            return Value(str, context._allocator);
        }

        return Value(jsonValue, context._allocator);
    }

    JsonExpression::JsonExpression(ColumnExpression* columnPtr, const Memory::IAllocator* allocator)
        :columnPtr(columnPtr), pathSegments(allocator), type(DataType::Null){
        this->expressionType = ExpressionType::Json;
    }

    DataType JsonExpression::GetReturnType() const{
        return this->type;
    }

    void CastExpression::BindVectorizedKernel(){
    }

    void CastExpression::BindRowKernel(){
        const auto childExprType = GetExpressionReturnType(this->childExpr);
        this->rowKernel = CoreEngine::RowKernels::LookupCastKernel(childExprType, this->targetType);
    }

    CastExpression::CastExpression(Expression* expression, const DataType targetType, const bool isTryCast)
        : childExpr(expression), targetType(targetType), isTryCast(isTryCast) {
        this->expressionType = ExpressionType::Cast;
    }

    void CastExpression::BindExpressionKernel(
        CastExpression* expression,
        const Constants::ExecutionMode mode
    ){
        Expressions::BindExpressionKernel(expression->childExpr, mode);
        switch (mode){
        case Constants::ExecutionMode::Row:
            expression->BindRowKernel();
            break;
        case Constants::ExecutionMode::Vectorized:
            expression->BindVectorizedKernel();
            break;
        }
    }

    DataType CastExpression::GetReturnType() const{ return this->targetType; }

    CoreEngine::DataVector* EvaluateExpression(
        const Expression* expression,
        const CoreEngine::ExecutionContext& executionContext,
        const CoreEngine::DataChunk* chunk
    ){
        return expression->vectorizedKernel(expression, chunk);
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
            return CoreEngine::RowKernels::KernelToValue<DataTypes::String>(expression, context);
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
            return Value::Null(context._allocator);
        }
    }

    CoreEngine::SelectionVector* EvaluateFilterExpression(
        const Expression* expression,
        const CoreEngine::ExecutionContext& executionContext,
        const CoreEngine::SelectionVector* selectionVector
    ){
        auto* result = executionContext.Allocate<CoreEngine::SelectionVector>();
        result->AllocateRids(executionContext.GetAllocator(), 0, selectionVector->selectedRidsCount);

        switch (expression->expressionType){
        case ExpressionType::Binary:{
            for (int i = 0;i < selectionVector->selectedRidsCount; i++){
            }
        }
        case ExpressionType::Logical:{

        }
        default:
            break;
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
        switch (expression->expressionType){
        case ExpressionType::Column:
            return expression->AsColumn()->GetReturnType();
        case ExpressionType::Constant:
            return expression->AsConstant()->GetReturnType();
        case ExpressionType::Binary:
            return expression->AsBinary()->GetReturnType();
        case ExpressionType::Logical:
            return LogicalExpression::GetReturnType();
        case ExpressionType::Variable:
            return expression->AsVariable()->GetReturnType();
        case ExpressionType::Branch:
            return expression->AsBranch()->GetReturnType();
        case ExpressionType::Function:
            return expression->AsFunction()->GetReturnType();
        case ExpressionType::Json:
            return expression->AsJson()->GetReturnType();
        case ExpressionType::Cast:
            return expression->AsCast()->GetReturnType();
        default:
            return DataType::Null;
        }
    }

    void BindExpressionKernel(Expression* expression, const Constants::ExecutionMode mode){
        if (expression == nullptr)
            return;

        switch (expression->expressionType){
        case ExpressionType::Column:
            ColumnExpression::BindExpressionKernel(expression->AsColumn(), mode);
            break;
        case ExpressionType::Constant:
            ConstantExpression::BindExpressionKernel(expression->AsConstant(), mode);
            break;
        case ExpressionType::Binary:
            BinaryExpression::BindExpressionKernel(expression->AsBinary(), mode);
            break;
        case ExpressionType::Logical:
            LogicalExpression::BindExpressionKernel(expression->AsLogical(), mode);
            break;
        case ExpressionType::Variable:
            VariableExpression::BindExpressionKernel(expression->AsVariable(), mode);
            break;
        case ExpressionType::Branch:
            break;
        case ExpressionType::Function:
            break;
        case ExpressionType::Json:
            break;
        case ExpressionType::Cast:
            CastExpression::BindExpressionKernel(expression->AsCast(), mode);
            break;
        default:
            break;
        }
    }

    void BindAndResolveExpressionKernel(Expression* expression, const CoreEngine::OutputSchema* schema){
        if (expression == nullptr)
            return;

        static auto constexpr EXECUTION_MODE = Constants::ExecutionMode::Vectorized;

        switch (expression->expressionType){
        case ExpressionType::Column:{
            auto* columnExpr = expression->AsColumn();
            ColumnExpression::BindExpressionKernel(columnExpr, EXECUTION_MODE);
            ColumnExpression::ResolveReference(columnExpr, schema);
            break;
        }
        case ExpressionType::Constant:
            ConstantExpression::BindExpressionKernel(expression->AsConstant(), EXECUTION_MODE);
            break;
        case ExpressionType::Binary:
            BinaryExpression::BindExpressionKernel(expression->AsBinary(), EXECUTION_MODE);
            break;
        case ExpressionType::Logical:
            LogicalExpression::BindExpressionKernel(expression->AsLogical(), EXECUTION_MODE);
            break;
        case ExpressionType::Variable:
            VariableExpression::BindExpressionKernel(expression->AsVariable(), EXECUTION_MODE);
            break;
        case ExpressionType::Branch:
            break;
        case ExpressionType::Function:
            break;
        case ExpressionType::Json:
            break;
        case ExpressionType::Cast:
            CastExpression::BindExpressionKernel(expression->AsCast(), EXECUTION_MODE);
            break;
        default:
            break;
        }

    }
}
