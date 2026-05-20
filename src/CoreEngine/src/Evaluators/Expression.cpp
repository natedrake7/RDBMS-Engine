#include "../../include/Evaluators/Expression.h"

#include "../../../Systemic/include/Coercions.h"
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
#include "Pages/Additional/Frame.h"

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

    EvaluationContext::EvaluationContext(const Memory::IAllocator* allocator)
        :   row(nullptr), joinRow(nullptr),
            allocator(allocator), table(nullptr),
            variables(nullptr), type(EvaluationContextType::Constant) {}

    EvaluationContext::EvaluationContext(
        const EvaluationContextType type,
        const Memory::IAllocator* allocator,
        const CoreEngine::StorageTypes::Table* table
    ):  row(nullptr), joinRow(nullptr),
        allocator(allocator), table(table),
        variables(nullptr), type(type){}

    EvaluationContext::EvaluationContext(
        const EvaluationContextType type,
        const CoreEngine::ExecutionContext& executionContext
    ){
        this->type = type;
        this->table = executionContext.GetTable(0);
        this->allocator = executionContext.GetAllocator();
        this->variables = executionContext.GetVariables();
        this->row = nullptr;
        this->joinRow = nullptr;
    }

    EvaluationContext::EvaluationContext(
      const CoreEngine::StorageTypes::RID* row,
      const CoreEngine::ExecutionContext& executionContext
    ){
        this->type = EvaluationContextType::SingleRow;
        this->row = row;
        this->joinRow = nullptr;
        this->allocator = executionContext.GetAllocator();
        this->table = executionContext.GetTable(0);
        this->variables = executionContext.GetVariables();
    }

    EvaluationContext::EvaluationContext(
        const QueryResult &row,
        const CoreEngine::ExecutionContext& executionContext
    ) {
        this->type = EvaluationContextType::MaterializedRow;
        this->allocator = executionContext.GetAllocator();
        this->table = executionContext.GetTable(0);
        this->variables = executionContext.GetVariables();
        this->materializedRow = row;
        this->row = nullptr;
        this->joinRow = nullptr;
    }

    EvaluationContext::EvaluationContext(
        const CoreEngine::StorageTypes::RID* row,
        const CoreEngine::StorageTypes::RID* joinRow,
        const CoreEngine::ExecutionContext& executionContext
    ) :     row(row), joinRow(joinRow),
            allocator(executionContext.GetAllocator()),
            table(executionContext.GetTable(0)),
            variables(executionContext.GetVariables()),
            type(EvaluationContextType::Join){}

    EvaluationContext EvaluationContext::CreateJoinContext(
        const CoreEngine::StorageTypes::RID* outerRow,
        const CoreEngine::StorageTypes::RID* innerRow,
        const CoreEngine::ExecutionContext& executionContext
    ){
        return EvaluationContext(outerRow, innerRow, executionContext);
    }

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

    Expression::Expression(){
        this->expressionType = ExpressionType::Expression;
        this->columnIndex = 0;
    }

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

    void Expression::SetIndex(const column_index_t index){ this->columnIndex = index; }

    Value ColumnExpression::EvaluateSingleRow(const EvaluationContext& context) const{
        // auto previousColumns = context.row->numberOfColumns;
        // if (this->columnIndex < previousColumns)
        //     return context.row->PartialMaterialize(context.allocator, this->columnIndex);
        //
        // for (const auto& joinedRow : context.row->logicalState->joinedRows) {
        //     const auto joinRowColumns = joinedRow->numberOfColumns;
        //
        //     if (this->columnIndex < previousColumns + joinRowColumns)
        //         return joinedRow->PartialMaterialize(
        //             context.allocator,
        //             this->columnIndex - previousColumns
        //         );
        //
        //     previousColumns += joinRowColumns;
        // }
        //
        // return Value::Null(context.allocator);
    }

    ColumnExpression::ColumnExpression(const DataTypes::String& name, const DataTypes::String& tableAlias){
        this->alias = name;
        this->tableAlias = tableAlias;

        this->tableId = INVALID_TABLE_ID;
        this->columnId = INVALID_COLUMN_ID;
        this->columnIndex = 0;
        this->size = 0;
        this->returnType = DataType::Null;
        this->expressionType = ExpressionType::Column;
    }

    ColumnExpression::ColumnExpression(DataTypes::String&& name, DataTypes::String&& tableAlias)
        : alias(std::move(name)), tableAlias(std::move(tableAlias)){
        this->tableId = INVALID_TABLE_ID;
        this->columnId = INVALID_COLUMN_ID;
        this->columnIndex = 0;
        this->size = 0;
        this->returnType = DataType::Null;
        this->expressionType = ExpressionType::Column;
    }

    ColumnExpression::ColumnExpression(const column_index_t index){
        this->columnIndex = index;
        this->size = 0;
        this->returnType = DataType::Null;
        this->tableId = INVALID_TABLE_ID;
        this->columnId = INVALID_COLUMN_ID;
        this->expressionType = ExpressionType::Column;
    }

    Value ColumnExpression::Evaluate(const EvaluationContext& context) const{
        return context.table->MaterializeColumn(context.allocator, context.row, this->columnIndex);
        // switch (context.type) {
        // case EvaluationContext::EvaluationContextType::SingleRow:
        //     return this->EvaluateSingleRow(context);
        // case EvaluationContext::EvaluationContextType::MaterializedRow:
        //     return context.materializedRow.GetColumnAt(this->columnIndex);
        // case EvaluationContext::EvaluationContextType::Join:
        //     return this->EvaluateJoin(context);
        // case EvaluationContext::EvaluationContextType::Constant:
        // case EvaluationContext::EvaluationContextType::Aggregate:
        // case EvaluationContext::EvaluationContextType::Window:
        //     break;
        // }
    }

    Value* ColumnExpression::Evaluate(
        const Expression* expression,
        const CoreEngine::ExecutionContext& context,
        const CoreEngine::SelectionVector* selectionVector
    ){
        const auto* columnExpr = expression->AsColumn();
        const auto size = selectionVector->selectedRidsCount;
        auto* allocator = context.GetAllocator();
        auto* valueArray = static_cast<Value*>(allocator->AllocateRaw(size * sizeof(Value)));

        const auto* table = context.GetTable(columnExpr->tableId);
        for (Int i = 0; i < size; i++ ){
            const auto rowIndex = selectionVector->selectedRids[0][i];
            const auto* row = context.GetRid(0, rowIndex);

            valueArray[i] = table->MaterializeColumn(context.GetAllocator(), row, columnExpr->columnIndex);
        }

        return valueArray;
    }

    Value* ColumnExpression::Evaluate(
        const Expression* expression,
        const CoreEngine::ExecutionContext& context,
        const Int rangeEnd
    ){
        const auto* columnExpr = expression->AsColumn();
        const auto* table = context.GetTable(0);
        return table->MaterializeColumn(context, rangeEnd, columnExpr->columnIndex);
    }

    DataType ColumnExpression::GetReturnType() const{ return this->returnType; }

    bool ColumnExpression::HasTableAlias() const { return !this->tableAlias.Empty();}

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

    Value ConstantExpression::Evaluate(const EvaluationContext &context) const{
        return this->value;
    }

    DataType ConstantExpression::GetReturnType() const{ return this->value.GetType(); }

    bool BinaryExpression::ValidateAddition()const{
        const auto leftType = GetExpressionReturnType(this->left);
        const auto rightType = GetExpressionReturnType(this->right);

        switch (Value::PromoteType(leftType, rightType)) {
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

        switch (Value::PromoteType(leftType, rightType)) {
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

        switch (Value::PromoteType(leftType, rightType)) {
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
        return false;
    }

    bool BinaryExpression::ValidateModulo() const{
        const auto leftType = GetExpressionReturnType(this->left);
        const auto rightType = GetExpressionReturnType(this->right);

        switch (Value::PromoteType(leftType, rightType)) {
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

    BinaryExpression::BinaryExpression(Expression *left, Expression *right, const BinaryOperator operation){
        this->left = left;
        this->right = right;
        this->operation = operation;
        this->expressionType = ExpressionType::Binary;
    }

    BinaryExpression::~BinaryExpression() = default;

    //TODO Implement field logical operations.
    Value BinaryExpression::Evaluate(const EvaluationContext& context) const{
        switch (this->operation) {
        case BinaryOperator::Add:
            return EvaluateExpression(this->left, context) + EvaluateExpression(this->right, context);
        case BinaryOperator::Subtract:
            return EvaluateExpression(this->left, context) - EvaluateExpression(this->right, context);
        case BinaryOperator::Multiply:
            return EvaluateExpression(this->left, context) * EvaluateExpression(this->right, context);
        case BinaryOperator::Divide:
            return EvaluateExpression(this->left, context) / EvaluateExpression(this->right, context);
        case BinaryOperator::Modulo:
            return EvaluateExpression(this->left, context) % EvaluateExpression(this->right, context);
        case BinaryOperator::Equal:
            return Value(EvaluateExpression(this->left, context) == EvaluateExpression(this->right, context), context.allocator);
        case BinaryOperator::EqualIgnoreOrdinalCase:
            return Value::EqualsIgnoreOrdinalCase(EvaluateExpression(this->left, context), EvaluateExpression(this->right, context));
        case BinaryOperator::NotEqual:
            return Value(EvaluateExpression(this->left, context) != EvaluateExpression(this->right, context), context.allocator);
        case BinaryOperator::Greater:
            return Value(EvaluateExpression(this->left, context) > EvaluateExpression(this->right, context), context.allocator);
        case BinaryOperator::GreaterEqual:
            return Value(EvaluateExpression(this->left, context) >= EvaluateExpression(this->right, context), context.allocator);
        case BinaryOperator::Less:
            return Value(EvaluateExpression(this->left, context) < EvaluateExpression(this->right, context), context.allocator);
        case BinaryOperator::LessEqual:
            return Value(EvaluateExpression(this->left, context) <= EvaluateExpression(this->right, context), context.allocator);
        default:
            throw std::runtime_error("BinaryExpression::Evaluate: Unknown operator" + std::to_string(static_cast<int>(this->operation)));
        }
    }

    DataType BinaryExpression::GetReturnType() const{
        switch (this->operation) {
        case BinaryOperator::Add:
        case BinaryOperator::Subtract:
        case BinaryOperator::Multiply:
        case BinaryOperator::Divide:
        case BinaryOperator::Modulo: {
            const auto leftType = GetExpressionReturnType(this->left);
            const auto rightType = GetExpressionReturnType(this->right);
            return Value::PromoteType(leftType, rightType);
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
                SqlTypesString[static_cast<int>(expectedType)],
                ", but got type: ",
                SqlTypesString[static_cast<int>(returnType)]
            );
            return false;
        }

        return true;
    }

    void FunctionExpression::ConstructInvalidCastMessage(DataTypes::String& errorMessage, const DataType fromType, const DataType toType) {
        errorMessage = errorMessage.ConcatInPlace(
            "Cannot cast safely type: ",
            SqlTypesString[static_cast<int>(fromType)],
            " to type: ",
            SqlTypesString[static_cast<int>(toType)]
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
        DataStructures::PolymorphicArray<Value> evaluatedArguments(context.allocator, this->arguments.Size());
        for (const auto& arg : this->arguments)
            evaluatedArguments.Push(std::move(EvaluateExpression(arg, context)));

        // if (!this->IsPlugin())
        return FunctionDictionary.Get(this->functionType)(context, evaluatedArguments);

        // External::registry.Get(this->name.ToView())(context, evaluatedArguments);
        // else {
        //     const auto& plugin = PluginDictionary.Get(this->functionType);
        //     return plugin->Evaluate(context, evaluatedArguments);
        // }

    }

    Value FunctionExpression::Concat(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        Value value(DataTypes::String::Null(), context.allocator, 0);

        for (const auto& argument : arguments)
            value += Value(argument.AsString(), context.allocator, 0);

        return value;
    }

    Value FunctionExpression::Length(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0];
        return Value(DataTypes::String::Length(field.AsStringView()), context.allocator, 0);
    }

    Value FunctionExpression::TrimLeft(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0];
        return Value(DataTypes::String::TrimLeft(field.AsStringView()), context.allocator, 0);
    }

    Value FunctionExpression::TrimRight(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0];
        return Value(DataTypes::String::TrimRight(field.AsStringView()), context.allocator, 0);
    }

    Value FunctionExpression::Trim(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0];
        return Value(DataTypes::String::Trim(field.AsStringView()), context.allocator, 0);
    }

    Value FunctionExpression::AsciiValue(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0];
        return Value(DataTypes::String::Ascii(field.AsStringView()), context.allocator, 0);
    }

    Value FunctionExpression::Char(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0];
        return Value(DataTypes::String::Char(field.AsInt(), context.allocator), context.allocator, 0);
    }

    Value FunctionExpression::CharIndex(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& subStr = arguments[0].AsStringView();

        const auto& str = arguments[1].AsStringView();

        const int pos = (arguments.Size() > 2)
                            ? arguments[2].AsInt()
                            : 0;

        return Value(DataTypes::String::CharIndex(subStr, str, pos), context.allocator, 0);
    }

    Value FunctionExpression::Lower(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0];
        return Value(DataTypes::String::Lower(field.AsStringView(), context.allocator), context.allocator, 0);
    }

    Value FunctionExpression::Upper(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0];
        return Value(DataTypes::String::Upper(field.AsStringView(), context.allocator), context.allocator, 0);
    }

    Value FunctionExpression::Replace(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& str = arguments[0].AsStringView();
        const auto& subStr = arguments[1].AsStringView();
        const auto& replaceStr = arguments[2].AsStringView();

        return Value(DataTypes::String::Replace(str, subStr, replaceStr, context.allocator), context.allocator, 0);
    }

    Value FunctionExpression::Substr(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0].AsString();
        const auto& startPos = arguments[1].AsInt();
        const auto& endPos = arguments[2].AsInt();

        return Value(DataTypes::String::SubString(field, startPos, endPos), context.allocator, 0);
    }

    Value FunctionExpression::Left(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0].AsString();
        const auto& startPos = arguments[1].AsInt();

        return Value(DataTypes::String::Left(field, startPos), context.allocator, 0);
    }

    Value FunctionExpression::Right(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& field = arguments[0].AsString();
        const auto& startPos = arguments[1].AsInt();

        return Value(DataTypes::String::Right(field, startPos), context.allocator, 0);
    }

    Value FunctionExpression::Reverse(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& str = arguments[0].AsString();
        return Value(DataTypes::String::Reverse(str), context.allocator, 0);
    }

    Value FunctionExpression::Space(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        const auto& size = arguments[0].AsInt();

        return Value(DataTypes::String::Space(size, context.allocator), context.allocator, 0);
    }

    Value FunctionExpression::GetDate(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        return Value(DataTypes::DateTime::Now(), context.allocator, 0);
    }

    Value FunctionExpression::NewGuid(const EvaluationContext& context, const DataStructures::PolymorphicArray<Value>& arguments){
        return Value(DataTypes::Guid::NewGuid(), context.allocator, 0);
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

        const auto promotedType = Value::PromoteType(
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
            promotedType = Value::PromoteType(promotedType, argType);
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

    Value LogicalExpression::Evaluate(const EvaluationContext& context) const {
        switch (this->logicalType) {
        case LogicalType::And: {
            const auto leftValue = EvaluateExpression(this->left, context);
            const auto rightValue = EvaluateExpression(this->right, context);
            return Value(leftValue.AsBool() && rightValue.AsBool(), context.allocator, 0);
        }
        case LogicalType::Or: {
            const auto leftValue = EvaluateExpression(this->left, context);
            const auto rightValue = EvaluateExpression(this->right, context);
            return Value(leftValue.AsBool() || rightValue.AsBool(), context.allocator, 0);
        }
        case LogicalType::Invalid:
        default:
            throw std::runtime_error("LogicalExpression::Evaluate: Unknown predicate" + std::to_string(static_cast<Int>(this->logicalType)));
        }
    }

    DataType LogicalExpression::GetReturnType() const{ return DataType::Bool; }

    Value BranchExpression::EvaluateSwitch(const EvaluationContext &context) const{
        for (int i = 0;i < this->branches.Size(); i++) {
            if (EvaluateExpression(this->branches[i], context).AsBool())
                return EvaluateExpression(this->results[i], context);
        }

        return EvaluateExpression(this->baseCase, context);
    }

    Value BranchExpression::EvaluateTernary(const EvaluationContext &context) const{
        if (EvaluateExpression(this->branches[0], context).AsBool())
            return EvaluateExpression(this->results[0], context);
        return EvaluateExpression(this->results[1], context);
    }

    BranchExpression::BranchExpression(const BranchType type, const ::Memory::IAllocator* allocator)
        :   branchType(type), branches(allocator),
            results(allocator), arguments(allocator),
            baseCase(nullptr) {
        this->expressionType = ExpressionType::Branch;
    }

    Value BranchExpression::Evaluate(const EvaluationContext &context) const {
        switch (this->branchType) {
        case BranchType::Switch:
            return this->EvaluateSwitch(context);
        case BranchType::Ternary:
            return this->EvaluateTernary(context);
        default:
            throw std::runtime_error("BranchExpression::Evaluate: Unknown expression branching type");
        }
    }

    DataType BranchExpression::GetReturnType() const {
        auto returnType = DataType::String;
        for (const auto& result : this->results)
            returnType = Value::PromoteType(GetExpressionReturnType(result), returnType);

        if (this->HasBaseCase())
            returnType = Value::PromoteType(GetExpressionReturnType(this->baseCase), returnType);

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
        this->normalizedName = DataTypes::String::Normalize(this->name, allocator);
        this->dataType = DataType::Null;
        this->expressionType = ExpressionType::Variable;
    }

    Value VariableExpression::Evaluate(const EvaluationContext &context) const {
        return context.variables->Get(this->normalizedName).GetValue();
    }

    DataType VariableExpression::GetReturnType() const { return this->dataType; }

    Value JsonExpression::EvaluateJsonPath(const EvaluationContext &context, const Value& columnValue) const{
        const auto jsonBinary = columnValue.AsJson();
        const auto jsonValue = jsonBinary.Navigate(this->pathSegments);

        const auto jsonType = jsonValue.Type();
        if (jsonType == Serialization::JsonType::Array
            || jsonType == Serialization::JsonType::Object
        ){
            const auto str = DataTypes::JsonBinary::JsonObjectToString(jsonValue, context.allocator);
            return Value(str, context.allocator);
        }

        return Value(jsonValue, context.allocator);
    }

    JsonExpression::JsonExpression(ColumnExpression* columnPtr, const Memory::IAllocator* allocator)
        :columnPtr(columnPtr), pathSegments(allocator), type(DataType::Null){
        this->expressionType = ExpressionType::Json;
    }

    Value JsonExpression::Evaluate(const EvaluationContext& context) const{
        // switch (context.type) {
        // case EvaluationContext::EvaluationContextType::SingleRow: {
        //     const auto columnValue = context.row->PartialMaterialize(
        //         context.allocator,
        //         this->columnPtr->columnIndex
        //     );
        //     return this->EvaluateJsonPath(context, columnValue);
        // }
        // case EvaluationContext::EvaluationContextType::MaterializedRow: {
        //     const auto columnValue = context.materializedRow.GetColumnAt(this->columnPtr->columnIndex);
        //     return this->EvaluateJsonPath(context, columnValue);
        // }
        // case EvaluationContext::EvaluationContextType::Join:{
        //     const auto columnValue = this->columnPtr->Evaluate(context);
        //     return this->EvaluateJsonPath(context, columnValue);
        // }
        // case EvaluationContext::EvaluationContextType::Constant:
        // case EvaluationContext::EvaluationContextType::Aggregate:
        // case EvaluationContext::EvaluationContextType::Window:
        //     break;
        // }
        //
        // return Value::Null(nullptr);
    }

    DataType JsonExpression::GetReturnType() const{
        return this->type;
    }

    CastExpression::CastExpression(Expression* expression, const DataType targetType, const bool isTryCast)
        : expression(expression), targetType(targetType), isTryCast(isTryCast) {
        this->expressionType = ExpressionType::Cast;
    }

    Value CastExpression::Evaluate(const EvaluationContext& context) const{
        auto value = EvaluateExpression(this->expression, context);

        if (value.IsNull())
            return value;

        // DataTypes::Coercions::CanBeParsedToType(
        //     this->targetType,
        //     value
        // );

        return Value::Null(nullptr);
        //
        // switch (this->targetType){
        // case DataType::String:
        //     return Value(value.AsString(), context.allocator, 0);
        // case DataType::Bool:
        //     return Value(value.AsBool(), context.allocator, 0);
        // case DataType::TinyInt:
        //     return Value(value.AsTinyInt(), context.allocator, 0);
        // case DataType::SmallInt:
        //     return Value(value.AsSmallInt(), context.allocator, 0);
        // case DataType::Int:
        //     return Value(value.AsInt(), context.allocator, 0);
        // case DataType::BigInt:
        //     return Value(value.AsBigInt(), context.allocator, 0);
        // case DataType::Decimal:
        //     return Value(value.AsDecimal(), context.allocator, 0);
        // case DataType::DateTime:
        //     return Value(value.AsDateTime(), context.allocator, 0);
        // case DataType::Guid:
        //     return Value(value.AsGuid(), context.allocator, 0);
        // case DataType::Json:
        //     return Value(value.AsJson(), context.allocator, 0);
        // case DataType::Null:
        //     return Value::Null(context.allocator);
        // case DataType::RowIdentifier:
        //     break;
        // }
    }

    DataType CastExpression::GetReturnType() const{ return this->targetType; }

    Value EvaluateExpression(const Expression* expression, const EvaluationContext& context){
        switch (expression->expressionType){
        case ExpressionType::Column:
            return expression->AsColumn()->Evaluate(context);
        case ExpressionType::Constant:
            return expression->AsConstant()->Evaluate(context);
        case ExpressionType::Binary:
            return expression->AsBinary()->Evaluate(context);
        case ExpressionType::Logical:
            return expression->AsLogical()->Evaluate(context);
        case ExpressionType::Variable:
            return expression->AsVariable()->Evaluate(context);
        case ExpressionType::Branch:
            return expression->AsBranch()->Evaluate(context);
        case ExpressionType::Function:
            return expression->AsFunction()->Evaluate(context);
        case ExpressionType::Json:
            return expression->AsJson()->Evaluate(context);
        case ExpressionType::Cast:
            return expression->AsCast()->Evaluate(context);
        case ExpressionType::Expression:
        default:
            return Value::Null(context.allocator);
        }
    }

    Value* EvaluateExpression(
        const Expression* expression,
        const CoreEngine::ExecutionContext& executionContext,
        const Int rangeEnd
    ){
        switch (expression->expressionType){
        case ExpressionType::Column:
            return ColumnExpression::Evaluate(expression, executionContext, rangeEnd);
        case ExpressionType::Expression:
            break;
        case ExpressionType::Constant:
            break;
        case ExpressionType::Binary:
            break;
        case ExpressionType::Logical:
            break;
        case ExpressionType::Variable:
            break;
        case ExpressionType::Branch:
            break;
        case ExpressionType::Function:
            break;
        case ExpressionType::Json:
            break;
        case ExpressionType::Cast:
            break;
        }

        return nullptr;
    }

    Value* EvaluateExpression(
        const Expression* expression,
        const CoreEngine::ExecutionContext& executionContext,
        const CoreEngine::SelectionVector* selectionVector
    ){
        switch (expression->expressionType){
        case ExpressionType::Column:
            return ColumnExpression::Evaluate(expression, executionContext, selectionVector);
        case ExpressionType::Expression:
            break;
        case ExpressionType::Constant:
            break;
        case ExpressionType::Binary:
            break;
        case ExpressionType::Logical:
            break;
        case ExpressionType::Variable:
            break;
        case ExpressionType::Branch:
            break;
        case ExpressionType::Function:
            break;
        case ExpressionType::Json:
            break;
        case ExpressionType::Cast:
            break;
        }

        return nullptr;
    }

    CoreEngine::SelectionVector* EvaluateFilterExpression(
        const Expression* expression,
        const CoreEngine::ExecutionContext& executionContext,
        CoreEngine::SelectionVector* selectionVector
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

    DataType GetExpressionReturnType(const Expression* expression){
        switch (expression->expressionType){
        case ExpressionType::Column:
            return expression->AsColumn()->GetReturnType();
        case ExpressionType::Constant:
            return expression->AsConstant()->GetReturnType();
        case ExpressionType::Binary:
            return expression->AsBinary()->GetReturnType();
        case ExpressionType::Logical:
            return expression->AsLogical()->GetReturnType();
        case ExpressionType::Variable:
            return expression->AsVariable()->GetReturnType();
        case ExpressionType::Branch:
            return expression->AsBranch()->GetReturnType();
        case ExpressionType::Function:
            return expression->AsFunction()->GetReturnType();
        case ExpressionType::Json:
            return expression->AsJson()->GetReturnType();
        case ExpressionType::Expression:
        default:
            return DataType::Null;
        }
    }
}
