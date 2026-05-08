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
#include "Pages/Additional/RowReference.h"

#include "../../../Systemic/include/DataTypes/Variable.h"
#include "DataTypes/DataTypes.StaticData.h"

namespace Expressions{
    using FunctionPtr   = Value(*)(const EvaluationContext&, const DataStructures::PolymorphicArray<Value>&);
    using ValidationPtr = bool(*)(const DataStructures::PolymorphicArray<Expression*>&, DataTypes::String&);

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

    EvaluationContext::EvaluationContext(const ::Memory::IAllocator* allocator){
        this->type = EvaluationContextType::Constant;
        this->allocator = allocator;
        this->variables = nullptr;
        this->row = nullptr;
    }

    EvaluationContext::EvaluationContext(
        const EvaluationContextType type,
        const ::Memory::IAllocator* allocator
    ){
        this->type = type;
        this->allocator = allocator;
        this->variables = nullptr;
        this->row = nullptr;
    }

    EvaluationContext::EvaluationContext(
        const EvaluationContextType type,
        const CoreEngine::ExecutionContext& executionContext
    ){
        this->type = type;
        this->allocator = executionContext.GetAllocator();
        this->variables = executionContext.GetVariables();
        this->row = nullptr;
    }

    EvaluationContext::EvaluationContext(
      const Pages::RowReference* row,
      const CoreEngine::ExecutionContext& executionContext
    ){
        this->type = EvaluationContextType::SingleRow;
        this->row = row;
        this->allocator = executionContext.GetAllocator();
        this->variables = executionContext.GetVariables();
    }

    // EvaluationContext::EvaluationContext(
    //     const Pages::RowReference* row,
    //     const DatabaseEngine::ExecutionProperties& properties
    // ){
    //     this->type = EvaluationContextType::SingleRow;
    //     this->properties = &executionContext;
    //     this->row = row;
    //     this->outerRow = nullptr;
    //     this->innerRow = nullptr;
    // }

    EvaluationContext::EvaluationContext(
        const QueryResult &row,
        const CoreEngine::ExecutionContext& executionContext
    ) {
        this->type = EvaluationContextType::MaterializedRow;
        this->allocator = executionContext.GetAllocator();
        this->variables = executionContext.GetVariables();
        this->materializedRow = row;
        this->row = nullptr;
    }

    EvaluationContext::EvaluationContext(
        const Pages::RowReference* row,
        const Pages::RowReference* joinRow,
        const CoreEngine::ExecutionContext& executionContext
    ) :     row(row), joinRow(joinRow),
            allocator(executionContext.GetAllocator()),
            variables(executionContext.GetVariables()),
            type(EvaluationContextType::Join){}

    EvaluationContext EvaluationContext::CreateJoinContext(
        const Pages::RowReference* outerRow,
        const Pages::RowReference* innerRow,
        const CoreEngine::ExecutionContext& executionContext
    ){
        return EvaluationContext(outerRow, innerRow, executionContext);
    }

    Value Expression::EvaluateJoin(const EvaluationContext& context) const{
        auto previousColumns = context.row->lazyState->numberOfColumns;
        if (this->columnIndex < previousColumns)
            return context.row->PartialMaterialize(context.allocator, this->columnIndex);

        for (const auto& joinedRow : context.row->lazyState->joinedRows) {
            const auto joinRowColumns = joinedRow.lazyState->numberOfColumns;

            if (this->columnIndex < previousColumns + joinRowColumns)
                return joinedRow.PartialMaterialize(context.allocator, this->columnIndex - previousColumns);

            previousColumns += joinRowColumns;
        }

        if (this->columnIndex < previousColumns + context.joinRow->lazyState->numberOfColumns)
            return context.joinRow->PartialMaterialize(context.allocator, this->columnIndex - previousColumns);

        return Value::Null();
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
    const BinaryExpression * Expression::AsBinary() const{ return static_cast<const BinaryExpression*>(this); }
    const LogicalExpression * Expression::AsLogical() const{ return static_cast<const LogicalExpression*>(this); }

    const ColumnExpression * Expression::AsColumn() const{ return static_cast<const ColumnExpression*>(this); }
    const VariableExpression * Expression::AsVariable() const{ return static_cast<const VariableExpression*>(this); }
    const ConstantExpression * Expression::AsConstant() const{ return static_cast<const ConstantExpression*>(this); }
    const BranchExpression * Expression::AsBranch() const{ return static_cast<const BranchExpression*>(this); }
    const FunctionExpression * Expression::AsFunction() const{ return static_cast<const FunctionExpression*>(this); }
    const JsonExpression* Expression::AsJson() const{ return static_cast<const JsonExpression*>(this); }
    void Expression::SetIndex(const column_index_t index){ this->columnIndex = index; }

    Value ColumnExpression::EvaluateSingleRow(const EvaluationContext& context) const{
        auto previousColumns = context.row->lazyState->numberOfColumns;
        if (this->columnIndex < previousColumns)
            return context.row->PartialMaterialize(context.allocator, this->columnIndex);

        for (const auto& joinedRow : context.row->lazyState->joinedRows) {
            const auto joinRowColumns = joinedRow.lazyState->numberOfColumns;

            if (this->columnIndex < previousColumns + joinRowColumns)
                return joinedRow.PartialMaterialize(context.allocator, this->columnIndex - previousColumns);

            previousColumns += joinRowColumns;
        }

        return Value::Null();
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
        switch (context.type) {
        case EvaluationContext::EvaluationContextType::SingleRow:
            return this->EvaluateSingleRow(context);
        case EvaluationContext::EvaluationContextType::MaterializedRow:
            return context.materializedRow.GetColumnAt(this->columnIndex);
        case EvaluationContext::EvaluationContextType::Join:
            return this->EvaluateJoin(context);
        case EvaluationContext::EvaluationContextType::Constant:
        case EvaluationContext::EvaluationContextType::Aggregate:
        case EvaluationContext::EvaluationContextType::Window:
            break;
        }

        return Value::Null();
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
        const auto leftType = this->left->GetReturnType();
        const auto rightType = this->right->GetReturnType();

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
        const auto leftType = this->left->GetReturnType();
        const auto rightType = this->right->GetReturnType();

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
        const auto leftType = this->left->GetReturnType();
        const auto rightType = this->right->GetReturnType();

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
        const auto leftType = this->left->GetReturnType();
        const auto rightType = this->right->GetReturnType();

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
            return this->left->Evaluate(context) + this->right->Evaluate(context);
        case BinaryOperator::Subtract:
            return this->left->Evaluate(context) - this->right->Evaluate(context);
        case BinaryOperator::Multiply:
            return this->left->Evaluate(context) * this->right->Evaluate(context);
        case BinaryOperator::Divide:
            return this->left->Evaluate(context) / this->right->Evaluate(context);
        case BinaryOperator::Modulo:
            return this->left->Evaluate(context) % this->right->Evaluate(context);
        case BinaryOperator::Equal:
            return Value(this->left->Evaluate(context) == this->right->Evaluate(context), context.allocator);
        case BinaryOperator::EqualIgnoreOrdinalCase:
            return Value::EqualsIgnoreOrdinalCase(this->left->Evaluate(context), this->right->Evaluate(context));
        case BinaryOperator::NotEqual:
            return Value(this->left->Evaluate(context) != this->right->Evaluate(context), context.allocator);
        case BinaryOperator::Greater:
            return Value(this->left->Evaluate(context) > this->right->Evaluate(context), context.allocator);
        case BinaryOperator::GreaterEqual:
            return Value(this->left->Evaluate(context) >= this->right->Evaluate(context), context.allocator);
        case BinaryOperator::Less:
            return Value(this->left->Evaluate(context) < this->right->Evaluate(context), context.allocator);
        case BinaryOperator::LessEqual:
            return Value(this->left->Evaluate(context) <= this->right->Evaluate(context), context.allocator);
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
            const auto& leftType = this->left->GetReturnType();
            const auto& rightType = this->right->GetReturnType();
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
            if (!FunctionExpression::ValidateReturnType(info, errorMessage, expectedType, this->arguments[i]->GetReturnType(), i))
                return false;

        return true;
    }

    bool FunctionExpression::ValidateArgumentTypes(const FunctionInfo &info, DataTypes::String& errorMessage) const{
        for (int i = 0;i < this->arguments.Size(); i++)
            if (!FunctionExpression::ValidateReturnType(info, errorMessage, info.expectedTypes[i], this->arguments[i]->GetReturnType(), i))
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
            evaluatedArguments.Push(std::move(arg->Evaluate(context)));

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
        const auto& firstArgumentType = arguments[0]->GetReturnType();
        const auto& secondArgumentType = arguments[1]->GetReturnType();

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

    bool FunctionExpression::ValidateCoalesce(const DataStructures::PolymorphicArray<Expression*>& arguments, DataTypes::String& errorMessage){
        auto promotedType = DataType::String;

        std::vector<DataType> argTypes;
        for (const auto& argument : arguments) {
            const auto argType = argument->GetReturnType();

            argTypes.push_back(argType);
            promotedType = Value::PromoteType(promotedType, argType);
        }

        for (const auto& type : argTypes) {
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
            const auto leftValue = this->left->Evaluate(context);
            const auto rightValue = this->right->Evaluate(context);
            return Value(leftValue.AsBool() && rightValue.AsBool(), context.allocator, 0);
        }
        case LogicalType::Or: {
            const auto leftValue = this->left->Evaluate(context);
            const auto rightValue = this->right->Evaluate(context);
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
            if (this->branches[i]->Evaluate(context).AsBool())
                return this->results[i]->Evaluate(context);
        }

        return this->baseCase->Evaluate(context);
    }

    Value BranchExpression::EvaluateTernary(const EvaluationContext &context) const{
        if (this->branches[0]->Evaluate(context).AsBool())
            return this->results[0]->Evaluate(context);
        return this->results[1]->Evaluate(context);
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
            returnType = Value::PromoteType(result->GetReturnType(), returnType);

        if (this->HasBaseCase())
            returnType = Value::PromoteType(this->baseCase->GetReturnType(), returnType);

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
        switch (context.type) {
        case EvaluationContext::EvaluationContextType::SingleRow: {
            const auto columnValue = context.row->PartialMaterialize(context.allocator, this->columnPtr->columnIndex);
            return this->EvaluateJsonPath(context, columnValue);
        }
        case EvaluationContext::EvaluationContextType::MaterializedRow: {
            const auto columnValue = context.materializedRow.GetColumnAt(this->columnPtr->columnIndex);
            return this->EvaluateJsonPath(context, columnValue);
        }
        case EvaluationContext::EvaluationContextType::Join:
            return this->EvaluateJsonPath(context, this->EvaluateJoin(context));
        case EvaluationContext::EvaluationContextType::Constant:
        case EvaluationContext::EvaluationContextType::Aggregate:
        case EvaluationContext::EvaluationContextType::Window:
            break;
        }

        return Value::Null(nullptr);
    }

    DataType JsonExpression::GetReturnType() const{
        return this->type;
    }
}
