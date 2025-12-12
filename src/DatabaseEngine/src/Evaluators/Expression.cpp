#include "../../include/Evaluators/Expression.h"

#include "../../../Systemic/include/Coercions.h"
#include "../../../Systemic/include/DataTypes/Value.h"
#include "../../../Systemic/include/QueryResult.h"
#include "../../../Systemic/include/Functions/StringFunctions.h"
#include "../../include/DataStorage/Row.h"
#include <iostream>

namespace Expressions{
     static Dictionary<Constants::FunctionType, std::function<Value(const std::vector<Value>& args)>> FunctionDictionary{
            //Date Functions

          { Constants::FunctionType::GetDate,    &FunctionExpression::GetDate },
          { Constants::FunctionType::DateAdd,    &FunctionExpression::GetDate },
          { Constants::FunctionType::DateDiff,   &FunctionExpression::GetDate },
          { Constants::FunctionType::DatePart,   &FunctionExpression::GetDate },
          { Constants::FunctionType::Year,       &FunctionExpression::GetDate },
          { Constants::FunctionType::Month,      &FunctionExpression::GetDate },
          { Constants::FunctionType::Day,        &FunctionExpression::GetDate },

            //Guid Functions

          { Constants::FunctionType::NewGuid,    &FunctionExpression::NewGuid },

            //String Functions

          { Constants::FunctionType::Concat,     &FunctionExpression::Concat },
          { Constants::FunctionType::Length,     &FunctionExpression::Length },
          { Constants::FunctionType::AsciiValue, &FunctionExpression::AsciiValue },
          { Constants::FunctionType::Char,       &FunctionExpression::Char },
          { Constants::FunctionType::CharIndex,  &FunctionExpression::CharIndex },
          { Constants::FunctionType::Lower,      &FunctionExpression::Lower },
          { Constants::FunctionType::Upper,      &FunctionExpression::Upper },
          { Constants::FunctionType::Trim,       &FunctionExpression::Trim },
          { Constants::FunctionType::TrimLeft,   &FunctionExpression::TrimLeft },
          { Constants::FunctionType::TrimRight,  &FunctionExpression::TrimRight },
          { Constants::FunctionType::Replace,    &FunctionExpression::Replace },
          { Constants::FunctionType::Substr,     &FunctionExpression::Substr },
          { Constants::FunctionType::Left,       &FunctionExpression::Left },
          { Constants::FunctionType::Right,      &FunctionExpression::Right },
          { Constants::FunctionType::Reverse,    &FunctionExpression::Reverse },
          { Constants::FunctionType::Space,      &FunctionExpression::Space },

          { Constants::FunctionType::NullIf,     &FunctionExpression::NullIf },
          { Constants::FunctionType::Coalesce,   &FunctionExpression::Coalesce },
    };

    static Dictionary<Constants::FunctionType, std::function<bool(const std::vector<Expressions::Expression*>& arguments, std::string& errorMessage)>> FunctionAdditionalValidationsDictionary{
          {Constants::FunctionType::NullIf,     &FunctionExpression::ValidateNullIf},
          {Constants::FunctionType::Coalesce,   &FunctionExpression::ValidateCoalesce},
    };

  EvaluationContext::EvaluationContext() {
    this->type = EvaluationContextType::Constant;
    this->row = nullptr;
    this->outerRow = nullptr;
    this->innerRow = nullptr;
    this->variables = nullptr;
  }

  EvaluationContext::EvaluationContext(const EvaluationContextType &type, const Dictionary<std::string, Variable>* variables) {
      this->type = type;
      this->variables = variables;
      this->row = nullptr;
      this->outerRow = nullptr;
      this->innerRow = nullptr;
   }

  EvaluationContext::EvaluationContext(const DatabaseEngine::StorageTypes::Row *row){
      this->type = EvaluationContextType::SingleRow;
      this->row = row;
      this->outerRow = nullptr;
      this->innerRow = nullptr;
      this->variables = nullptr;
  }

  EvaluationContext::EvaluationContext(const DatabaseEngine::StorageTypes::Row *outerRow, const DatabaseEngine::StorageTypes::Row *innerRow) {
      this->type = EvaluationContextType::Join;
      this->outerRow = outerRow;
      this->innerRow = innerRow;
      this->row = nullptr;
      this->variables = nullptr;
   }

  bool Expression::IsBinary() const{ return this->expressionType == ExpressionType::Binary; }
  bool Expression::IsLogical() const{ return this->expressionType == ExpressionType::Logical; }
  bool Expression::IsConstant() const{ return this->expressionType == ExpressionType::Constant; }
  bool Expression::IsVariable() const{ return this->expressionType == ExpressionType::Variable; }
  bool Expression::IsColumn() const{ return this->expressionType == ExpressionType::Column; }
  bool Expression::IsFunction() const{ return this->expressionType == ExpressionType::Function; }
  bool Expression::IsBranch() const{ return this->expressionType == ExpressionType::Branch; }

  BinaryExpression * Expression::AsBinary(){ return this->IsBinary() ? static_cast<BinaryExpression*>(this) : nullptr; }
  LogicalExpression * Expression::AsLogical(){ return this->IsLogical() ? static_cast<LogicalExpression*>(this) : nullptr; }
  ColumnExpression * Expression::AsColumn(){ return this->IsColumn() ? static_cast<ColumnExpression*>(this) : nullptr; }
  VariableExpression * Expression::AsVariable(){ return this->IsVariable() ? static_cast<VariableExpression*>(this) : nullptr; }
  ConstantExpression * Expression::AsConstant(){ return this->IsConstant() ? static_cast<ConstantExpression*>(this) : nullptr; }
  BranchExpression * Expression::AsBranch(){ return this->IsBranch() ? static_cast<BranchExpression*>(this) : nullptr; }
  FunctionExpression * Expression::AsFunction(){ return this->IsFunction() ? static_cast<FunctionExpression*>(this) : nullptr; }

  const BinaryExpression * Expression::AsBinary() const{ return this->IsBinary() ? static_cast<const BinaryExpression*>(this) : nullptr; }
  const LogicalExpression * Expression::AsLogical() const{ return this->IsLogical() ? static_cast<const LogicalExpression*>(this) : nullptr; }
  const ColumnExpression * Expression::AsColumn() const{ return this->IsColumn() ? static_cast<const ColumnExpression*>(this) : nullptr; }
  const VariableExpression * Expression::AsVariable() const{ return this->IsVariable() ? static_cast<const VariableExpression*>(this) : nullptr; }
  const ConstantExpression * Expression::AsConstant() const{ return this->IsConstant() ? static_cast<const ConstantExpression*>(this) : nullptr; }
  const BranchExpression * Expression::AsBranch() const{ return this->IsBranch() ? static_cast<const BranchExpression*>(this) : nullptr; }
  const FunctionExpression * Expression::AsFunction() const{ return this->IsFunction() ? static_cast<const FunctionExpression*>(this) : nullptr; }

  EvaluationContext::EvaluationContext(const QueryResult &row) {
      this->type = EvaluationContextType::MaterializedRow;
      this->row = nullptr;
      this->outerRow = nullptr;
      this->innerRow = nullptr;
      this->materializedRow = row;
      this->variables = nullptr;
   }

  ColumnExpression::ColumnExpression(const std::string &name, const std::string &tableAlias){
    this->alias = name;
    this->tableAlias = tableAlias;

    this->tableId = Constants::INVALID_TABLE_ID;
    this->columnId = Constants::INVALID_COLUMN_ID;
    this->index = 0;
    this->size = 0;
    this->returnType = DataType::Unknown;
    this->expressionType = ExpressionType::Column;
  }

  ColumnExpression::ColumnExpression(const column_index_t &index){
    this->index = index;
    this->size = 0;
    this->returnType = DataType::Unknown;
    this->tableId = Constants::INVALID_TABLE_ID;
    this->columnId = Constants::INVALID_COLUMN_ID;
    this->expressionType = ExpressionType::Column;
  }

  DataType ColumnExpression::GetReturnType() const{ return this->returnType; }

  bool ColumnExpression::HasTableAlias() const { return !this->tableAlias.empty();}

  Value ColumnExpression::Evaluate(const EvaluationContext& context) const{
    switch (context.type) {
    case EvaluationContext::EvaluationContextType::SingleRow:
      return context.row->GetColumnByIndex(this->index);
    case EvaluationContext::EvaluationContextType::MaterializedRow:
      return context.materializedRow.GetData().at(this->index);
    case EvaluationContext::EvaluationContextType::Join: {
      const auto& outerRowData = context.outerRow->GetData();

      //figure out index assignment
      return this->index < outerRowData.size()
          ? context.outerRow->GetColumnByIndex(this->index)
          : context.innerRow->GetColumnByIndex(this->index - outerRowData.size());
    }
    case EvaluationContext::EvaluationContextType::Constant:
    case EvaluationContext::EvaluationContextType::Aggregate:
    case EvaluationContext::EvaluationContextType::Window:
      break;
    }

    return {};
  }

  ConstantExpression::ConstantExpression(const Value &value){
    this->value = value;
    this->expressionType = ExpressionType::Constant;
  }

  ConstantExpression::ConstantExpression(Value &value) {
    this->value = std::move(value);
    this->expressionType = ExpressionType::Constant;
  }

  Value ConstantExpression::Evaluate(const EvaluationContext &context) const{
    return this->value;
  }

  DataType ConstantExpression::GetReturnType() const{ return this->value.GetType(); }

  BinaryExpression::BinaryExpression(Expression *left, Expression *right, const BinaryOperator &operation){
    this->left = left;
    this->right = right;
    this->operation = operation;
    this->expressionType = ExpressionType::Binary;
  }

  BinaryExpression::~BinaryExpression(){
    delete this->left;
    delete this->right;
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
        throw std::runtime_error("Unknown operator" + std::to_string(static_cast<int>(this->operation)));
    }
  }


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
      case DataType::UnicodeString:
      case DataType::Bool:
        return true;
      case DataType::DateTime:
      case DataType::Guid:
      case DataType::RowIdentifier:
      case DataType::Unknown:
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
      case DataType::UnicodeString:
      case DataType::DateTime:
      case DataType::Guid:
      case DataType::RowIdentifier:
      case DataType::Unknown:
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
      case DataType::UnicodeString:
      case DataType::DateTime:
      case DataType::Guid:
      case DataType::RowIdentifier:
      case DataType::Unknown:
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
      case DataType::UnicodeString:
      case DataType::DateTime:
      case DataType::Guid:
      case DataType::RowIdentifier:
      case DataType::Unknown:
      default:
        return false;
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
        throw std::runtime_error("Unknown operator" + std::to_string(static_cast<int>(this->operation)));
    }
  }

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
        return this->left->Evaluate(context) == this->right->Evaluate(context);
      case BinaryOperator::EqualIgnoreOrdinalCase:
        return Value::EqualsIgnoreOrdinalCase(this->left->Evaluate(context), this->right->Evaluate(context));
      case BinaryOperator::NotEqual:
        return this->left->Evaluate(context) != this->right->Evaluate(context);
      case BinaryOperator::Greater:
        return this->left->Evaluate(context) > this->right->Evaluate(context);
      case BinaryOperator::GreaterEqual:
        return this->left->Evaluate(context) >= this->right->Evaluate(context);
      case BinaryOperator::Less:
        return this->left->Evaluate(context) < this->right->Evaluate(context);
      case BinaryOperator::LessEqual:
        return this->left->Evaluate(context) <= this->right->Evaluate(context);
      default:
          throw std::runtime_error("Unknown operator" + std::to_string(static_cast<int>(this->operation)));
    }
  }

  LogicalExpression::LogicalExpression(
    Expression *leftExpression,
    Expression *RightExpression,
    const LogicalType &logicalType
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

  LogicalExpression::~LogicalExpression(){
    delete this->left;
    delete this->right;
  }

  bool LogicalExpression::IsOr() const{ return this->logicalType == LogicalType::Or; }

  bool LogicalExpression::IsAnd() const{ return this->logicalType == LogicalType::And; }

  bool LogicalExpression::HasAtLeastOneConstant() const{ return this->left->IsConstant() || this->right->IsConstant(); }

  DataType LogicalExpression::GetReturnType() const{ return DataType::Bool; }

  Value BranchExpression::EvaluateSwitch(const EvaluationContext &context) const{
    for (int i = 0;i < this->branches.size(); i++) {
      if (this->branches[i]->Evaluate(context).GetBool())
        return this->results[i]->Evaluate(context);
    }

    return this->baseCase->Evaluate(context);
  }

  Value BranchExpression::EvaluateTernary(const EvaluationContext &context) const{
    if (this->branches[0]->Evaluate(context).GetBool())
      return this->results[0]->Evaluate(context);

    return this->results[1]->Evaluate(context);
  }

  BranchExpression::BranchExpression(const BranchType &type) {
    this->branchType = type;
    this->baseCase = nullptr;
    this->expressionType = ExpressionType::Branch;
  }

  BranchExpression::~BranchExpression() {
    for (const auto* branch : this->branches)
      delete branch;

    for (const auto* result : this->results)
      delete result;

    delete this->baseCase;
  }

  Value BranchExpression::Evaluate(const EvaluationContext &context) const {
    switch (this->branchType) {
      case BranchType::Switch:
        return this->EvaluateSwitch(context);
      case BranchType::Ternary:
        return this->EvaluateTernary(context);
      default:
        throw std::runtime_error("Unknown expression branching type");
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
        return this->branches.size() > 0 && this->branches.size() == this->results.size() && this->baseCase != nullptr;
      case BranchType::Ternary:
        return this->branches.size() == 1 && this->results.size() == 2 && this->baseCase == nullptr;
      default:
          throw std::runtime_error("Unknown expression branching type");
    }
  }

  VariableExpression::VariableExpression(const std::string &name) {
    this->name = name;
    this->normalizedName = Functions::String::NormalizeString(this->name);
    this->dataType = DataType::Unknown;
    this->expressionType = ExpressionType::Variable;
  }

  Value VariableExpression::Evaluate(const EvaluationContext &context) const {
    return context.variables->Get(this->normalizedName).GetValue();
  }

  DataType VariableExpression::GetReturnType() const { return this->dataType; }

  Value LogicalExpression::Evaluate(const EvaluationContext& context) const {
    switch (this->logicalType) {
      case LogicalType::And:{
        const auto leftValue = this->left->Evaluate(context);
        const auto rightValue = this->right->Evaluate(context);

        return Value(leftValue.GetBool() && rightValue.GetBool(), 0);
      }
      case LogicalType::Or:{
        const auto leftValue = this->left->Evaluate(context);
        const auto rightValue = this->right->Evaluate(context);

        return Value(leftValue.GetBool() || rightValue.GetBool(), 0);
      }
      case LogicalType::Invalid:
      default:
      throw std::runtime_error("Unknown predicate" + std::to_string(static_cast<int>(this->logicalType)));
    }
  }

  FunctionExpression::FunctionExpression(const Constants::FunctionType& functionType, std::vector<Expression*>& arguments) {
    this->functionType = functionType;
    this->arguments = std::move(arguments);
    this->expressionType = ExpressionType::Function;
  }

  FunctionExpression::~FunctionExpression() {
    for (const auto* expression: this->arguments)
      delete expression;
  }

  bool FunctionExpression::ValidateNumberOfArguments(std::string& errorMessage)const {
    const auto& info = FunctionInfoDictionary.Get(this->functionType);

    const auto argSize = this->arguments.size();
    if (argSize < info.minArgs || (argSize > info.maxArgs && info.maxArgs != UNLIMITED_ARGS)) {

      std::ostringstream message;

      message << "Function: " << info.name
              << " expects number of arguments from: "
              << info.minArgs << " to "
              << (info.maxArgs == UNLIMITED_ARGS ? "unlimited" : to_string(info.maxArgs))
              << " but " << argSize << " were given";

      errorMessage = message.str();
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

  bool FunctionExpression::ValidateUnlimitedArgumentTypes(const FunctionInfo& info, std::string& errorMessage)const{
    const auto& expectedType = info.expectedTypes.front();

    for (int i = 0;i < this->arguments.size(); i++)
      if (!FunctionExpression::ValidateReturnType(info, errorMessage, expectedType, this->arguments[i]->GetReturnType(), i))
        return false;

    return true;
  }

  bool FunctionExpression::ValidateArgumentTypes(const FunctionInfo &info, std::string &errorMessage) const{
    for (int i = 0;i < this->arguments.size(); i++)
      if (!FunctionExpression::ValidateReturnType(info, errorMessage, info.expectedTypes[i], this->arguments[i]->GetReturnType(), i))
        return false;

    return true;
  }

  bool FunctionExpression::ValidateReturnType(
      const FunctionInfo &info,
      std::string &errorMessage,
      const DataType& expectedType,
      const DataType& returnType,
      const int& index
  ) {
    if (returnType == DataType::Unknown) {
      errorMessage = "Function: " + info.name +
                        " has an argument at position " + std::to_string(index + 1) +
                        " with invalid type";
      return false;
    }

    if (!DataTypes::Coercions::IsCoercionAllowed(returnType, expectedType, info.allowImplicitCast)) {
      errorMessage = "Function: " + info.name +
                     " expects argument " + std::to_string(index + 1) +
                     " to be of type: " + ColumnTypesToStringDictionary.Get(expectedType) +
                     ", but got type: " + ColumnTypesToStringDictionary.Get(returnType);
      return false;
    }

    return true;
  }

  void FunctionExpression::ConstructInvalidCastMessage(std::string &errorMessage, const DataType &fromType, const DataType &toType) {
    ostringstream os;

    os  << "Cannot cast safely type: "
        << ColumnTypesToStringDictionary.Get(fromType)
        << " to type "
        << ColumnTypesToStringDictionary.Get(toType);

    errorMessage = os.str();
  }

  bool FunctionExpression::PerformAdditionalValidations(std::string& errorMessage)const {
    return FunctionAdditionalValidationsDictionary.Get(this->functionType)(this->arguments, errorMessage);
  }

  bool FunctionExpression::ValidateNullIf(const std::vector<Expressions::Expression*>& arguments, std::string &errorMessage) {
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

  bool FunctionExpression::ValidateCoalesce(const std::vector<Expressions::Expression*>& arguments, std::string &errorMessage){
    auto promotedType = DataType::String;

    std::vector<DataType> argTypes;
    for (const auto& argument : arguments) {
      const auto argType = argument->GetReturnType();

      argTypes.push_back(argType);
      promotedType = Value::PromoteType(promotedType, argType);
    }

    for (const auto& type : argTypes) {
      if (!DataTypes::Coercions::IsCoercionAllowed(type, promotedType)) {
        FunctionExpression::ConstructInvalidCastMessage(errorMessage, type, promotedType);
        return false;
      }
    }

    return true;
  }

  DataType FunctionExpression::GetReturnType() const{
    return FunctionInfoDictionary.Get(this->functionType).returnType;
  }

  Value FunctionExpression::Evaluate(const EvaluationContext& context) const {
    std::vector<Value> argVals;
    argVals.reserve(this->arguments.size());

    for (const auto& arg : this->arguments)
      argVals.emplace_back(arg->Evaluate(context));

    return FunctionDictionary.Get(this->functionType)(argVals);
  }

  Value FunctionExpression::Concat(const std::vector<Value>& arguments){
    Value value(string(""), 0);

    for (const auto& argument : arguments)
      value += Value(argument.GetString(), 0);

    return value;
  }

  Value FunctionExpression::Length(const std::vector<Value>& arguments){
    const auto& field = arguments.front();

    return Value(Functions::String::Length(field.GetString()), 0);
  }

  Value FunctionExpression::TrimLeft(const std::vector<Value>& arguments){
    const auto& field = arguments.front();

    return Value(Functions::String::TrimLeft(field.GetString()), 0);
  }

  Value FunctionExpression::TrimRight(const std::vector<Value>& arguments){
    const auto& field = arguments.front();

    return Value(Functions::String::TrimRight(field.GetString()), 0);
  }

  Value FunctionExpression::Trim(const std::vector<Value>& arguments){
    const auto& field = arguments.front();

    return Value(Functions::String::Trim(field.GetString()), 0);
  }

  Value FunctionExpression::AsciiValue(const std::vector<Value>& arguments){
    const auto& field = arguments.front();

    return Value(Functions::String::Ascii(field.GetString()), 0);
  }

  Value FunctionExpression::Char(const std::vector<Value>& arguments){
    const auto& field = arguments.front();

    return Value(Functions::String::Char(field.GetInt()), 0);
  }

  Value FunctionExpression::CharIndex(const std::vector<Value>& arguments){
    const auto& subStr = arguments.front().GetString();

    const auto& str = arguments.at(1).GetString();

    const int pos = (arguments.size() > 2)
        ? arguments.at(2).GetInt()
        : 0;

    return Value(Functions::String::CharIndex(subStr, str, pos), 0);
  }

  Value FunctionExpression::Lower(const std::vector<Value>& arguments){
    const auto& field = arguments.front();

    return Value(Functions::String::Lower(field.GetString()), 0);
  }

  Value FunctionExpression::Upper(const std::vector<Value>& arguments){
    const auto& field = arguments.front();

    return Value(Functions::String::Upper(field.GetString()), 0);
  }

  Value FunctionExpression::Replace(const std::vector<Value>& arguments){
    const auto& str = arguments.at(0).GetString();

    const auto& subStr = arguments.at(1).GetString();

    const auto& replaceStr = arguments.at(2).GetString();

    return Value(Functions::String::Replace(str, subStr, replaceStr), 0);
  }

  Value FunctionExpression::Substr(const std::vector<Value>& arguments){
    const auto& field = arguments.at(0).GetString();

    const auto& startPos = arguments.at(1).GetInt();

    const auto& endPos = arguments.at(2).GetInt();

    return Value(Functions::String::SubString(field, startPos, endPos), 0);
  }

  Value FunctionExpression::Left(const std::vector<Value>& arguments){
    const auto& field = arguments.at(0).GetString();

    const auto& startPos = arguments.at(1).GetInt();

    return Value(Functions::String::Left(field, startPos), 0);
  }

  Value FunctionExpression::Right(const std::vector<Value>& arguments){
    const auto& field = arguments.at(0).GetString();

    const auto& startPos = arguments.at(1).GetInt();

    return Value(Functions::String::Right(field, startPos), 0);
  }

  Value FunctionExpression::Reverse(const std::vector<Value>& arguments){
    const auto& str = arguments.at(0).GetString();

    return Value(Functions::String::Reverse(str), 0);
  }

  Value FunctionExpression::Space(const std::vector<Value>& arguments){
    const auto& size = arguments.at(0).GetInt();

    return Value(Functions::String::Space(size), 0);
  }

  Value FunctionExpression::GetDate(const std::vector<Value>& arguments){
    return Value(DataTypes::DateTime::Now(), 0);
  }

  Value FunctionExpression::NewGuid(const std::vector<Value>& arguments){
    return Value(DataTypes::Guid::NewGuid(), 0);
  }

  Value FunctionExpression::NullIf(const std::vector<Value> &arguments) {
    auto& firstArg = arguments.front();
    const auto& secondArg = arguments.at(1);

    return (firstArg == secondArg).GetBool()
        ? Value(nullptr, 0)
          : firstArg;
  }

  Value FunctionExpression::Coalesce(const std::vector<Value> &arguments) {
    for (auto& argument : arguments) {
      if (!argument.IsNull())
        return argument;
    }

    return arguments.front();
  }
}