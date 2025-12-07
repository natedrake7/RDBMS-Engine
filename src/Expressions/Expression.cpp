#include "Expression.h"

#include "../Systemic/Coercions/Coercions.h"
#include "../Systemic/DataTypes/Value/Value.h"
#include "../Systemic/QueryResult/QueryResult.h"
#include "../Systemic/Functions/StringFunctions.h"
#include "../Database/Row/Row.h"
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
          { Constants::FunctionType::Space,      &FunctionExpression::Space }
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
    this->returnType = DataType::Invalid;
  }

  ColumnExpression::ColumnExpression(const column_index_t &index){
    this->index = index;
    this->size = 0;
    this->returnType = DataType::Invalid;
    this->tableId = Constants::INVALID_TABLE_ID;
    this->columnId = Constants::INVALID_COLUMN_ID;
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

  LiteralExpression::LiteralExpression(const Value &value){
    this->value = value;
  }

  LiteralExpression::LiteralExpression(Value &value) {
    this->value = std::move(value);
  }

  Value LiteralExpression::Evaluate(const EvaluationContext &context) const{
    return this->value;
  }

  DataType LiteralExpression::GetReturnType() const{ return this->value.GetType(); }

  BinaryExpression::BinaryExpression(Expression *left, Expression *right, const BinaryOperator &operation){
    this->left = left;
    this->right = right;
    this->operation = operation;
  }

  BinaryExpression::~BinaryExpression(){
    delete this->left;
    delete this->right;
  }

  DataType BinaryExpression::GetReturnType() const{
    const auto& leftType = this->left->GetReturnType();

    const auto& rightType = this->right->GetReturnType();

    return Value::PromoteType(leftType, rightType);
  }


  bool BinaryExpression::ValidateAddition()const{
    const auto leftType = this->left->GetReturnType();
    const auto rightType = this->right->GetReturnType();

    switch (Value::PromoteType(leftType, rightType)) {
      case Constants::DataType::TinyInt:
      case Constants::DataType::SmallInt:
      case Constants::DataType::Int:
      case Constants::DataType::BigInt:
      case Constants::DataType::Decimal:
      case Constants::DataType::String:
      case Constants::DataType::UnicodeString:
      case Constants::DataType::Bool:
        return true;
      case Constants::DataType::DateTime:
      case Constants::DataType::Guid:
      case Constants::DataType::RowIdentifier:
      case Constants::DataType::Invalid:
      default:
        return false;
    }
  }

  bool BinaryExpression::ValidateSubtraction() const{
    const auto leftType = this->left->GetReturnType();
    const auto rightType = this->right->GetReturnType();

    switch (Value::PromoteType(leftType, rightType)) {
      case Constants::DataType::TinyInt:
      case Constants::DataType::SmallInt:
      case Constants::DataType::Int:
      case Constants::DataType::BigInt:
      case Constants::DataType::Decimal:
      case Constants::DataType::Bool:
        return true;
      case Constants::DataType::String:
      case Constants::DataType::UnicodeString:
      case Constants::DataType::DateTime:
      case Constants::DataType::Guid:
      case Constants::DataType::RowIdentifier:
      case Constants::DataType::Invalid:
      default:
        return false;
    }
  }

  bool BinaryExpression::ValidateMultiplication() const{
    const auto leftType = this->left->GetReturnType();
    const auto rightType = this->right->GetReturnType();

    switch (Value::PromoteType(leftType, rightType)) {
      case Constants::DataType::TinyInt:
      case Constants::DataType::SmallInt:
      case Constants::DataType::Int:
      case Constants::DataType::BigInt:
      case Constants::DataType::Decimal:
      case Constants::DataType::Bool:
        return true;
      case Constants::DataType::String:
      case Constants::DataType::UnicodeString:
      case Constants::DataType::DateTime:
      case Constants::DataType::Guid:
      case Constants::DataType::RowIdentifier:
      case Constants::DataType::Invalid:
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
      case Constants::DataType::TinyInt:
      case Constants::DataType::SmallInt:
      case Constants::DataType::Int:
      case Constants::DataType::BigInt:
      case Constants::DataType::Bool:
      case Constants::DataType::Decimal:
        return true;
      case Constants::DataType::String:
      case Constants::DataType::UnicodeString:
      case Constants::DataType::DateTime:
      case Constants::DataType::Guid:
      case Constants::DataType::RowIdentifier:
      case Constants::DataType::Invalid:
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
    const LogicalType &type){
    this->type = type;
    this->left = leftExpression;
    this->right = RightExpression;
  }

  LogicalExpression::LogicalExpression(){
    this->type = LogicalType::Invalid;
    this->left = nullptr;
    this->right = nullptr;
  }

  LogicalExpression::~LogicalExpression(){
    delete this->left;
    delete this->right;
  }

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
    this->type = type;
    this->baseCase = nullptr;
  }

  Value BranchExpression::Evaluate(const EvaluationContext &context) const {
    switch (this->type) {
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
    return this->type == BranchType::Switch;
  }

  bool BranchExpression::ValidateNumberOfArguments() const {
    switch (this->type) {
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
    this->type = DataType::Invalid;
  }

  Value VariableExpression::Evaluate(const EvaluationContext &context) const {
    return context.variables->Get(this->normalizedName).GetValue();
  }

  DataType VariableExpression::GetReturnType() const { return this->type; }

  Value LogicalExpression::Evaluate(const EvaluationContext& context) const {
    switch (this->type) {
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
      throw std::runtime_error("Unknown predicate" + std::to_string(static_cast<int>(this->type)));
    }
  }

  FunctionExpression::FunctionExpression(const Constants::FunctionType& type, std::vector<Expression*>& arguments) {
    this->type = type;
    this->arguments = std::move(arguments);
  }

  FunctionExpression::~FunctionExpression() {
    for (const auto* expression: this->arguments)
      delete expression;
  }

  bool FunctionExpression::ValidateNumberOfArguments(std::string& errorMessage)const {
    const auto& info = FunctionInfoDictionary.Get(this->type);

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

    return (info.maxArgs == UNLIMITED_ARGS)
      ? this->ValidateUnlimitedArgumentTypes(info, errorMessage)
      : this->ValidateArgumentTypes(info, errorMessage);
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
    if (returnType == DataType::Invalid) {
      errorMessage = "Function: " + info.name +
                        " has an argument at position " + std::to_string(index + 1) +
                        " with invalid type";
      return false;
    }

    if (!DataTypes::Coercions::IsCoercionAllowed(returnType, expectedType, info.allowImplicitCast)) {
      errorMessage = "Function: " + info.name +
                     " expects argument " + std::to_string(index + 1) +
                     " to be of type: " + Constants::ColumnTypesToStringDictionary.Get(expectedType) +
                     ", but got type: " + Constants::ColumnTypesToStringDictionary.Get(returnType);
      return false;
    }

    return true;
  }

  Constants::DataType FunctionExpression::GetReturnType() const{
    return FunctionInfoDictionary.Get(this->type).returnType;
  }

  Value FunctionExpression::Evaluate(const EvaluationContext& context) const {
    std::vector<Value> argVals;
    argVals.reserve(this->arguments.size());

    for (const auto& arg : this->arguments)
      argVals.emplace_back(arg->Evaluate(context));

    return FunctionDictionary.Get(this->type)(argVals);
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
}