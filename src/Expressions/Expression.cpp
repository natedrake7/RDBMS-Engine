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
  }

  EvaluationContext::EvaluationContext(const EvaluationContextType &type) {
     this->type = type;
     this->row = nullptr;
     this->outerRow = nullptr;
     this->innerRow = nullptr;
   }

  EvaluationContext::EvaluationContext(const DatabaseEngine::StorageTypes::Row *row){
     this->type = EvaluationContextType::SingleRow;
     this->row = row;
     this->outerRow = nullptr;
     this->innerRow = nullptr;
  }

  EvaluationContext::EvaluationContext(const DatabaseEngine::StorageTypes::Row *outerRow, const DatabaseEngine::StorageTypes::Row *innerRow) {
     this->type = EvaluationContextType::Join;
     this->outerRow = outerRow;
     this->innerRow = innerRow;
     this->row = nullptr;
   }

  EvaluationContext::EvaluationContext(const QueryResult &row) {
      this->type = EvaluationContextType::MaterializedRow;
      this->row = nullptr;
      this->outerRow = nullptr;
      this->innerRow = nullptr;
      this->materializedRow = row;
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

  Value LiteralExpression::Evaluate(const EvaluationContext &context) const{
    return this->value;
  }

  DataType LiteralExpression::GetReturnType() const{ return this->value.GetType(); }

  BinaryExpression::BinaryExpression(Expression *left, Expression *right, const ExpressionOperator &operation){
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

//TODO Implement field logical operations.
  Value BinaryExpression::Evaluate(const EvaluationContext& context) const{
    switch (this->operation) {
      case ExpressionOperator::Add:
        return this->left->Evaluate(context) + this->right->Evaluate(context);
      case ExpressionOperator::Subtract:
        return this->left->Evaluate(context) - this->right->Evaluate(context);
      case ExpressionOperator::Multiply:
        return this->left->Evaluate(context) * this->right->Evaluate(context);
      case ExpressionOperator::Divide:
        return this->left->Evaluate(context) / this->right->Evaluate(context);
      case ExpressionOperator::Modulo:
        return this->left->Evaluate(context) % this->right->Evaluate(context);
      case ExpressionOperator::Equal:
        return this->left->Evaluate(context) == this->right->Evaluate(context);
      case ExpressionOperator::EqualIgnoreOrdinalCase:
        return Value::EqualsIgnoreOrdinalCase(this->left->Evaluate(context), this->right->Evaluate(context));
      case ExpressionOperator::NotEqual:
        return this->left->Evaluate(context) != this->right->Evaluate(context);
      case ExpressionOperator::Greater:
        return this->left->Evaluate(context) > this->right->Evaluate(context);
      case ExpressionOperator::GreaterEqual:
        return this->left->Evaluate(context) >= this->right->Evaluate(context);
      case ExpressionOperator::Less:
        return this->left->Evaluate(context) < this->right->Evaluate(context);
      case ExpressionOperator::LessEqual:
        return this->left->Evaluate(context) <= this->right->Evaluate(context);
      default:
          throw std::runtime_error("Unknown operator" + std::to_string(static_cast<int>(this->operation)));
    }
  }

  LogicalExpression::LogicalExpression(
    Expression *leftExpression,
    Expression *RightExpression,
    const ExpressionType &type){
    this->type = type;
    this->left = leftExpression;
    this->right = RightExpression;
  }

  LogicalExpression::LogicalExpression(){
    this->type = ExpressionType::Invalid;
    this->left = nullptr;
    this->right = nullptr;
  }

  LogicalExpression::~LogicalExpression(){
    delete this->left;
    delete this->right;
  }

  DataType LogicalExpression::GetReturnType() const{ return DataType::Bool; }

  Value LogicalExpression::Evaluate(const EvaluationContext& context) const {
    switch (this->type) {
      case ExpressionType::And:{
        const auto leftValue = this->left->Evaluate(context);
        const auto rightValue = this->right->Evaluate(context);

        return Value(leftValue.GetBool() && rightValue.GetBool(), 0);
      }
      case ExpressionType::Or:{
        const auto leftValue = this->left->Evaluate(context);
        const auto rightValue = this->right->Evaluate(context);

        return Value(leftValue.GetBool() || rightValue.GetBool(), 0);
      }
      case ExpressionType::Invalid:
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
    const int& index) {
    if (returnType == DataType::Invalid) {
      errorMessage = "Function: " + info.name +
                        " has an argument at position " + std::to_string(index + 1) +
                        " with invalid type";
      return false;
    }

    if (returnType != expectedType && !info.allowImplicitCast) {
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