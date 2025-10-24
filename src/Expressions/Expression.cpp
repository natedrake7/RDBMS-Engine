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

  Value ColumnExpression::Evaluate(const DatabaseEngine::StorageTypes::Row *row) const{
    return row->GetColumnByIndex(this->index);
  }

  Value ColumnExpression::Evaluate(const QueryResult &row) const{
    return row.GetData().at(this->index);
  }

  LiteralExpression::LiteralExpression(const Value &value){
    this->value = value;
  }

  Value ColumnExpression::Evaluate(const DatabaseEngine::StorageTypes::Row *outerRow, const DatabaseEngine::StorageTypes::Row *innerRow) const{
    const auto& outerRowData = outerRow->GetData();

    //figure out index assignment
    const auto& data = this->index < outerRowData.size()
        ? outerRow->GetColumnByIndex(this->index)
        : innerRow->GetColumnByIndex(this->index - outerRowData.size());

    return data;
  }

  DataType LiteralExpression::GetReturnType() const{ return this->value.GetType(); }

  Value LiteralExpression::Evaluate(
    const DatabaseEngine::StorageTypes::Row *outerRow,
    const DatabaseEngine::StorageTypes::Row *innerRow
  ) const{ return this->value; }

  Value LiteralExpression::Evaluate(const DatabaseEngine::StorageTypes::Row *row) const{
    return this->value;
  }

  Value LiteralExpression::Evaluate(const QueryResult &row) const{
    return this->value;
  }

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
  Value BinaryExpression::Evaluate(const DatabaseEngine::StorageTypes::Row *row) const{
    switch (this->operation) {
      case ExpressionOperator::Add:
        return this->left->Evaluate(row) + this->right->Evaluate(row);
      case ExpressionOperator::Subtract:
        return this->left->Evaluate(row) - this->right->Evaluate(row);
      case ExpressionOperator::Multiply:
        return this->left->Evaluate(row) * this->right->Evaluate(row);
      case ExpressionOperator::Divide:
        return this->left->Evaluate(row) / this->right->Evaluate(row);
      case ExpressionOperator::Modulo:
        return this->left->Evaluate(row) % this->right->Evaluate(row);
      case ExpressionOperator::Equal:
        return this->left->Evaluate(row) == this->right->Evaluate(row);
      case ExpressionOperator::EqualIgnoreOrdinalCase:
        return Value::EqualsIgnoreOrdinalCase(this->left->Evaluate(row), this->right->Evaluate(row));
      case ExpressionOperator::NotEqual:
        return this->left->Evaluate(row) != this->right->Evaluate(row);
      case ExpressionOperator::Greater:
        return this->left->Evaluate(row) > this->right->Evaluate(row);
      case ExpressionOperator::GreaterEqual:
        return this->left->Evaluate(row) >= this->right->Evaluate(row);
      case ExpressionOperator::Less:
        return this->left->Evaluate(row) < this->right->Evaluate(row);
      case ExpressionOperator::LessEqual:
        return this->left->Evaluate(row) <= this->right->Evaluate(row);
      default:
          throw std::runtime_error("Unknown operator" + std::to_string(static_cast<int>(this->operation)));
    }
  }

  Value BinaryExpression::Evaluate(const QueryResult &row) const{
    switch (this->operation) {
    case ExpressionOperator::Add:
      return this->left->Evaluate(row) + this->right->Evaluate(row);
    case ExpressionOperator::Subtract:
      return this->left->Evaluate(row) - this->right->Evaluate(row);
    case ExpressionOperator::Multiply:
      return this->left->Evaluate(row) * this->right->Evaluate(row);
    case ExpressionOperator::Divide:
      return this->left->Evaluate(row) / this->right->Evaluate(row);
    case ExpressionOperator::Modulo:
      return this->left->Evaluate(row) % this->right->Evaluate(row);
    case ExpressionOperator::Equal:
      return this->left->Evaluate(row) == this->right->Evaluate(row);
    case ExpressionOperator::NotEqual:
      return this->left->Evaluate(row) != this->right->Evaluate(row);
    case ExpressionOperator::Greater:
      return this->left->Evaluate(row) > this->right->Evaluate(row);
    case ExpressionOperator::GreaterEqual:
      return this->left->Evaluate(row) >= this->right->Evaluate(row);
    case ExpressionOperator::Less:
      return this->left->Evaluate(row) < this->right->Evaluate(row);
    case ExpressionOperator::LessEqual:
      return this->left->Evaluate(row) <= this->right->Evaluate(row);
    default:
      throw std::runtime_error("Unknown operator" + std::to_string(static_cast<int>(this->operation)));
    }
  }

  Value BinaryExpression::Evaluate(
    const DatabaseEngine::StorageTypes::Row *outerRow,
    const DatabaseEngine::StorageTypes::Row *innerRow
  ) const{
    switch (this->operation) {
      case ExpressionOperator::Add:
        return this->left->Evaluate(outerRow, innerRow) + this->right->Evaluate(outerRow, innerRow);
      case ExpressionOperator::Subtract:
        return this->left->Evaluate(outerRow, innerRow) - this->right->Evaluate(outerRow, innerRow);
      case ExpressionOperator::Multiply:
        return this->left->Evaluate(outerRow, innerRow) * this->right->Evaluate(outerRow, innerRow);
      case ExpressionOperator::Divide:
        return this->left->Evaluate(outerRow, innerRow) / this->right->Evaluate(outerRow, innerRow);
      case ExpressionOperator::Modulo:
        return this->left->Evaluate(outerRow, innerRow) % this->right->Evaluate(outerRow, innerRow);
      case ExpressionOperator::Equal:
        return this->left->Evaluate(outerRow, innerRow) == this->right->Evaluate(outerRow, innerRow);
      case ExpressionOperator::NotEqual:
        return this->left->Evaluate(outerRow, innerRow) != this->right->Evaluate(outerRow, innerRow);
      case ExpressionOperator::Greater:
        return this->left->Evaluate(outerRow, innerRow) > this->right->Evaluate(outerRow, innerRow);
      case ExpressionOperator::GreaterEqual:
        return this->left->Evaluate(outerRow, innerRow) >= this->right->Evaluate(outerRow, innerRow);
      case ExpressionOperator::Less:
        return this->left->Evaluate(outerRow, innerRow) < this->right->Evaluate(outerRow, innerRow);
      case ExpressionOperator::LessEqual:
        return this->left->Evaluate(outerRow, innerRow) <= this->right->Evaluate(outerRow, innerRow);
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

  Value LogicalExpression::Evaluate(const DatabaseEngine::StorageTypes::Row *row) const {
    switch (this->type) {
      case ExpressionType::And:{
        const auto leftValue = this->left->Evaluate(row);
        const auto rightValue = this->right->Evaluate(row);

        return Value(leftValue.GetBool() && rightValue.GetBool(), 0);
      }
      case ExpressionType::Or:{
        const auto leftValue = this->left->Evaluate(row);
        const auto rightValue = this->right->Evaluate(row);

        return Value(leftValue.GetBool() || rightValue.GetBool(), 0);
      }
      case ExpressionType::Invalid:
      default:
      throw std::runtime_error("Unknown predicate" + std::to_string(static_cast<int>(this->type)));
    }
  }

  Value LogicalExpression::Evaluate(const QueryResult &row) const{
    switch (this->type) {
      case ExpressionType::And:{
        const auto leftValue = this->left->Evaluate(row);
        const auto rightValue = this->right->Evaluate(row);

        return Value(leftValue.GetBool() && rightValue.GetBool(), 0);
      }
      case ExpressionType::Or:{
        const auto leftValue = this->left->Evaluate(row);
        const auto rightValue = this->right->Evaluate(row);

        return Value(leftValue.GetBool() || rightValue.GetBool(), 0);
      }
      case ExpressionType::Invalid:
      default:
        throw std::runtime_error("Unknown predicate" + std::to_string(static_cast<int>(this->type)));
    }
  }

  Value LogicalExpression::Evaluate(const DatabaseEngine::StorageTypes::Row *outerRow, const DatabaseEngine::StorageTypes::Row *innerRow) const{
    switch (this->type) {
      case ExpressionType::And:{
        const auto leftValue = this->left->Evaluate(outerRow, innerRow);
        const auto rightValue = this->right->Evaluate(outerRow, innerRow);

        return Value(leftValue.GetBool() && rightValue.GetBool(), 0);
      }
      case ExpressionType::Or:{
        const auto leftValue = this->left->Evaluate(outerRow, innerRow);
        const auto rightValue = this->right->Evaluate(outerRow, innerRow);

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

  Value FunctionExpression::Evaluate(const DatabaseEngine::StorageTypes::Row *row) const {
    std::vector<Value> argVals;
    argVals.reserve(this->arguments.size());

    for (const auto& arg : this->arguments)
      argVals.emplace_back(arg->Evaluate(row));

    return FunctionDictionary.Get(this->type)(argVals);
  }

  Value FunctionExpression::Evaluate(const QueryResult &row) const{
    std::vector<Value> argVals;
    argVals.reserve(this->arguments.size());

    for (const auto& arg : this->arguments)
      argVals.emplace_back(arg->Evaluate(row));

    return FunctionDictionary.Get(this->type)(argVals);
  }

  Value FunctionExpression::Evaluate(const DatabaseEngine::StorageTypes::Row *outerRow, const DatabaseEngine::StorageTypes::Row *innerRow) const{
    std::vector<Value> argVals;
    argVals.reserve(this->arguments.size());

    for (const auto& arg : this->arguments)
      argVals.emplace_back(arg->Evaluate(outerRow, innerRow));

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