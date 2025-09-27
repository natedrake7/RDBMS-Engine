#include "../../AdditionalLibraries/DataTypes/Value/Value.h"
#include "../../AdditionalLibraries/Expressions/Expression.h"
#include "../../AdditionalLibraries/Functions/StringFunctions.h"
#include "../Block/Block.h"
#include "../Row/Row.h"
#include <functional>

namespace Expressions {

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

  Value ColumnExpression::Evaluate(const DatabaseEngine::StorageTypes::Row *row) const{
    const auto& data = row->GetData().at(this->index);

    return Value(data->GetBlockData(), data->GetBlockSize(), data->GetColumnType());
  }

  Value ColumnExpression::Evaluate(const QueryResult &row) const{
    return row.GetData().at(this->index);
  }

  Value LiteralExpression::Evaluate(const DatabaseEngine::StorageTypes::Row *row) const{
    return this->value;
  }

  Value LiteralExpression::Evaluate(const QueryResult &row) const{
    return this->value;
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

  Value FunctionExpression::Concat(const std::vector<Value>& arguments){
    Value value(string(""), 0);

    for (const auto& argument : arguments)
      value += Value(argument.GetString(), 0);

    return value;
  }

  Value FunctionExpression::Length(const std::vector<Value>& arguments){
    const auto& field = arguments.front();

    return Value(AdditionalLibraries::StringFunctions::Length(field.GetString()), 0);
  }

  Value FunctionExpression::TrimLeft(const std::vector<Value>& arguments){
    const auto& field = arguments.front();

    return Value(AdditionalLibraries::StringFunctions::TrimLeft(field.GetString()), 0);
  }

  Value FunctionExpression::TrimRight(const std::vector<Value>& arguments){
    const auto& field = arguments.front();

    return Value(AdditionalLibraries::StringFunctions::TrimRight(field.GetString()), 0);
  }

  Value FunctionExpression::Trim(const std::vector<Value>& arguments){
    const auto& field = arguments.front();

    return Value(AdditionalLibraries::StringFunctions::Trim(field.GetString()), 0);
  }

  Value FunctionExpression::AsciiValue(const std::vector<Value>& arguments){
    const auto& field = arguments.front();

    return Value(AdditionalLibraries::StringFunctions::Ascii(field.GetString()), 0);
  }

  Value FunctionExpression::Char(const std::vector<Value>& arguments){
    const auto& field = arguments.front();

    return Value(AdditionalLibraries::StringFunctions::Char(field.GetInt()), 0);
  }

  Value FunctionExpression::CharIndex(const std::vector<Value>& arguments){
    const auto& subStr = arguments.front().GetString();

    const auto& str = arguments.at(1).GetString();

    const int pos = (arguments.size() > 2)
        ? arguments.at(2).GetInt()
        : 0;

    return Value(AdditionalLibraries::StringFunctions::CharIndex(subStr, str, pos), 0);
  }

  Value FunctionExpression::Lower(const std::vector<Value>& arguments){
    const auto& field = arguments.front();

    return Value(AdditionalLibraries::StringFunctions::Lower(field.GetString()), 0);
  }

  Value FunctionExpression::Upper(const std::vector<Value>& arguments){
    const auto& field = arguments.front();

    return Value(AdditionalLibraries::StringFunctions::Upper(field.GetString()), 0);
  }

  Value FunctionExpression::Replace(const std::vector<Value>& arguments){
    const auto& str = arguments.at(0).GetString();

    const auto& subStr = arguments.at(1).GetString();

    const auto& replaceStr = arguments.at(2).GetString();

    return Value(AdditionalLibraries::StringFunctions::Replace(str, subStr, replaceStr), 0);
  }

  Value FunctionExpression::Substr(const std::vector<Value>& arguments){
    const auto& field = arguments.at(0).GetString();

    const auto& startPos = arguments.at(1).GetInt();

    const auto& endPos = arguments.at(2).GetInt();

    return Value(AdditionalLibraries::StringFunctions::SubString(field, startPos, endPos), 0);
  }

  Value FunctionExpression::Left(const std::vector<Value>& arguments){
    const auto& field = arguments.at(0).GetString();

    const auto& startPos = arguments.at(1).GetInt();

    return Value(AdditionalLibraries::StringFunctions::Left(field, startPos), 0);
  }

  Value FunctionExpression::Right(const std::vector<Value>& arguments){
    const auto& field = arguments.at(0).GetString();

    const auto& startPos = arguments.at(1).GetInt();

    return Value(AdditionalLibraries::StringFunctions::Right(field, startPos), 0);
  }

  Value FunctionExpression::Reverse(const std::vector<Value>& arguments){
    const auto& str = arguments.at(0).GetString();

    return Value(AdditionalLibraries::StringFunctions::Reverse(str), 0);
  }

  Value FunctionExpression::Space(const std::vector<Value>& arguments){
    const auto& size = arguments.at(0).GetInt();

    return Value(AdditionalLibraries::StringFunctions::Space(size), 0);
  }

  Value FunctionExpression::GetDate(const std::vector<Value>& arguments){
    return Value(DataTypes::DateTime::Now(), 0);
  }

  Value FunctionExpression::NewGuid(const std::vector<Value>& arguments){
    return Value(DataTypes::Guid::NewGuid(), 0);
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
}