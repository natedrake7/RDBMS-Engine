#include "../../AdditionalLibraries/AdditionalDataTypes/Field/Field.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Expression/Expression.h"
#include "../../AdditionalLibraries/Functions/StringFunctions.h"
#include "../Block/Block.h"
#include "../Row/Row.h"
#include <functional>

namespace Expressions {

  static Dictionary<Constants::FunctionType, std::function<Field(const Expressions::FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row*)>> FunctionDictionary{
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


  Field ColumnExpression::Evaluate(const DatabaseEngine::StorageTypes::Row *row) const{
    const auto& data = row->GetData().at(this->columnIndex);

    return Field(data->GetBlockData(), data->GetBlockSize(), data->GetColumnType());
  }

  Field LiteralExpression::Evaluate(const DatabaseEngine::StorageTypes::Row *row) const{
    return this->value;
  }

//TODO Implement field logical operations.
  Field BinaryExpression::Evaluate(const DatabaseEngine::StorageTypes::Row *row) const{
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

  Field LogicalExpression::Evaluate(const DatabaseEngine::StorageTypes::Row *row) const {
    switch (this->type) {
      case ExpressionType::And:{
        const auto left = this->left->Evaluate(row);
        const auto right = this->right->Evaluate(row);

        return Field(left.GetBool() && right.GetBool(), 0);
      }
      case ExpressionType::Or:{
        const auto left = this->left->Evaluate(row);
        const auto right = this->right->Evaluate(row);

        return Field(left.GetBool() || right.GetBool(), 0);
      }
      case ExpressionType::Invalid:
      default:
      throw std::runtime_error("Unknown predicate" + std::to_string(static_cast<int>(this->type)));
    }
  }

  Field FunctionExpression::Concat(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row){
    Field value(string(""), 0);

    for (const auto* argument : expression->arguments)
      value += argument->Evaluate(row);

    return value;
  }

  Field FunctionExpression::Length(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row *row){
    const auto& field = expression->arguments.front()->Evaluate(row);

    return Field(AdditionalLibraries::StringFunctions::Length(field.GetString()), 0);
  }

  Field FunctionExpression::TrimLeft(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row *row){
    const auto& field = expression->arguments.front()->Evaluate(row);

    return Field(AdditionalLibraries::StringFunctions::TrimLeft(field.GetString()), 0);
  }

  Field FunctionExpression::TrimRight(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row *row){
    const auto& field = expression->arguments.front()->Evaluate(row);

    return Field(AdditionalLibraries::StringFunctions::TrimRight(field.GetString()), 0);
  }

  Field FunctionExpression::Trim(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row *row){
    const auto& field = expression->arguments.front()->Evaluate(row);

    return Field(AdditionalLibraries::StringFunctions::Trim(field.GetString()), 0);
  }

  Field FunctionExpression::AsciiValue(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row *row){
    const auto& field = expression->arguments.front()->Evaluate(row);

    return Field(AdditionalLibraries::StringFunctions::Ascii(field.GetString()), 0);
  }

  Field FunctionExpression::Char(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row *row){
    const auto& field = expression->arguments.front()->Evaluate(row);

    return Field(AdditionalLibraries::StringFunctions::Char(field.GetInt()), 0);
  }

  Field FunctionExpression::CharIndex(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row *row){
    const auto& subStr = expression->arguments.front()->Evaluate(row).GetString();

    const auto& str = expression->arguments[1]->Evaluate(row).GetString();

    const int pos = (expression->arguments.size() > 2)
        ? expression->arguments[2]->Evaluate(row).GetInt()
        : 0;

    return Field(AdditionalLibraries::StringFunctions::CharIndex(subStr, str, pos), 0);
  }

  Field FunctionExpression::Lower(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row *row){
    const auto& field = expression->arguments.front()->Evaluate(row);

    return Field(AdditionalLibraries::StringFunctions::Lower(field.GetString()), 0);
  }

  Field FunctionExpression::Upper(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row *row){
    const auto& field = expression->arguments.front()->Evaluate(row);

    return Field(AdditionalLibraries::StringFunctions::Upper(field.GetString()), 0);
  }

  Field FunctionExpression::Replace(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row *row){
    const auto& str = expression->arguments.front()->Evaluate(row).GetString();

    const auto& subStr = expression->arguments[1]->Evaluate(row).GetString();

    const auto& replaceStr = expression->arguments[2]->Evaluate(row).GetString();

    return Field(AdditionalLibraries::StringFunctions::Replace(str, subStr, replaceStr), 0);
  }

  Field FunctionExpression::Substr(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row *row){
    const auto& field = expression->arguments.front()->Evaluate(row).GetString();

    const auto& startPos = expression->arguments[1]->Evaluate(row).GetInt();

    const auto& endPos = expression->arguments[2]->Evaluate(row).GetInt();

    return Field(AdditionalLibraries::StringFunctions::SubString(field, startPos, endPos), 0);
  }

  Field FunctionExpression::Left(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row *row){
    const auto& field = expression->arguments.front()->Evaluate(row).GetString();

    const auto& startPos = expression->arguments[1]->Evaluate(row).GetInt();

    return Field(AdditionalLibraries::StringFunctions::Left(field, startPos), 0);
  }

  Field FunctionExpression::Right(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row *row){
    const auto& field = expression->arguments.front()->Evaluate(row).GetString();

    const auto& startPos = expression->arguments[1]->Evaluate(row).GetInt();

    return Field(AdditionalLibraries::StringFunctions::Right(field, startPos), 0);
  }

  Field FunctionExpression::Reverse(const FunctionExpression *expression, const DatabaseEngine::StorageTypes::Row *row){
    const auto& str = expression->arguments.front()->Evaluate(row).GetString();

    return Field(AdditionalLibraries::StringFunctions::Reverse(str), 0);
  }

  Field FunctionExpression::Space(const FunctionExpression *expression, const DatabaseEngine::StorageTypes::Row *row){
    const auto& size = expression->arguments.front()->Evaluate(row).GetInt();

    return Field(AdditionalLibraries::StringFunctions::Space(size), 0);
  }

  Field FunctionExpression::GetDate(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row *row){
    return Field(DataTypes::DateTime::Now(), 0);
  }

  Field FunctionExpression::NewGuid(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row *row){
    return Field(DataTypes::Guid::NewGuid(), 0);
  }

  Field FunctionExpression::Evaluate(const DatabaseEngine::StorageTypes::Row *row) const {
    return FunctionDictionary.Get(this->type)(this, row);
  }

}