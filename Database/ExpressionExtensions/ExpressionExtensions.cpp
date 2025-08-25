#include "../../AdditionalLibraries/AdditionalDataTypes/Field/Field.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Expression/Expression.h"
#include "../../AdditionalLibraries/StringFunctions/StringFunctions.h"
#include "../Block/Block.h"
#include "../Row/Row.h"

namespace Expressions {
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
        return this->left->Evaluate(row) >= this->right->Evaluate(row);
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
    return Field(nullptr, 0);
  }

  Field FunctionExpression::Concat(const DatabaseEngine::StorageTypes::Row* row)const{
    Field value(string(""), 0);

    for (const auto* expression : this->arguments)
      value += expression->Evaluate(row);

    return value;
  }

  Field FunctionExpression::Length(const DatabaseEngine::StorageTypes::Row *row) const{
    const auto& field = this->arguments.front()->Evaluate(row);

    return Field(AdditionalLibraries::StringFunctions::Length(field.GetString()), 0);
  }

  Field FunctionExpression::TrimLeft(const DatabaseEngine::StorageTypes::Row *row) const{
    const auto& field = this->arguments.front()->Evaluate(row);

    return Field(AdditionalLibraries::StringFunctions::TrimLeft(field.GetString()), 0);
  }

  Field FunctionExpression::TrimRight(const DatabaseEngine::StorageTypes::Row *row) const{
    const auto& field = this->arguments.front()->Evaluate(row);

    return Field(AdditionalLibraries::StringFunctions::TrimRight(field.GetString()), 0);
  }

  Field FunctionExpression::Evaluate(const DatabaseEngine::StorageTypes::Row *row) const {
    switch (this->type) {
      case FunctionType::GetDate:
          return Field(DataTypes::DateTime::Now(), 0);
      case FunctionType::NewGuid:
          return Field(DataTypes::Guid::NewGuid(), 0);
      case FunctionType::Concat:
        return this->Concat(row);
      case FunctionType::Length:
        return this->Length(row);
      case FunctionType::AsciiValue:
        break;
      case FunctionType::Char:
        break;
      case FunctionType::CharIndex:
        break;
      case FunctionType::Lower:
        break;
      case FunctionType::Upper:
        break;
      case FunctionType::Trim:
        break;
      case FunctionType::TrimLeft:
        return this->TrimLeft(row);
      case FunctionType::TrimRight:
        return this->TrimRight(row);
      case FunctionType::Replace:
        break;
      case FunctionType::Substr:
        break;
      case FunctionType::Left:
        break;
      case FunctionType::Right:
        break;
      default:
        throw std::runtime_error("Unknown function type");
    }

    return Field(nullptr, 0);
  }

}