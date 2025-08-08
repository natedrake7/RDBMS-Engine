#include "../../AdditionalLibraries/AdditionalDataTypes/Field/Field.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Expression/Expression.h"
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

}