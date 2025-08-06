#include "../../AdditionalLibraries/AdditionalDataTypes/Field/Field.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Expression/Expression.h"
#include "../Block/Block.h"
#include "../Row/Row.h"

namespace Expressions {

  Field ColumnExpression::Evaluate(const DatabaseEngine::StorageTypes::Row &row) const{
    const auto& data = row.GetData().at(this->columnIndex);

    return Field(data->GetBlockData(), data->GetBlockSize(), data->GetColumnType());
  }

  Field LiteralExpression::Evaluate(const DatabaseEngine::StorageTypes::Row &row) const{
    return this->value;
  }

  Field BinaryExpression::Evaluate(const DatabaseEngine::StorageTypes::Row &row) const{
    switch (this->operation) {
      case BinaryExpressionOperator::Add:
        return this->left->Evaluate(row) + this->right->Evaluate(row);
      case BinaryExpressionOperator::Subtract:
        return this->left->Evaluate(row) - this->right->Evaluate(row);
      case BinaryExpressionOperator::Multiply:
        return this->left->Evaluate(row) * this->right->Evaluate(row);
      case BinaryExpressionOperator::Divide:
        return this->left->Evaluate(row) / this->right->Evaluate(row);
      case BinaryExpressionOperator::Modulo:
        return this->left->Evaluate(row) % this->right->Evaluate(row);
      default:
          throw std::runtime_error("Unknown operator" + std::to_string(static_cast<int>(this->operation)));
    }
  }

}