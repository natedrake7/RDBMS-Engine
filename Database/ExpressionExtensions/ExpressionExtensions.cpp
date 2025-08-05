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

}