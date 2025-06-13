#include "Expression.h"
#include "../../HashSet/HashSet.h"

namespace Expressions{
  Expression Expression::Predicate(const std::string& alias, const std::string &column, const ExpressionOperator &operation, const Field &value) {
    return Expression{
      .type = ExpressionType::Predicate,
      .left = nullptr,
      .right  = nullptr,
      .column{
        .name =  column,
        .alias = alias,
      },
      .operation = operation,
      .value = value
    };
  }

  Expression Expression::Predicate(const column_index_t & column, const ExpressionOperator & operation, const Field & value){
   return Expression{
   ExpressionType::Predicate,
   nullptr,
   nullptr,
    {},
       operation,
       value,
      column
  };
  }

  Expression Expression::Logical(const ExpressionType &type, Expression *leftExpression, Expression *RightExpression){
    return Expression{
      type,
      leftExpression,
      RightExpression
    };
  }

  Expression::~Expression(){
    delete left;
    delete right;
  }

  bool Expression::Validate(const Dictionary<string, Headers::ColumnHeader>& columnsDictionary){
    if (this->type != ExpressionType::Predicate
      && this->left != nullptr
      && this->right != nullptr)
       return this->left->Validate(columnsDictionary) &&
              this->right->Validate(columnsDictionary);

    Headers::ColumnHeader header;
    // if (!columnsDictionary.TryGetValue(this->column, header))
    //   return false;

    this->columnIndex = header.ordinalPosition;
    this->value.Validate(header);

    return true;
  }

  bool Expression::IsComplex()const{
    if (this->left != nullptr && this->right != nullptr)
      return this->left->IsComplex() || this->right->IsComplex();

    if (this->type == ExpressionType::Or)
      return true;

    return false;
  }

  void Expression::GetColumns(HashSet<column_index_t>& columnsSet) const{
    if (this->left != nullptr && this->right != nullptr) {
        this->left->GetColumns(columnsSet);
        this->right->GetColumns(columnsSet);
    }

    if (!columnsSet.contains(this->columnIndex))
      columnsSet.Add(this->columnIndex);
  }
}