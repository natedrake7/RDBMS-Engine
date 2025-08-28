#include "Expression.h"
#include "../../HashSet/HashSet.h"

namespace Expressions{

ColumnExpression::ColumnExpression(const std::string &name, const std::string &alias){
  this->name = name;
  this->alias = alias;

  this->tableId = -1;
  this->columnId = -1;
  this->columnIndex = 0;
}

ColumnExpression::ColumnExpression(const column_index_t &index){
  this->columnIndex = index;
}

LiteralExpression::LiteralExpression(const Field &value){
  this->value = value;
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

FunctionExpression::FunctionExpression(const Constants::FunctionType& type, std::vector<Expression*>& arguments) {
  this->type = type;
  this->arguments = std::move(arguments);
}

FunctionExpression::~FunctionExpression() {
  for (const auto* expression: this->arguments)
    delete expression;
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

  bool LogicalExpression::Validate(const Dictionary<string, Headers::ColumnHeader>& columnsDictionary){
    // if (this->type != ExpressionType::Predicate
    //   && this->left != nullptr
    //   && this->right != nullptr)
    //    return this->left->Validate(columnsDictionary) &&
    //           this->right->Validate(columnsDictionary);
    //
    // Headers::ColumnHeader header;
    // if (!columnsDictionary.TryGetValue(this->column.name, header))
    //   return false;
    //
    // this->columnIndex = header.ordinalPosition;
    //
    // if (this->value.GetIsNull()
    //   && ( this->operation != ExpressionOperator::Equal
    //       || this->operation != ExpressionOperator::NotEqual))
    //   return false;
    //
    // this->value.Validate(header);

    return true;
  }

  bool LogicalExpression::IsComplex()const{
    // if (this->left != nullptr && this->right != nullptr)
    //   return this->left->IsComplex() || this->right->IsComplex();

    if (this->type == ExpressionType::Or)
      return true;

    return false;
  }

  void LogicalExpression::GetColumns(HashSet<column_index_t>& columnsSet) const{
    if (this->left != nullptr && this->right != nullptr) {
        // this->left->GetColumns(columnsSet);
        // this->right->GetColumns(columnsSet);
    }

    // if (!columnsSet.contains(this->columnIndex))
      // columnsSet.Add(this->columnIndex);
  }
}