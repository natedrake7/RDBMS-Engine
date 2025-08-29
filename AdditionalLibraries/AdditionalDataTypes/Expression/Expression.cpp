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
}