#include "Expression.h"
#include "../HashSet/HashSet.h"

namespace Expressions{

  ColumnExpression::ColumnExpression(const std::string &name, const std::string &tableAlias){
    this->name = name;
    this->tableAlias = tableAlias;

    this->tableId = -1;
    this->columnId = -1;
    this->columnIndex = 0;
    this->returnType = DataType::Invalid;
  }

  ColumnExpression::ColumnExpression(const column_index_t &index){
    this->columnIndex = index;
    this->returnType = DataType::Invalid;
  }

  DataType ColumnExpression::GetReturnType() const{ return returnType; }

  LiteralExpression::LiteralExpression(const Value &value){
    this->value = value;
  }

  DataType LiteralExpression::GetReturnType() const{ return this->value.GetType(); }

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

    //validate types and validate return type?

    for (int i = 0;i < this->arguments.size(); i++) {
      const auto* argument = this->arguments[i];

      const auto& returnType = argument->GetReturnType();
      const auto& expectedType = info.maxArgs == UNLIMITED_ARGS
            ? info.expectedTypes.front()
            : info.expectedTypes[i];

      if (returnType == DataType::Invalid) {
        errorMessage = "Function: " + info.name +
                          " has an argument at position " + std::to_string(i + 1) +
                          " with invalid type";
        return false;
      }

      if (returnType != expectedType) {
        errorMessage = "Function: " + info.name +
                       " expects argument " + std::to_string(i + 1) +
                       " to be of type: " + Constants::ColumnTypesToStringDictionary.Get(expectedType) +
                       ", but got type: " + Constants::ColumnTypesToStringDictionary.Get(returnType);
        return false;
      }
    }

    return true;
  }

  Constants::DataType FunctionExpression::GetReturnType() const{
    return FunctionInfoDictionary.Get(this->type).returnType;
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
}