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

  LiteralExpression::LiteralExpression(const Value &value){
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
}