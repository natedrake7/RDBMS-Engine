#include "Visitor.h"
#include "../../AdditionalLibraries/SafeConverter/SafeConverter.h"

namespace QueryPipeline {

  Expression Expression::Predicate(const std::string &column, const std::string &operation, const std::string &value) {
    return Expression{
      ExpressionType::Predicate,
      nullptr,
      nullptr,
      column,
      operation,
      value
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

  string ParseString(const string &str){
    return std::string(str).substr(1, str.size() - 2);
  }

  antlrcpp::Any SQLVisitorImplementation::visitSqlStatement(SQLParser::SqlStatementContext *context)  {
    if (context->selectStatement())
      return visit(context->selectStatement());
    if (context->createDbStatement())
      return visit(context->createDbStatement());
    if (context->dropDbStatement())
      return visit(context->dropDbStatement());
    if (context->insertStatement())
      return visit(context->insertStatement());
    
    return nullptr;
  }

  antlrcpp::Any SQLVisitorImplementation::visitCreateDbStatement(SQLParser::CreateDbStatementContext *context) {
    CreateDbStatement statement;

    if (context->IDENTIFIER())
      statement.name = context->IDENTIFIER()->getText();

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitDropDbStatement(SQLParser::DropDbStatementContext *context) {
    DropDbStatement statement;

    if (context->IDENTIFIER())
      statement.name = context->IDENTIFIER()->getText();

    return statement;
  }



antlrcpp::Any SQLVisitorImplementation::visitSelectStatement(SQLParser::SelectStatementContext *ctx) {
    SelectStatement statement;

    // Visit columnList and get column names
    auto colCtx = ctx->columnList();
    for (const auto col : colCtx->columnName())
      statement.columns.push_back(col->getText());

    const auto tableName = ctx->tableName();

    statement.table = tableName == nullptr ?  "" : tableName->getText();

    if (const auto& whereClause = ctx->whereClause();whereClause != nullptr)
      statement.where = std::any_cast<WhereClause>(visit(whereClause));

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitWhereClause(SQLParser::WhereClauseContext *context){
    WhereClause where;
    where.expression = std::any_cast<Expression*>(visitExpression(context->expression()));
    return where;
  }

  antlrcpp::Any SQLVisitorImplementation::visitExpression(SQLParser::ExpressionContext *context){
    return visit(context->orExpression());
  }

  antlrcpp::Any SQLVisitorImplementation::visitLiteralValue(SQLParser::LiteralValueContext *context){
    return ParseString(context->STRING()->getText());
  }

  antlrcpp::Any SQLVisitorImplementation::visitOrExpression(SQLParser::OrExpressionContext *context){
    auto* expression = std::any_cast<Expression*>(visit(context->andExpression(0)));

    for (size_t i = 1; i < context->andExpression().size(); i++) {
      auto* right = std::any_cast<Expression*>(visit(context->andExpression(i)));
      
      expression = new Expression(ExpressionType::Or, expression, right);  // assuming you have a class like this
    }

    return expression;
  }

antlrcpp::Any SQLVisitorImplementation::visitAndExpression(SQLParser::AndExpressionContext *context) {
    auto* expression = std::any_cast<Expression*>(visit(context->predicate(0)));

    for (size_t i = 1; i < context->predicate().size(); i++) {
      auto* right = std::any_cast<Expression*>(visit(context->predicate(i)));
      
      expression = new Expression(ExpressionType::And, expression, right);  // assuming you have a class like this
    }

    return expression;
  }
  antlrcpp::Any SQLVisitorImplementation::visitPredicate(SQLParser::PredicateContext *context){
    if (context->expression())
      return visit(context->expression());
    
    return new Expression{
      ExpressionType::Predicate,
      nullptr,
      nullptr,
      context->columnName()->getText(),
      context->op->getText(),
      context->literalValue()->getText()
    };
  }

  antlrcpp::Any SQLVisitorImplementation::visitInsertStatement(SQLParser::InsertStatementContext *context){
    InsertStatement statement;

    statement.tableName = context->tableName()->getText();
    
    const auto columns = visit(context->columnList());

    statement.columns = std::any_cast<std::vector<string>>(columns);

    const auto values = visit(context->literalValueList());

    statement.values = std::any_cast<std::vector<Field>>(values);

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitLiteralValueList(SQLParser::LiteralValueListContext *context){
    vector<Field> values;
    
    for (const auto& literalValue : context->literalValue()) {
      if (literalValue->STRING()) {

        values.emplace_back();
        values.back().SetData(literalValue->STRING()->getText());
        
        continue;
      }

      const auto numberNode = literalValue->NUMBER();

      const auto number = SafeConverter<int64_t>::SafeStoi(numberNode->getText());

      values.emplace_back();
      values.back().SetData(number);
    }

    return values;
  }

  antlrcpp::Any SQLVisitorImplementation::visitColumnName(SQLParser::ColumnNameContext *context){
    return context == nullptr ? "" : context->getText();
  }

  antlrcpp::Any SQLVisitorImplementation::visitTableName(SQLParser::TableNameContext *context) {
    return context == nullptr ? "" : context->getText();
  }

  antlrcpp::Any SQLVisitorImplementation::visitDbName(SQLParser::DbNameContext *context){
      return context == nullptr ? "" : context->getText();
  }

  antlrcpp::Any SQLVisitorImplementation::visitColumnList(SQLParser::ColumnListContext *context){
      std::vector<std::string> columns;

      for (const auto &columnName : context->columnName())
        columns.push_back(columnName->getText());

      return columns; // Return vector of column names
  }
}
