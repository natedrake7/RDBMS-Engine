#include "Visitor.h"

namespace QueryPipeline {
  antlrcpp::Any SQLVisitorImplementation::visitSqlStatement(SQLParser::SqlStatementContext *context)  {
    if (context->selectStatement())
      return visit(context->selectStatement());
    if (context->createDbStatement())
      return visit(context->createDbStatement());
    if (context->dropDbStatement())
      return visit(context->dropDbStatement());
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
    return context->STRING()->getText();
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
    
    const std::string value = context->literalValue()->getText();

    return new Expression{
      ExpressionType::Predicate,
      nullptr,
      nullptr,
      context->columnName()->getText(),
      context->op->getText(),
      value.substr(1, value.size() - 2)
  };
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
