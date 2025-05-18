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
    for (const auto& exprCtx : context->expression()) {
      const auto expr = std::any_cast<Expression>(visitExpression(exprCtx));
      where.expressions.push_back(expr);
    }

    return where;
  }

  antlrcpp::Any SQLVisitorImplementation::visitExpression(SQLParser::ExpressionContext *context){
    const std::string raw = context->literalValue()->getText();
    
    return Expression {
      .column = context->columnName()->getText(),
      .operation = context->op->getText(),
      .value = raw.substr(1, raw.length() - 2)
    };
  }

  antlrcpp::Any SQLVisitorImplementation::visitLiteralValue(SQLParser::LiteralValueContext *context){
    return context->STRING()->getText();
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
