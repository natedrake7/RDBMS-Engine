#pragma once
#include <SQLVisitor.h>
#include <string>
#include <vector>

typedef struct SelectStatement{
  std::string table;
  std::vector<std::string> columns;
}SelectStatement;

class SQLVisitorImplementation final : public SQLVisitor {
public:
  antlrcpp::Any visitSelectStatement(SQLParser::SelectStatementContext *ctx) override {
    SelectStatement statement;

    // Visit columnList and get column names
    auto colCtx = ctx->columnList();
    for (const auto col : colCtx->columnName()) 
      statement.columns.push_back(col->getText());

    const auto tableName = ctx->tableName();
      
    statement.table = tableName == nullptr ?  "" : tableName->getText();

    return statement;
  }

  antlrcpp::Any visitColumnName(SQLParser::ColumnNameContext *ctx) override {
    return ctx == nullptr ? "" : ctx->getText();
  }

  antlrcpp::Any visitTableName(SQLParser::TableNameContext *ctx) override {
    return ctx == nullptr ? "" : ctx->getText();
  }

  antlrcpp::Any visitColumnList(SQLParser::ColumnListContext *context) override {
    std::vector<std::string> columns;
      
    for (const auto &columnName : context->columnName())
      columns.push_back(columnName->getText());

    return columns; // Return vector of column names
  }

  antlrcpp::Any visitSqlStatement(SQLParser::SqlStatementContext *context) override {
    if (context->selectStatement()) {
      return visit(context->selectStatement());
    }

    return nullptr;
  }
};