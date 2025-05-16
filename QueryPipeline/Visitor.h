#pragma once
#include <SQLVisitor.h>
#include <string>
#include <vector>

namespace QueryPipeline {
  typedef struct SelectStatement{
    std::string table;
    std::vector<std::string> columns;
  }SelectStatement;

  typedef struct CreateDbStatement {
    std::string name;
  }CreateDbStatement;

  typedef struct DropDbStatement {
    std::string name;
  }DropDbStatement;

  class SQLVisitorImplementation : public SQLVisitor {
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

      antlrcpp::Any visitColumnName(SQLParser::ColumnNameContext *context) override {
        return context == nullptr ? "" : context->getText();
      }

      antlrcpp::Any visitTableName(SQLParser::TableNameContext *context) override {
        return context == nullptr ? "" : context->getText();
      }

      antlrcpp::Any visitDbName(SQLParser::DbNameContext *context) override {
        return context == nullptr ? "" : context->getText();
      }

      antlrcpp::Any visitColumnList(SQLParser::ColumnListContext *context) override {
        std::vector<std::string> columns;

        for (const auto &columnName : context->columnName())
          columns.push_back(columnName->getText());

        return columns; // Return vector of column names
      }

      antlrcpp::Any visitSqlStatement(SQLParser::SqlStatementContext *context) override {
        if (context->selectStatement())
          return visit(context->selectStatement());
        if (context->createDbStatement())
          return visit(context->createDbStatement());

        return nullptr;
      }

      antlrcpp::Any visitCreateDbStatement(SQLParser::CreateDbStatementContext *context) override {
        CreateDbStatement statement;

        if (context->IDENTIFIER())
          statement.name = context->IDENTIFIER()->getText();

        return statement;

      }

      antlrcpp::Any visitDropDbStatement(SQLParser::DropDbStatementContext *context) override {
          DropDbStatement statement;

          if (context->IDENTIFIER())
            statement.name = context->IDENTIFIER()->getText();

          return statement;

        }
  };
}
