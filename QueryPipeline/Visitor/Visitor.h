#pragma once
#include <SQLVisitor.h>
#include <string>
#include <vector>

namespace QueryPipeline {

  typedef struct Expression {
    std::string column;
    std::string operation;
    std::string value;
  }Expression;

  typedef struct WhereClause{
    std::vector<Expression> expressions;
  }WhereClause;

  typedef struct SelectStatement{
    std::string table;
    std::vector<std::string> columns;
    WhereClause where;
  }SelectStatement;

  typedef struct CreateDbStatement {
    std::string name;
  }CreateDbStatement;

  typedef struct DropDbStatement {
    std::string name;
  }DropDbStatement;

  class SQLVisitorImplementation final : public SQLVisitor {
    public:
      antlrcpp::Any visitSelectStatement(SQLParser::SelectStatementContext *ctx) override;

      antlrcpp::Any visitColumnName(SQLParser::ColumnNameContext *context) override;

      antlrcpp::Any visitTableName(SQLParser::TableNameContext *context) override;

      antlrcpp::Any visitDbName(SQLParser::DbNameContext *context) override;

      antlrcpp::Any visitColumnList(SQLParser::ColumnListContext *context) override;

      antlrcpp::Any visitSqlStatement(SQLParser::SqlStatementContext *context) override;

      antlrcpp::Any visitCreateDbStatement(SQLParser::CreateDbStatementContext *context) override;

      antlrcpp::Any visitDropDbStatement(SQLParser::DropDbStatementContext *context) override;

      antlrcpp::Any visitWhereClause(SQLParser::WhereClauseContext *context) override;
  
      antlrcpp::Any visitExpression(SQLParser::ExpressionContext *context) override;
  
      antlrcpp::Any visitLiteralValue(SQLParser::LiteralValueContext *context) override;
  };
}
