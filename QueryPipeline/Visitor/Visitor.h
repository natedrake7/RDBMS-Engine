#pragma once
#include "../../Database/Constants.h"


#include <SQLVisitor.h>
#include <string>
#include <vector>

namespace QueryPipeline {

  enum class ExpressionType {
    And = 0,
    Or = 1,
    Predicate = 2
  };

  typedef struct Expression {
    ExpressionType type;

    Expression* left;
    Expression* right;
      
    std::string column;
    std::string operation;
    std::string value;

    Constants::column_index_t columnIndex;
    
    static Expression Predicate(
      const std::string& column,
      const std::string& operation,
      const std::string& value) {
      return Expression{
        ExpressionType::Predicate,
        nullptr,
        nullptr,
        column,
        operation,
        value
      };
    }

    static Expression Logical(const ExpressionType& type, Expression* leftExpression, Expression* RightExpression) {
      return Expression{
        type,
        leftExpression,
        RightExpression
      };
    }

    ~Expression() {
      delete left;
      delete right;
    }
    
  }Expression;

  typedef struct WhereClause{
    Expression* expression;

    WhereClause() { this->expression = nullptr; }
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
    
      antlrcpp::Any visitOrExpression(SQLParser::OrExpressionContext *context) override;
    
      antlrcpp::Any visitAndExpression(SQLParser::AndExpressionContext *context) override;
    
      antlrcpp::Any visitPredicate(SQLParser::PredicateContext *context) override;
  };
}
