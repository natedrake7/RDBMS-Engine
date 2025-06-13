#pragma once
#include "../Statements/Statements.h"


#include <SQLVisitor.h>

namespace QueryPipeline {
  static std::string String = "string";
  static std::string UnicodeString = "unicodestring";

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

      antlrcpp::Any visitInsertStatement(SQLParser::InsertStatementContext *context) override;

      antlrcpp::Any visitLiteralValueList(SQLParser::LiteralValueListContext *context) override;

      antlrcpp::Any visitGetDate(SQLParser::GetDateContext *context) override;

      antlrcpp::Any visitCreateTableStatement(SQLParser::CreateTableStatementContext *context) override;

      antlrcpp::Any visitDataType(SQLParser::DataTypeContext *context) override;

      antlrcpp::Any visitPrimaryKey(SQLParser::PrimaryKeyContext *context) override;

      antlrcpp::Any visitAddColumn(SQLParser::AddColumnContext *context) override;

      antlrcpp::Any visitVarcharType(SQLParser::VarcharTypeContext *context) override;

      antlrcpp::Any visitNvarcharType(SQLParser::NvarcharTypeContext *context) override;

      antlrcpp::Any visitDecimalType(SQLParser::DecimalTypeContext *context) override;

      antlrcpp::Any visitPrimaryKeyConstraint(SQLParser::PrimaryKeyConstraintContext *context) override;

      antlrcpp::Any visitCreateSchemaStatement(SQLParser::CreateSchemaStatementContext *context) override;

      antlrcpp::Any visitDeleteStatement(SQLParser::DeleteStatementContext *context) override;

      antlrcpp::Any visitUpdateStatement(SQLParser::UpdateStatementContext *context) override;

      antlrcpp::Any visitUpdateColumnsList(SQLParser::UpdateColumnsListContext *context) override;

      antlrcpp::Any visitUpdateColumn(SQLParser::UpdateColumnContext *context) override;

      antlrcpp::Any visitAutoIncrementKey(SQLParser::AutoIncrementKeyContext *context) override;

      antlrcpp::Any visitOrderByStatement(SQLParser::OrderByStatementContext *context) override;

      antlrcpp::Any visitNewGuid(SQLParser::NewGuidContext *context) override;

      antlrcpp::Any visitCreateIndexStatement(SQLParser::CreateIndexStatementContext *context) override;

      antlrcpp::Any visitJoinStatement(SQLParser::JoinStatementContext *context) override;

      antlrcpp::Any visitJoinType(SQLParser::JoinTypeContext *context) override;

      antlrcpp::Any visitAlias(SQLParser::AliasContext *context) override;

      antlrcpp::Any visitColumnAlias(SQLParser::ColumnAliasContext *context) override;

      std::vector<Statements::ColumnName> GetColumnsList(SQLParser::ColumnListContext *context);
  };
}
