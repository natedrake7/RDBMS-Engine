#pragma once
#include "Statements.h"


#include <SQLVisitor.h>

namespace QueryPipeline {
  static std::string String = "string";
  static std::string UnicodeString = "unicodestring";
  static std::string Decimal = "decimal";

  class SQLVisitorImplementation final : public SQLVisitor {
    public:
      antlrcpp::Any visitStatement(SQLParser::StatementContext *context) override;

      //Statements
      antlrcpp::Any visitSelectStatement(SQLParser::SelectStatementContext *context) override;

      antlrcpp::Any visitCreateDbStatement(SQLParser::CreateDbStatementContext *context) override;

      antlrcpp::Any visitDropDbStatement(SQLParser::DropDbStatementContext *context) override;

      antlrcpp::Any visitDeleteStatement(SQLParser::DeleteStatementContext *context) override;

      antlrcpp::Any visitUpdateStatement(SQLParser::UpdateStatementContext *context) override;

      antlrcpp::Any visitOrderByStatement(SQLParser::OrderByStatementContext *context) override;

      antlrcpp::Any visitCreateIndexStatement(SQLParser::CreateIndexStatementContext *context) override;

      antlrcpp::Any visitJoinStatement(SQLParser::JoinStatementContext *context) override;

      antlrcpp::Any visitColumnName(SQLParser::ColumnNameContext *context) override;

      antlrcpp::Any visitTableName(SQLParser::TableNameContext *context) override;

      antlrcpp::Any visitColumnList(SQLParser::ColumnListContext *context) override;

      antlrcpp::Any visitSqlStatement(SQLParser::SqlStatementContext *context) override;

      antlrcpp::Any visitWhereClause(SQLParser::WhereClauseContext *context) override;

      antlrcpp::Any visitLiteralValue(SQLParser::LiteralValueContext *context) override;

      antlrcpp::Any visitInsertStatement(SQLParser::InsertStatementContext *context) override;

      antlrcpp::Any visitLiteralValueList(SQLParser::LiteralValueListContext *context) override;

      antlrcpp::Any visitCreateTableStatement(SQLParser::CreateTableStatementContext *context) override;

      antlrcpp::Any visitDataType(SQLParser::DataTypeContext *context) override;

      antlrcpp::Any visitPrimaryKey(SQLParser::PrimaryKeyContext *context) override;

      antlrcpp::Any visitAddColumn(SQLParser::AddColumnContext *context) override;

      antlrcpp::Any visitDecimalType(SQLParser::DecimalTypeContext *context) override;

      antlrcpp::Any visitPrimaryKeyConstraint(SQLParser::PrimaryKeyConstraintContext *context) override;

      antlrcpp::Any visitCreateSchemaStatement(SQLParser::CreateSchemaStatementContext *context) override;

      antlrcpp::Any visitUpdateColumnsList(SQLParser::UpdateColumnsListContext *context) override;

      antlrcpp::Any visitUpdateColumn(SQLParser::UpdateColumnContext *context) override;

      antlrcpp::Any visitAutoIncrementKey(SQLParser::AutoIncrementKeyContext *context) override;

      antlrcpp::Any visitJoinType(SQLParser::JoinTypeContext *context) override;

      antlrcpp::Any visitAlias(SQLParser::AliasContext *context) override;

      antlrcpp::Any visitColumnAlias(SQLParser::ColumnAliasContext *context) override;

      antlrcpp::Any visitIdentifier(SQLParser::IdentifierContext *context) override;

      std::vector<Statements::ColumnName> GetColumnsList(SQLParser::ColumnListContext *context);

      antlrcpp::Any visitAlterTableStatement(SQLParser::AlterTableStatementContext *context) override;

      antlrcpp::Any visitAlterTableAction(SQLParser::AlterTableActionContext *context) override;

      antlrcpp::Any visitAlterTableAddColumn(SQLParser::AlterTableAddColumnContext *context) override;

      antlrcpp::Any visitAlterTableDropColumn(SQLParser::AlterTableDropColumnContext *context) override;

      antlrcpp::Any visitAlterTableModifyColumn(SQLParser::AlterTableModifyColumnContext *context) override;

      antlrcpp::Any visitAlterTableRenameColumn(SQLParser::AlterTableRenameColumnContext *context) override;

      antlrcpp::Any visitDefaultValue(SQLParser::DefaultValueContext *context) override;

      antlrcpp::Any visitDeclareVariableStatement(SQLParser::DeclareVariableStatementContext *context) override;

      antlrcpp::Any visitVariableName(SQLParser::VariableNameContext *context) override;

      antlrcpp::Any visitVariableType(SQLParser::VariableTypeContext *context) override;

      antlrcpp::Any visitSetVariableStatement(SQLParser::SetVariableStatementContext *context) override;

      antlrcpp::Any visitResultList(SQLParser::ResultListContext *context) override;

      antlrcpp::Any visitResultValue(SQLParser::ResultValueContext *context) override;

      antlrcpp::Any visitAtomicOperator(SQLParser::AtomicOperatorContext *context) override;

      antlrcpp::Any visitFunctionCall(SQLParser::FunctionCallContext *context) override;

      antlrcpp::Any visitFunctionName(SQLParser::FunctionNameContext *context) override;

      antlrcpp::Any visitRelationalExpr(SQLParser::RelationalExprContext *context) override;

      antlrcpp::Any visitRelationalOperator(SQLParser::RelationalOperatorContext *context) override;

      antlrcpp::Any visitAdditiveExpr(SQLParser::AdditiveExprContext *context) override;

      antlrcpp::Any visitAdditiveOperator(SQLParser::AdditiveOperatorContext *context) override;

      antlrcpp::Any visitMultiplicativeExpr(SQLParser::MultiplicativeExprContext *context) override;

      antlrcpp::Any visitMultiplicativeOperator(SQLParser::MultiplicativeOperatorContext *context) override;

      antlrcpp::Any visitPrimaryExpr(SQLParser::PrimaryExprContext *context) override;

      antlrcpp::Any visitResultExpression(SQLParser::ResultExpressionContext *context) override;

      antlrcpp::Any visitAndExpr(SQLParser::AndExprContext *context) override;

      antlrcpp::Any visitEqualityExpr(SQLParser::EqualityExprContext *context) override;

      antlrcpp::Any visitStringType(SQLParser::StringTypeContext *context) override;

      antlrcpp::Any visitUStringType(SQLParser::UStringTypeContext *context) override;

      antlrcpp::Any visitOrderColumnList(SQLParser::OrderColumnListContext *context) override;

      antlrcpp::Any visitOrderColumn(SQLParser::OrderColumnContext *context) override;

      antlrcpp::Any visitOrder(SQLParser::OrderContext *context) override;

      antlrcpp::Any visitValuesStatement(SQLParser::ValuesStatementContext *context) override;

      antlrcpp::Any visitValuesList(SQLParser::ValuesListContext *context) override;

      antlrcpp::Any visitSign(SQLParser::SignContext *context) override;

      antlrcpp::Any visitTop(SQLParser::TopContext *context) override;

      antlrcpp::Any visitDistinct(SQLParser::DistinctContext *context) override;

      antlrcpp::Any visitUseDbStatement(SQLParser::UseDbStatementContext *context) override;

      antlrcpp::Any visitCreateUserStatement(SQLParser::CreateUserStatementContext *context) override;

      antlrcpp::Any visitGrantRoleStatement(SQLParser::GrantRoleStatementContext *context) override;

      antlrcpp::Any visitDatasource(SQLParser::DatasourceContext *context) override;

      antlrcpp::Any visitBranchingExpression(SQLParser::BranchingExpressionContext *context) override;

      antlrcpp::Any visitSwitchExpression(SQLParser::SwitchExpressionContext *context) override;

      antlrcpp::Any visitCaseExpression(SQLParser::CaseExpressionContext *context) override;

      antlrcpp::Any visitTernaryExpression(SQLParser::TernaryExpressionContext *context) override;
  };

  static std::string CreatePositionErrorMessage(const antlr4::ParserRuleContext* context) {
    const auto* token = context->getStart();

    return ". Error at line: " + std::to_string(token->getLine()) + ", at position: " + std::to_string(token->getStartIndex());
  }

  struct ExpressionWrapper {
    Expressions::Expression* expression;
  };
}
