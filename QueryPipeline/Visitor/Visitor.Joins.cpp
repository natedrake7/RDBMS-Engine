#include "Visitor.h"
#include "../Statements/Statements.h"

namespace QueryPipeline {
  antlrcpp::Any SQLVisitorImplementation::visitJoinStatement(SQLParser::JoinStatementContext *context) {
    auto* statement  = new Statements::JoinStatement();

    statement->type = (context->joinType())
          ? std::any_cast<Constants::JoinType>(visit(context->joinType()))
          : Constants::JoinType::Inner;
    statement->table = std::any_cast<Statements::TableName*>(visit(context->tableName()));
    statement->expression = std::any_cast<Expressions::LogicalExpression*>(visit(context->resultExpression()));

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitJoinType(SQLParser::JoinTypeContext *context){
    if (context->INNER())
      return Constants::JoinType::Inner;
    if (context->FULL())
      return Constants::JoinType::Full;
    if (context->LEFT())
      return Constants::JoinType::Left;
    if (context->RIGHT())
      return Constants::JoinType::Right;

    return Constants::JoinType::Inner;
  }
}