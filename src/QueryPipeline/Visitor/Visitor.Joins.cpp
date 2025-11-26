#include "Visitor.h"
#include "../Statements/Statements.h"

namespace QueryPipeline {
  antlrcpp::Any SQLVisitorImplementation::visitJoinStatement(SQLParser::JoinStatementContext *context) {
    auto* statement  = new Statements::JoinStatement();

    statement->type = (context->joinType())
          ? std::any_cast<Constants::JoinType>(visit(context->joinType()))
          : Constants::JoinType::Inner;
    statement->table = std::any_cast<Statements::DataSource*>(visit(context->tableName()));

    auto [expression] = std::any_cast<ExpressionWrapper>(visit(context->resultExpression()));
    statement->expression = std::any_cast<Expressions::Expression*>(expression);

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