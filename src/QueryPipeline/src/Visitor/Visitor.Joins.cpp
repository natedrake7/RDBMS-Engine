#include "../../include/Visitor.h"
#include "../../include/Statements.h"

namespace QueryPipeline {
  antlrcpp::Any SQLVisitorImplementation::visitJoinStatement(SQLParser::JoinStatementContext *context) {
    auto* statement  = new Statements::JoinStatement();

    statement->type = (context->joinType())
          ? std::any_cast<JoinType>(visit(context->joinType()))
          : JoinType::Inner;
    statement->table = std::any_cast<Statements::DataSource*>(visit(context->tableName()));

    auto [expression] = std::any_cast<ExpressionWrapper>(visit(context->resultExpression()));
    statement->expression = std::any_cast<Expressions::Expression*>(expression);

    return statement;
  }

  antlrcpp::Any SQLVisitorImplementation::visitJoinType(SQLParser::JoinTypeContext *context){
    if (context->INNER())
      return JoinType::Inner;
    if (context->FULL())
      return JoinType::Full;
    if (context->LEFT())
      return JoinType::Left;
    if (context->RIGHT())
      return JoinType::Right;

    return JoinType::Inner;
  }
}