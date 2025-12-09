#include "Optimizer.h"

#include "../LogicalPlan/LogicalPlan.h"

namespace QueryPipeline::Optimizer {
  SeekRange::SeekRange() {
    this->endInclusive = false;
    this->startInclusive = false;
  }

  SeekRange::SeekRange(
   const Value &otherStart,
   const Value &otherEnd,
   const bool &includeStart,
   const bool &includeEnd
  ) {
    this->start = otherStart;
    this->end = otherEnd;
    this->startInclusive = includeStart;
    this->endInclusive = includeEnd;
  }

  IndexSeekAnalyzeResults::IndexSeekAnalyzeResults() {
      this->expression = nullptr;
      this->canIndexSeek = false;
  }

  IndexSeekAnalyzeResults::IndexSeekAnalyzeResults(Expressions::Expression *otherExpr) {
      this->expression = otherExpr;
      this->canIndexSeek = false;
  }

  std::vector<IndexSeekAnalyzeResults> AnalyzeTableScan(
    const LogicalTableScan *plan,
    const std::vector<Headers::IndexColumnsHeader> &indexColumns
  ){
    std::vector<IndexSeekAnalyzeResults> results;

    const auto _ = Analyze(plan->expression, indexColumns, results);

    return results;
  }

  bool Analyze(
    const Expressions::ColumnExpression *columnExpression,
    const Expressions::Expression* otherExpression,
    const Expressions::BinaryOperator& operation,
    const std::vector<Headers::IndexColumnsHeader> &indexColumns,
    std::vector<IndexSeekAnalyzeResults> &results
  ) {
    //can use index seek
    if (columnExpression->columnId != indexColumns[0].columnId)
      return false;

    if (otherExpression->IsConstant()) {
      auto* constantExpr = otherExpression->AsConstant();

      const auto includeStart =
        operation == Expressions::BinaryOperator::GreaterEqual
        || operation == Expressions::BinaryOperator::LessEqual
        ||  operation == Expressions::BinaryOperator::Equal;

      auto result = IndexSeekAnalyzeResults();

      result.canIndexSeek = true;
      result.range = SeekRange(constantExpr->value, constantExpr->value, includeStart, includeStart);
      results.emplace_back(result);

      return true;
    }

     return false;
  }

  bool Analyze(
    Expressions::Expression *expression,
    const std::vector<Headers::IndexColumnsHeader> &indexColumns,
    std::vector<IndexSeekAnalyzeResults> &results
  ){
    switch (expression->expressionType) {
      case Expressions::ExpressionType::Binary:
        return Analyze(expression->AsBinary(), indexColumns, results);
      case Expressions::ExpressionType::Logical:
        return Analyze(expression->AsLogical(), indexColumns, results);
      case Expressions::ExpressionType::Expression:
      case Expressions::ExpressionType::Column:
      case Expressions::ExpressionType::Constant:
      case Expressions::ExpressionType::Variable:
      case Expressions::ExpressionType::Branch:
      case Expressions::ExpressionType::Function:
      default:
        return false;
    }
  }

  bool Analyze(
    const Expressions::BinaryExpression *expression,
    const std::vector<Headers::IndexColumnsHeader> &indexColumns,
    std::vector<IndexSeekAnalyzeResults> &results
  ){
    switch (expression->operation) {
      case Expressions::BinaryOperator::Equal:
      case Expressions::BinaryOperator::Greater:
      case Expressions::BinaryOperator::GreaterEqual:
      case Expressions::BinaryOperator::Less:
      case Expressions::BinaryOperator::LessEqual: {
        if (expression->left->IsColumn()) {
          return Analyze(expression->left->AsColumn(), expression->right, expression->operation, indexColumns, results);
        }
        if (expression->right->IsColumn()) {
          return Analyze(expression->left->AsColumn(), expression->right, expression->operation, indexColumns, results);
        }

        break;
      }
      case Expressions::BinaryOperator::EqualIgnoreOrdinalCase:
      case Expressions::BinaryOperator::NotEqual:
      case Expressions::BinaryOperator::Add:
      case Expressions::BinaryOperator::Subtract:
      case Expressions::BinaryOperator::Multiply:
      case Expressions::BinaryOperator::Divide:
      case Expressions::BinaryOperator::Modulo:
      default:
        break;
    }

    return false;
  }

  bool Analyze(
    const Expressions::LogicalExpression *expression,
    const std::vector<Headers::IndexColumnsHeader> &indexColumns,
    std::vector<IndexSeekAnalyzeResults> &results
  ){
    if (expression->IsAnd()) {
      return Analyze(expression->left, indexColumns, results)
          && Analyze(expression->right, indexColumns, results);
    }

    if (expression->IsOr()) {
      return Analyze(expression->left, indexColumns, results)
        && Analyze(expression->right, indexColumns, results);
    }

    return false;
  }

}