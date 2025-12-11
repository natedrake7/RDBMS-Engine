#pragma once
#include "../../Database/include/Constants.h"
#include "../../Expressions/include/Expression.h"
#include "../../Systemic/include/DataTypes/Value.h"
#include <vector>

namespace Headers {
  struct IndexColumnsHeader;
}

namespace QueryPipeline {
  class LogicalTableScan;
}

namespace Expressions {
  class BinaryExpression;
  class LogicalExpression;
  class Expression;
}

namespace QueryPipeline::Optimizer {
  struct SeekRange {
    Value start;
    Value end;

    bool startInclusive;
    bool endInclusive;

    SeekRange();
    SeekRange(
      const Value& otherStart,
      const Value& otherEnd,
      const bool& includeStart,
      const bool& includeEnd
    );
  };

  struct IndexSeekAnalyzeResults {
    bool canIndexSeek;
    SeekRange range;

    std::vector<Constants::column_index_t> indexCoveredColumns;

    Expressions::Expression* expression;

    IndexSeekAnalyzeResults();
    explicit IndexSeekAnalyzeResults(Expressions::Expression* otherExpr);
  };

  [[nodiscard]] std::vector<IndexSeekAnalyzeResults> AnalyzeTableScan(
    const LogicalTableScan* plan,
    const std::vector<Headers::IndexColumnsHeader> &indexColumns
  );

  [[nodiscard]] bool Analyze(
    const Expressions::ColumnExpression* columnExpression,
    const Expressions::Expression* otherExpression,
    const Expressions::BinaryOperator& operation,
    const std::vector<Headers::IndexColumnsHeader>& indexColumns,
    std::vector<IndexSeekAnalyzeResults>& results
  );

  [[nodiscard]] bool Analyze(
    const Expressions::BinaryExpression* expression,
    const std::vector<Headers::IndexColumnsHeader>& indexColumns,
    std::vector<IndexSeekAnalyzeResults>& results
  );

  [[nodiscard]] bool Analyze(
    const Expressions::LogicalExpression* expression,
    const std::vector<Headers::IndexColumnsHeader>& indexColumns,
    std::vector<IndexSeekAnalyzeResults>& results
  );
  [[nodiscard]] bool Analyze(
    Expressions::Expression* expression,
    const std::vector<Headers::IndexColumnsHeader>& indexColumns,
    std::vector<IndexSeekAnalyzeResults>& results
  );

}