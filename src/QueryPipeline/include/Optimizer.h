#pragma once
#include "../../DatabaseEngine/include/Constants.h"
#include "../../DatabaseEngine/include/Evaluators/Expression.h"
#include "../../Systemic/include/DataTypes/Value.h"
#include <vector>

#include "../../Systemic/include/Constants.h"
#include "../../Systemic/include/DataStructures/HashSet.h"

namespace QueryPipeline::Statements {
  struct JoinStatement;
  struct SelectStatement;
}

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

namespace QueryPipeline {
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

    std::vector<column_index_t> indexCoveredColumns;

    Expressions::Expression* expression;

    IndexSeekAnalyzeResults();
    explicit IndexSeekAnalyzeResults(Expressions::Expression* otherExpr);
  };

  struct JoinOrderAnalyzeResult{
    std::vector<table_id_t> order;
    std::vector<Statements::JoinStatement*> orderedJoins;
    bool isReordered;

    JoinOrderAnalyzeResult();
  };

  struct JoinOrderAnalyzeInfo{
    table_id_t tableId;
    int64_t rowCount;
    bool hasIndex;

    Statements::JoinStatement* joinStatement;

    JoinOrderAnalyzeInfo(){
      this->tableId = INVALID_TABLE_ID;
      this->rowCount = 0;
      this->hasIndex = false;
      this->joinStatement = nullptr;
    }

    JoinOrderAnalyzeInfo(
      const table_id_t& tableId,
      const int64_t& rowCount,
      const bool& hasIndex
    ){
      this->tableId = tableId;
      this->rowCount = rowCount;
      this->hasIndex = hasIndex;
      this->joinStatement = nullptr;
    }

    JoinOrderAnalyzeInfo(
      const table_id_t& tableId,
      const int64_t& rowCount,
      const bool& hasIndex,
      Statements::JoinStatement* joinStatement
    ){
      this->tableId = tableId;
      this->rowCount = rowCount;
      this->hasIndex = hasIndex;
      this->joinStatement = joinStatement;
    }

    bool operator()(const JoinOrderAnalyzeInfo& lhs, const JoinOrderAnalyzeInfo& rhs) const{
      if (lhs.hasIndex != rhs.hasIndex)
        return lhs.hasIndex;

      return lhs.rowCount < rhs.rowCount;
    }
  };

  struct PredicatePushDownResult{
    Dictionary<table_id_t, Expressions::Expression*> tablePredicatesDictionary;
    Expressions::Expression* remainingPredicate;

    PredicatePushDownResult(){
      this->remainingPredicate = nullptr;
    }

    Expressions::Expression* PushDownFilter(const table_id_t& tableId) const{
      Expressions::Expression* filter = nullptr;
      this->tablePredicatesDictionary.TryGetValue(tableId, filter);
      return filter;
    }
  };

  class Optimizer final{
      [[nodiscard]] static bool Analyze(
        const Expressions::ColumnExpression* columnExpression,
        const Expressions::Expression* otherExpression,
        const Expressions::BinaryOperator& operation,
        const std::vector<Headers::IndexColumnsHeader>& indexColumns,
        std::vector<IndexSeekAnalyzeResults>& results
      );

      [[nodiscard]] static bool Analyze(
        const Expressions::BinaryExpression* expression,
        const std::vector<Headers::IndexColumnsHeader>& indexColumns,
        std::vector<IndexSeekAnalyzeResults>& results
      );

      [[nodiscard]] static bool Analyze(
        const Expressions::LogicalExpression* expression,
        const std::vector<Headers::IndexColumnsHeader>& indexColumns,
        std::vector<IndexSeekAnalyzeResults>& results
      );
      [[nodiscard]] static bool Analyze(
        Expressions::Expression* expression,
        const std::vector<Headers::IndexColumnsHeader>& indexColumns,
        std::vector<IndexSeekAnalyzeResults>& results
      );

      static void SplitConjunctions(
        Expressions::Expression* expression,
        std::vector<Expressions::Expression*>& conjunctions
      );

      static void GetInvolvedTables(
        const Expressions::Expression* expression,
        HashSet<table_id_t>& involvedTables
      );

      static std::vector<table_id_t> GetInvolvedTables(const Expressions::Expression* expression);

      static void CombineExpressionsWithAnd(
        Expressions::Expression*& baseExpression,
        Expressions::Expression* newExpression
      );

      static void ProcessPredicate(
        Expressions::Expression* baseExpression,
        Expressions::Expression*& remainingPredicate,
        Dictionary<table_id_t, Expressions::Expression*>& tablePredicatesDictionary
      );

    public:

      [[nodiscard]] static std::vector<IndexSeekAnalyzeResults> AnalyzeTableScan(
        const LogicalTableScan* plan,
        const std::vector<Headers::IndexColumnsHeader> &indexColumns
      );

      [[nodiscard]] static JoinOrderAnalyzeResult DetermineJoinOrder(Statements::SelectStatement* statement);
      [[nodiscard]] static PredicatePushDownResult PushDownPredicates(
        const std::vector<table_id_t>& tables,
        Expressions::Expression* expression,
        const std::vector<Statements::JoinStatement*>& joins
      );
  };



}