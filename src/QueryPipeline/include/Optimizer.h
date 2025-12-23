#pragma once
#include "../../DatabaseEngine/include/PipelineConstants.h"
#include "../../DatabaseEngine/include/Evaluators/Expression.h"
#include "../../Systemic/include/DataTypes/Value.h"
#include <vector>

#include "../../Systemic/include/Constants.h"
#include "../../Systemic/include/Key.h"
#include "../../Systemic/include/DataStructures/HashSet.h"
#include "../../Systemic/include/Headers.h"

namespace QueryPipeline::Statements {
  struct JoinStatement;
  struct SelectStatement;
}

namespace Headers {
  struct IndexColumnsHeader;
}

namespace QueryPipeline {
  namespace PipelineConstants {
    enum class JoinAlgorithm : UnsignedTinyInt;
  }

  class LogicalTableScan;
}

namespace Expressions {
  class BinaryExpression;
  class LogicalExpression;
  class Expression;
}

namespace QueryPipeline {
  struct JoinConditionInfo{
    Int leftColumnId;
    column_index_t leftColumnIndex;

    Int rightColumnId;
    column_index_t rightColumnIndex;

    bool isEqualityJoin;
    Expressions::Expression* expression;
  };

  struct JoinAlgorithmAnalysisResult{
    PipelineConstants::JoinAlgorithm algorithm;

    std::vector<column_index_t> leftKeyColumns;
    std::vector<column_index_t> rightKeyColumns;

    Expressions::Expression* remainingPredicate;

    JoinAlgorithmAnalysisResult();
    explicit JoinAlgorithmAnalysisResult(const PipelineConstants::JoinAlgorithm& algorithm);
    JoinAlgorithmAnalysisResult(
      const PipelineConstants::JoinAlgorithm& algorithm,
      Expressions::Expression* expression,
      std::vector<column_index_t>& leftKeyColumns,
      std::vector<column_index_t>& rightKeyColumns
    );
  };

  struct JoinAlgorithmInfo{
    std::vector<JoinConditionInfo> joinConditions;
    Expressions::Expression* remainingPredicate;
  };

  struct Range{
    DataTypes::Indexing::Key start;
    DataTypes::Indexing::Key end;

    bool hasRange;
    bool canSeek;
    Expressions::Expression* remainingPredicate;

    Range();
  };

  struct SeekRange {
    Value start;
    Value end;

    bool startInclusive;
    bool endInclusive;
    bool hasRange;

    SeekRange();
    SeekRange(
      const Value& otherStart,
      const Value& otherEnd,
      const bool& includeStart,
      const bool& includeEnd
    );

    [[nodiscard]] bool HasStart() const;
    [[nodiscard]] bool HasEnd() const;
  };

  struct IndexSeekColumnAnalysisResults {
    bool canIndexSeek;
    bool needsParameterBinding;
    Int columnId;
    SeekRange range;

    Expressions::Expression* expression;

    IndexSeekColumnAnalysisResults();
    explicit IndexSeekColumnAnalysisResults(Expressions::Expression* otherExpr);
  };

  struct IndexSeekAnalysisResult{
    std::vector<IndexSeekColumnAnalysisResults> analyzeResults;
    std::vector<Expressions::Expression*> conjunctions;
  };

  struct IndexCandidate{
    Headers::IndexHeader* header;
    std::vector<IndexSeekColumnAnalysisResults> analyzeInfo;
    std::vector<Expressions::Expression*>* conjunctions;
    double estimatedCost;
    int matchingColumns;

    bool operator()(const IndexCandidate& lhs, const IndexCandidate& rhs) const{
      if (lhs.matchingColumns != rhs.matchingColumns)
        return lhs.matchingColumns > rhs.matchingColumns;

      if (std::abs(lhs.estimatedCost - rhs.estimatedCost) > 0.01)
        return lhs.estimatedCost < rhs.estimatedCost;

      return lhs.header->isClustered && !rhs.header->isClustered;
    }
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

      static void AnalyzeTableScan(
        Expressions::Expression* baseExpression,
        const Expressions::BinaryExpression* expression,
        Dictionary<column_id_t, std::vector<Expressions::Expression*>>& columnPredicatesDictionary
      );

      static void DetermineCanSeekOnEquality(
        const Value& predicateValue,
        SeekRange& range,
        bool& canSeek
      );

      static void DetermineCanSeekOnGreaterThan(
        const Value& predicateValue,
        SeekRange& range,
        bool& canSeek,
        const bool& inclusive
      );

      static void DetermineCanSeekOnLessThan(
        const Value& predicateValue,
        SeekRange& range,
        bool& canSeek,
        const bool& inclusive
      );

      static void DetermineSeekRange(
        const Expressions::BinaryExpression* expression,
        const Value& predicateValue,
        SeekRange& range,
        bool& canSeek
      );

      static std::vector<IndexSeekColumnAnalysisResults> AnalyzeTableScan(
        const Headers::IndexHeader& index,
        const std::vector<Expressions::Expression*>& conjunctions
      );

      static Range BuildSeekKeys(
        const std::vector<IndexSeekColumnAnalysisResults>& analyzeResults,
        std::vector<Expressions::Expression*>& conjunctions
      );

      static void ProcessJoinCondition(
        Expressions::Expression* expression,
        std::vector<JoinConditionInfo>& conditionsInfo,
        bool& isEqualityJoin
      );

      [[nodiscard]] static std::vector<Int> CheckPredicatesSorting(
        const Headers::TableStatistics& tableStats,
        const std::vector<JoinConditionInfo>& joinConditions,
        const bool& isLeftTable
      );

      static JoinAlgorithmAnalysisResult CreateMergeJoinKeys(
        const std::vector<JoinConditionInfo>& conditionsInfo,
        const std::vector<Int>& leftKeyColumns,
        const std::vector<Int>& rightKeyColumns
      );

    public:
      [[nodiscard]] static JoinOrderAnalyzeResult DetermineJoinOrder(Statements::SelectStatement* statement);
      [[nodiscard]] static PredicatePushDownResult PushDownPredicates(
        const std::vector<table_id_t>& tables,
        Expressions::Expression* expression,
        const std::vector<Statements::JoinStatement*>& joins
      );

      [[nodiscard]] static Range PerformIndexAnalysis(
        std::vector<Headers::IndexHeader>& indexes,
        Expressions::Expression* expression,
        const Headers::TableStatistics& tableStatistics
      );

      [[nodiscard]] static JoinAlgorithmAnalysisResult ChooseJoinAlgorithm(
        const Int& leftTableId,
        const Int& rightTableId,
        Expressions::Expression* joinCondition
      );
  };



}