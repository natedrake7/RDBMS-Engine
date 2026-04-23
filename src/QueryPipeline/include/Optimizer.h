#pragma once
#include "../../CoreEngine/include/Evaluators/Expression.h"
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
    struct QueryContext;
    class CompileContext;

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
    Int leftTableId;
    Int leftColumnId;
    column_index_t leftColumnIndex;

    Int rightTableId;
    Int rightColumnId;
    column_index_t rightColumnIndex;

    bool isEqualityJoin;
    Expressions::Expression* expression;
  };

  struct JoinAlgorithmAnalysisResult{
    PipelineConstants::JoinAlgorithm algorithm;

    DataStructures::PolymorphicArray<column_index_t> leftKeyColumns;
    DataStructures::PolymorphicArray<column_index_t> rightKeyColumns;

    Expressions::Expression* remainingPredicate;

    JoinAlgorithmAnalysisResult();
    explicit JoinAlgorithmAnalysisResult(const PipelineConstants::JoinAlgorithm& algorithm);
    JoinAlgorithmAnalysisResult(
      const PipelineConstants::JoinAlgorithm& algorithm,
      Expressions::Expression* expression,
      DataStructures::PolymorphicArray<column_index_t>& leftKeyColumns,
      DataStructures::PolymorphicArray<column_index_t>& rightKeyColumns
    );
  };

  struct JoinAlgorithmInfo{
    DataStructures::PolymorphicArray<JoinConditionInfo> joinConditions;
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
      bool includeStart,
      bool includeEnd
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
    DataStructures::PolymorphicArray<IndexSeekColumnAnalysisResults> analyzeResults;
    DataStructures::PolymorphicArray<Expressions::Expression*> conjunctions;
  };

  struct IndexCandidate{
    Headers::IndexHeader* header;
    DataStructures::PolymorphicArray<IndexSeekColumnAnalysisResults> analyzeInfo;
    DataStructures::PolymorphicArray<Expressions::Expression*>* conjunctions;
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
    DataStructures::PolymorphicArray<table_id_t> order;
    DataStructures::PolymorphicArray<Statements::JoinStatement*> orderedJoins;
    bool isReordered;

    JoinOrderAnalyzeResult(const ::Memory::IAllocator* allocator);
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
      const table_id_t tableId,
      const BigInt rowCount,
      const bool hasIndex
    ){
      this->tableId = tableId;
      this->rowCount = rowCount;
      this->hasIndex = hasIndex;
      this->joinStatement = nullptr;
    }

    JoinOrderAnalyzeInfo(
      const table_id_t tableId,
      const BigInt rowCount,
      const bool hasIndex,
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

    Expressions::Expression* PushDownFilter(const table_id_t tableId) const{
      Expressions::Expression* filter = nullptr;
      this->tablePredicatesDictionary.TryGetValue(tableId, filter);
      return filter;
    }
  };

  class Optimizer final{
      QueryContext* context;

      static void SplitConjunctions(
        Expressions::Expression* expression,
        DataStructures::PolymorphicArray<Expressions::Expression*>& conjunctions
      );

      static void GetInvolvedTables(
        const Expressions::Expression* expression,
        HashSet<table_id_t>& involvedTables
      );

      DataStructures::PolymorphicArray<table_id_t> GetInvolvedTables(const Expressions::Expression* expression) const;

      void CombineExpressionsWithAnd(
        Expressions::Expression*& baseExpression,
        Expressions::Expression* newExpression
      ) const;

      void ProcessPredicate(
        Expressions::Expression* baseExpression,
        Dictionary<table_id_t, Expressions::Expression*>& tablePredicatesDictionary,
        Expressions::Expression*& remainingPredicate
      ) const;

      static void AnalyzeTableScan(
        Expressions::Expression* baseExpression,
        const Expressions::BinaryExpression* expression,
        Dictionary<column_id_t, DataStructures::PolymorphicArray<Expressions::Expression*>>& columnPredicatesDictionary
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
        bool inclusive
      );

      static void DetermineCanSeekOnLessThan(
        const Value& predicateValue,
        SeekRange& range,
        bool& canSeek,
        bool inclusive
      );

      static void DetermineSeekRange(
        const Expressions::BinaryExpression* expression,
        const Value& predicateValue,
        SeekRange& range,
        bool& canSeek
      );

      static void AnalyzeTableScan(
        IndexSeekColumnAnalysisResults& analyzeResult,
        Expressions::BinaryExpression* binaryExpr,
        const Expressions::ColumnExpression* columnExpr,
        Expressions::Expression* otherExpression
      );

      static DataStructures::PolymorphicArray<IndexSeekColumnAnalysisResults> AnalyzeTableScan(
        const Headers::IndexHeader& index,
        const DataStructures::PolymorphicArray<Expressions::Expression*>& conjunctions
      );

      Range BuildSeekKeys(
        const DataStructures::PolymorphicArray<IndexSeekColumnAnalysisResults>& analyzeResults,
        DataStructures::PolymorphicArray<Expressions::Expression*>& conjunctions
      );

      static void ProcessJoinCondition(
        Expressions::Expression* expression,
        DataStructures::PolymorphicArray<JoinConditionInfo>& conditionsInfo,
        bool& isEqualityJoin
      );

      [[nodiscard]] DataStructures::PolymorphicArray<Int> CheckPredicatesSorting(
        const Headers::TableStatistics& tableStats,
        const DataStructures::PolymorphicArray<JoinConditionInfo>& joinConditions
      ) const;

      JoinAlgorithmAnalysisResult CreateMergeJoinKeys(
        const DataStructures::PolymorphicArray<JoinConditionInfo>& conditionsInfo,
        const DataStructures::PolymorphicArray<Int>& leftKeyColumns,
        const DataStructures::PolymorphicArray<Int>& rightKeyColumns,
        Int leftTableId,
        Int rightTableId
      ) const;

    public:
        explicit Optimizer(QueryContext& context);

        [[nodiscard]] JoinOrderAnalyzeResult DetermineJoinOrder(Statements::SelectStatement* statement) const;
        [[nodiscard]] PredicatePushDownResult PushDownPredicates(
            const DataStructures::PolymorphicArray<table_id_t>& tables,
            Expressions::Expression* whereClause,
            const DataStructures::PolymorphicArray<Statements::JoinStatement*>& joins
        ) const;
        [[nodiscard]] Range PerformIndexAnalysis(
            DataStructures::PolymorphicArray<Headers::IndexHeader>& indexes,
            Expressions::Expression* expression,
            const Headers::TableStatistics& tableStatistics
        );
        [[nodiscard]] JoinAlgorithmAnalysisResult ChooseJoinAlgorithm(
            Int leftTableId,
            Int rightTableId,
            Expressions::Expression* joinCondition
        );
  };



}