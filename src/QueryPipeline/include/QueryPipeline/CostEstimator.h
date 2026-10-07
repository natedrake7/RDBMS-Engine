#pragma once

#include <vector>

#include <Systemic/DataTypes/DataTypes.h>
#include <CoreEngine/Evaluators/Expression.h>

class Value;
namespace CoreEngine::Catalog {
    struct ColumnHistograms;
}

enum class DataType : unsigned char;

namespace CoreEngine::Catalog {
    struct ColumnStatistics;
    struct TableStatistics;
    struct IndexHeader;
}

namespace QueryPipeline{
    struct QueryContext;
    struct IndexCandidate;
    struct SeekRange;
    struct IndexSeekColumnAnalysisResults;

    class CostEstimator final{
        struct HistogramSelectivityEstimate{
            int previousRows;
            int bucketSize;
            BigInt totalTableRows;
            int bucketIndex;
            const Value* value;

            HistogramSelectivityEstimate();
            void CalculatePreviousRows();
        };

        static constexpr Int DEFAULT_EXPRESSION_COMPLEXITY = 0;
        static constexpr Int CONSTANT_EXPRESSION_COMPLEXITY = 0;
        static constexpr Int ADDITION_EXPRESSION_COMPLEXITY = 3;
        static constexpr Int MULTIPLICATION_EXPRESSION_COMPLEXITY = 2;
        static constexpr Int DIVISION_EXPRESSION_COMPLEXITY = 5;
        static constexpr Int EQUALITY_EXPRESSION_COMPLEXITY = 2;
        static constexpr Int FUNCTION_EXPRESSION_COMPLEXITY = 10;
        static constexpr Int LOGICAL_AND_EXPRESSION_COMPLEXITY = 2;
        static constexpr Int BRANCH_EXPRESSION_COMPLEXITY = 8;


        static constexpr double INTEGER_COMPARISON_COST = 1.0;
        static constexpr double DECIMAL_COMPARISON_COST = 1.5;
        static constexpr double STRING_COMPARISON_COST = 5.0;
        static constexpr double DEFAULT_COMPARISON_COST = 0.0;


        [[nodiscard]] static Int EstimateExpressionComplexity(const Expressions::Expression* expression);
        [[nodiscard]] static Int EstimateBinaryExpressionComplexity(const Expressions::BinaryExpression* binaryExpr);
        [[nodiscard]] static Int EstimateOperationComplexity(const Expressions::BinaryOperator& binaryExpr);
        [[nodiscard]] static Int EstimateLogicalExpressionComplexity(const Expressions::LogicalExpression* logicalExpr);
        [[nodiscard]] static Int EstimateFunctionExpressionComplexity(const Expressions::FunctionExpression* functionExpr);
        [[nodiscard]] static Int EstimateBranchExpressionComplexity(const Expressions::BranchExpression* branchExpr);
        [[nodiscard]] static double EstimateExpressionComparisonCost(const Expressions::Expression* expression);

        [[nodiscard]] static double InterpolateBucket(
            const CoreEngine::Catalog::ColumnHistograms& bucket,
            const Value& value
        );

        [[nodiscard]] static int FindBucketForValue(
            const DataStructures::PolymorphicArray<CoreEngine::Catalog::ColumnHistograms>& histograms,
            const Value& value
        );

        [[nodiscard]] static double EstimateEqualSelectivityByHistograms(
            const SeekRange& range,
            const CoreEngine::Catalog::ColumnHistograms& histogram
        );

        [[nodiscard]] static double EstimateRangeStartSelectivityByHistograms(
            const CoreEngine::Catalog::ColumnHistograms& histogram,
            const HistogramSelectivityEstimate& info
        );

        [[nodiscard]] static double EstimateRangeEndSelectivityByHistograms(
            const CoreEngine::Catalog::ColumnHistograms& histogram,
            const HistogramSelectivityEstimate& info
        );

        [[nodiscard]] static double EstimateRangeSelectivityByHistograms(
            const CoreEngine::Catalog::ColumnHistograms& startHistogram,
            const HistogramSelectivityEstimate& startInfo,
            const CoreEngine::Catalog::ColumnHistograms& endHistogram,
            const HistogramSelectivityEstimate& endInfo
        );

        [[nodiscard]] static double EstimateSelectivityByHistograms(
            const QueryContext* context,
            const SeekRange& range,
            const CoreEngine::Catalog::TableStatistics& tableStats,
            const CoreEngine::Catalog::ColumnStatistics& columnStats
        );

        [[nodiscard]] static double EstimateSelectivityForSmallTable(
            const SeekRange& range,
            const CoreEngine::Catalog::ColumnStatistics& columnStats
        );

        [[nodiscard]] static double EstimateSelectivity(
            const QueryContext* context,
            const SeekRange& range,
            const CoreEngine::Catalog::ColumnStatistics& columnStats,
            const CoreEngine::Catalog::TableStatistics& tableStats
        );

    public:
        static void EstimateIndexCost(
            const QueryContext* context,
            IndexCandidate& candidate,
            const CoreEngine::Catalog::TableStatistics& tableStats
        );

        [[nodiscard]] static double EstimateFilterCost(
            const Expressions::Expression* filterExpression,
            BigInt inputRows,
            double selectivity
        );

        [[nodiscard]] static double EstimateProjectionCost(
            const std::vector<Expressions::Expression*>& projections,
            BigInt inputRows
        );

        [[nodiscard]] static double EstimateSortCost(
            BigInt inputRows,
            const std::vector<Expressions::Expression*>& sortExpressions
        );
    };
}