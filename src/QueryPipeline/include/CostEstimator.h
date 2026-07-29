#pragma once

#include <vector>

#include "../../Systemic/include/DataTypes/DataTypes.h"
#include "../../CoreEngine/include/Evaluators/Expression.h"

class Value;
namespace Headers {
    struct ColumnHistograms;
}

enum class DataType : unsigned char;

namespace Headers {
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
            const Headers::ColumnHistograms& bucket,
            const Value& value
        );

        [[nodiscard]] static int FindBucketForValue(
            const DataStructures::PolymorphicArray<Headers::ColumnHistograms>& histograms,
            const Value& value
        );

        [[nodiscard]] static double EstimateEqualSelectivityByHistograms(
            const SeekRange& range,
            const Headers::ColumnHistograms& histogram
        );

        [[nodiscard]] static double EstimateRangeStartSelectivityByHistograms(
            const Headers::ColumnHistograms& histogram,
            const HistogramSelectivityEstimate& info
        );

        [[nodiscard]] static double EstimateRangeEndSelectivityByHistograms(
            const Headers::ColumnHistograms& histogram,
            const HistogramSelectivityEstimate& info
        );

        [[nodiscard]] static double EstimateRangeSelectivityByHistograms(
            const Headers::ColumnHistograms& startHistogram,
            const HistogramSelectivityEstimate& startInfo,
            const Headers::ColumnHistograms& endHistogram,
            const HistogramSelectivityEstimate& endInfo
        );

        [[nodiscard]] static double EstimateSelectivityByHistograms(
            const QueryContext* context,
            const SeekRange& range,
            const Headers::TableStatistics& tableStats,
            const Headers::ColumnStatistics& columnStats
        );

        [[nodiscard]] static double EstimateSelectivityForSmallTable(
            const SeekRange& range,
            const Headers::ColumnStatistics& columnStats
        );

        [[nodiscard]] static double EstimateSelectivity(
            const QueryContext* context,
            const SeekRange& range,
            const Headers::ColumnStatistics& columnStats,
            const Headers::TableStatistics& tableStats
        );

    public:
        static void EstimateIndexCost(
            const QueryContext* context,
            IndexCandidate& candidate,
            const Headers::TableStatistics& tableStats
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