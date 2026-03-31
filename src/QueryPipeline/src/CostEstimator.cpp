#include "../include/CostEstimator.h"

#include <cmath>

#include "DatabaseConstants.h"
#include "Optimizer.h"
#include "Parser.h"
#include "../../Systemic/include/Headers.h"
#include "Managers/StatisticsManager.h"
#include "SystemDatabases/SystemCatalog.h"

namespace QueryPipeline{
    CostEstimator::HistogramSelectivityEstimate::HistogramSelectivityEstimate(){
        this->value = nullptr;
        this->bucketIndex = 0;
        this->bucketSize = 0;
        this->previousRows = 0;
        this->totalTableRows = 0;
    }

    void CostEstimator::HistogramSelectivityEstimate::CalculatePreviousRows(){
        this->previousRows = this->bucketSize * this->bucketIndex;
    }

    Int CostEstimator::EstimateExpressionComplexity(const Expressions::Expression* expression){
        if (expression == nullptr)
            return CostEstimator::DEFAULT_EXPRESSION_COMPLEXITY;

        switch (expression->expressionType) {
            case Expressions::ExpressionType::Column:
            case Expressions::ExpressionType::Constant:
            case Expressions::ExpressionType::Variable:
                return CostEstimator::CONSTANT_EXPRESSION_COMPLEXITY;
            case Expressions::ExpressionType::Binary:
                return CostEstimator::EstimateBinaryExpressionComplexity(expression->AsBinary());
            case Expressions::ExpressionType::Logical:
                return CostEstimator::EstimateLogicalExpressionComplexity(expression->AsLogical());
            case Expressions::ExpressionType::Branch:
                return CostEstimator::EstimateBranchExpressionComplexity(expression->AsBranch());
            case Expressions::ExpressionType::Function:
                return CostEstimator::EstimateFunctionExpressionComplexity(expression->AsFunction());
            default:
                return 0;
        }
    }

    Int CostEstimator::EstimateBinaryExpressionComplexity(const Expressions::BinaryExpression* binaryExpr){
        const auto leftCost = CostEstimator::EstimateExpressionComplexity(binaryExpr->left);
        const auto rightCost = CostEstimator::EstimateExpressionComplexity(binaryExpr->right);

        const auto operationCost = CostEstimator::EstimateOperationComplexity(binaryExpr->operation);

        return leftCost + rightCost + operationCost;
    }

    Int CostEstimator::EstimateOperationComplexity(const Expressions::BinaryOperator& binaryExpr){
        switch (binaryExpr) {
            case Expressions::BinaryOperator::Add:
            case Expressions::BinaryOperator::Subtract:
                return CostEstimator::ADDITION_EXPRESSION_COMPLEXITY;
            case Expressions::BinaryOperator::Multiply:
                return CostEstimator::MULTIPLICATION_EXPRESSION_COMPLEXITY;
            case Expressions::BinaryOperator::Divide:
            case Expressions::BinaryOperator::Modulo:
                return CostEstimator::DIVISION_EXPRESSION_COMPLEXITY;
            case Expressions::BinaryOperator::Equal:
            case Expressions::BinaryOperator::NotEqual:
            case Expressions::BinaryOperator::Less:
            case Expressions::BinaryOperator::LessEqual:
            case Expressions::BinaryOperator::Greater:
            case Expressions::BinaryOperator::GreaterEqual:
                return CostEstimator::EQUALITY_EXPRESSION_COMPLEXITY;
            default:
                return 3;
        }
    }

    Int CostEstimator::EstimateLogicalExpressionComplexity(const Expressions::LogicalExpression* logicalExpr){
        switch (logicalExpr->logicalType){
            case Expressions::LogicalType::And:
            case Expressions::LogicalType::Or:
                return CostEstimator::LOGICAL_AND_EXPRESSION_COMPLEXITY
                    + CostEstimator::EstimateExpressionComplexity(logicalExpr->left)
                    + CostEstimator::EstimateExpressionComplexity(logicalExpr->right);
            // case Expressions::LogicalType::Not:
            //     return 1 + CostEstimator::EstimateExpressionComplexity(logicalExpr->left);
            default:
                return 0;
        }
    }

    Int CostEstimator::EstimateFunctionExpressionComplexity(const Expressions::FunctionExpression* functionExpr){
        Int cost = CostEstimator::FUNCTION_EXPRESSION_COMPLEXITY;

        for (const auto& argument: functionExpr->arguments)
            cost += CostEstimator::EstimateExpressionComplexity(argument);

        return cost;
    }

    Int CostEstimator::EstimateBranchExpressionComplexity(const Expressions::BranchExpression* branchExpr){
        Int cost = CostEstimator::BRANCH_EXPRESSION_COMPLEXITY;

        for (const auto& argument: branchExpr->arguments)
            cost += CostEstimator::EstimateExpressionComplexity(argument);

        for (const auto& branch: branchExpr->branches)
            cost += CostEstimator::EstimateExpressionComplexity(branch);

        for (const auto& result: branchExpr->results)
            cost += CostEstimator::EstimateExpressionComplexity(result);

        if (branchExpr->baseCase != nullptr)
            cost += CostEstimator::EstimateExpressionComplexity(branchExpr->baseCase);

        return cost;
    }

    double CostEstimator::EstimateExpressionComparisonCost(const Expressions::Expression* expression){
        switch (expression->GetReturnType()) {
            case DataType::TinyInt:
            case DataType::SmallInt:
            case DataType::Int:
            case DataType::BigInt:
            case DataType::Bool:
            case DataType::DateTime:
                return CostEstimator::INTEGER_COMPARISON_COST;
            case DataType::Decimal:
                return CostEstimator::DECIMAL_COMPARISON_COST;
            case DataType::String:
            case DataType::UnicodeString:
            case DataType::Guid:
                return CostEstimator::STRING_COMPARISON_COST;
            default:
                return CostEstimator::DECIMAL_COMPARISON_COST;
        }
    }

    double CostEstimator::InterpolateBucket(const Headers::ColumnHistograms& bucket, const Value& value){
        const auto minInterpolated = bucket.rangeStart.Interpolate();
        const auto maxInterpolated = bucket.rangeEnd.Interpolate();
        const auto valueInterpolated = value.Interpolate();

        const auto total = maxInterpolated - minInterpolated;
        if (total <= 0.0)
            return 0.5;

        const auto position = (valueInterpolated - minInterpolated) / total;
        return std::clamp(
            static_cast<double>(position),
            0.0,
            1.0
        );
    }

    int CostEstimator::FindBucketForValue(
        const std::vector<Headers::ColumnHistograms>& histograms,
        const Value& value
    ){
        for (int i = 0;i < histograms.size(); i++){
            const auto& histogram = histograms[i];

            if ((value < histogram.rangeEnd).AsBool())
                return i;
        }

        return -1;
    }

    double CostEstimator::EstimateEqualSelectivityByHistograms(
        const SeekRange& range,
        const Headers::ColumnHistograms& histogram
    ){
        auto selectivity = CostEstimator::InterpolateBucket(histogram, range.start);

        selectivity /= static_cast<double>(histogram.rowCount);

        return selectivity;
    }

    double CostEstimator::EstimateRangeStartSelectivityByHistograms(
        const Headers::ColumnHistograms& histogram,
        const HistogramSelectivityEstimate& info
    ){
        const auto fraction = CostEstimator::InterpolateBucket(histogram, *info.value);

        const auto nonTouchedBucketRows = info.bucketSize * fraction;

        return (static_cast<double>(info.totalTableRows) - nonTouchedBucketRows - info.previousRows) / static_cast<double>(info.totalTableRows);
    }

    double CostEstimator::EstimateRangeEndSelectivityByHistograms(
        const Headers::ColumnHistograms& histogram,
        const HistogramSelectivityEstimate& info
    ){
        const auto fraction = CostEstimator::InterpolateBucket(histogram, *info.value);

        const auto inBucketTouchedRows = info.bucketSize * fraction;

        return (inBucketTouchedRows + info.previousRows) / static_cast<double>(info.totalTableRows);
    }

    double CostEstimator::EstimateRangeSelectivityByHistograms(
        const Headers::ColumnHistograms& startHistogram,
        const HistogramSelectivityEstimate& startInfo,
        const Headers::ColumnHistograms& endHistogram,
        const HistogramSelectivityEstimate& endInfo
    ){
        const auto fractionStart = CostEstimator::InterpolateBucket(startHistogram, *startInfo.value);
        const auto rowsBeforeStart = startInfo.previousRows + (startInfo.bucketSize * fractionStart);

        const auto fractionEnd = CostEstimator::InterpolateBucket(endHistogram, *endInfo.value);
        const auto rowsBeforeEnd = endInfo.previousRows + (endInfo.bucketSize * fractionEnd);

        const auto selectivity = (startInfo.totalTableRows - rowsBeforeStart - rowsBeforeEnd) / static_cast<double>(startInfo.totalTableRows);

        return std::max(0.0, selectivity);
    }

    double CostEstimator::EstimateSelectivityByHistograms(
        const QueryContext* context,
        const SeekRange& range,
        const Headers::TableStatistics& tableStats,
        const Headers::ColumnStatistics& columnStats
    ){
        static const auto& catalog = CoreEngine::SystemCatalog::Get();
        const auto histograms = catalog.SelectColumnHistogramsByColumnId(
            context->_context.GetAllocator(),
            tableStats.tableId,
            columnStats.columnId
        );

        if (histograms.empty())
            return CostEstimator::EstimateSelectivityForSmallTable(range, columnStats);

        const auto hasStart = range.HasStart();
        const auto hasEnd = range.HasEnd();

        const auto averageRowsPerBucket = static_cast<double>(tableStats.rowCount) / static_cast<double>(NUMBER_OF_HISTOGRAM_BUCKETS);

        //is equality
        if (!range.hasRange)
            return static_cast<double>(1.0 / static_cast<long double>(columnStats.distinctCount));

        if (hasStart && hasEnd){
            const auto startBucketIndex = CostEstimator::FindBucketForValue(histograms, range.start);
            const auto endBucketIndex = CostEstimator::FindBucketForValue(histograms, range.end);

            if (startBucketIndex == -1 || endBucketIndex == -1)
                return 1.0;

            const auto& startBucket = histograms[startBucketIndex];
            const auto& endBucket = histograms[endBucketIndex];

            auto startInfo = HistogramSelectivityEstimate();
            startInfo.bucketIndex = startBucketIndex;
            startInfo.bucketSize = static_cast<int>(averageRowsPerBucket);
            startInfo.totalTableRows = tableStats.rowCount;
            startInfo.value = &range.start;
            startInfo.CalculatePreviousRows();

            auto endInfo = HistogramSelectivityEstimate();
            endInfo.bucketIndex = endBucketIndex;
            endInfo.bucketSize = static_cast<int>(averageRowsPerBucket);
            endInfo.totalTableRows = tableStats.rowCount;
            endInfo.value = &range.end;
            endInfo.CalculatePreviousRows();

            return CostEstimator::EstimateRangeSelectivityByHistograms(startBucket, startInfo, endBucket, endInfo);
        }

        if (hasStart){
            const auto bucketIndex = CostEstimator::FindBucketForValue(histograms, range.start);

            const auto& bucket = histograms[bucketIndex];


            auto info = HistogramSelectivityEstimate();
            info.bucketIndex = bucketIndex;
            info.bucketSize = static_cast<int>(averageRowsPerBucket);
            info.totalTableRows = tableStats.rowCount;
            info.value = &range.start;
            info.CalculatePreviousRows();

            return CostEstimator::EstimateRangeStartSelectivityByHistograms(bucket, info);
        }

        if (hasEnd) {
            const auto bucketIndex = CostEstimator::FindBucketForValue(histograms, range.end);

            const auto& bucket = histograms[bucketIndex];

            auto info = HistogramSelectivityEstimate();
            info.bucketIndex = bucketIndex;
            info.bucketSize = static_cast<int>(averageRowsPerBucket);
            info.totalTableRows = tableStats.rowCount;
            info.value = &range.end;
            info.CalculatePreviousRows();

            return CostEstimator::EstimateRangeEndSelectivityByHistograms(bucket, info);
        }

        return 1.0;
    }

    double CostEstimator::EstimateSelectivityForSmallTable(
        const SeekRange& range,
        const Headers::ColumnStatistics& columnStats
    ){
        if (!range.hasRange)
            return static_cast<double>(1.0 / static_cast<long double>(columnStats.distinctCount));

        const auto hasStart = range.HasStart();
        const auto hasEnd = range.HasEnd();

        //is equality
        if (!range.hasRange)
            return static_cast<double>(1.0 / static_cast<long double>(columnStats.distinctCount));

        const auto minInterpolated = columnStats.min.Interpolate();
        const auto maxInterpolated = columnStats.max.Interpolate();
        const auto totalRange = maxInterpolated - minInterpolated;

        //fallback for edge cases (min = max)
        if (totalRange <= 0.0)
            return 0.5;

        if (hasStart && hasEnd){
            const auto startInterpolated = range.start.Interpolate();
            const auto endInterpolated = range.end.Interpolate();

            const auto rangeSize = endInterpolated - startInterpolated;
            return std::clamp(static_cast<double>(rangeSize / totalRange), 0.0, 1.0);
        }

        if (hasStart){
            const auto startInterpolated = range.start.Interpolate();
            const auto touchedRange = maxInterpolated - startInterpolated;

            return std::clamp(static_cast<double>(touchedRange / totalRange), 0.0, 1.0);
        }

        if (hasEnd) {
            const auto endInterpolated = range.end.Interpolate();
            const auto touchedRange = endInterpolated - minInterpolated;

            return std::clamp(static_cast<double>(touchedRange / totalRange), 0.0, 1.0);
        }

        return 1.0;
    }

    double CostEstimator::EstimateSelectivity(
        const QueryContext* context,
        const SeekRange& range,
        const Headers::ColumnStatistics& columnStats,
        const Headers::TableStatistics& tableStats
    ){
        if (tableStats.rowCount <= PipelineConstants::SMALL_TABLE)
            return CostEstimator::EstimateSelectivityForSmallTable(range, columnStats);

        //todo pass column type here.
        return CostEstimator::EstimateSelectivityByHistograms(
            context,
            range,
            tableStats,
            columnStats
        );
    }

    void CostEstimator::EstimateIndexCost(
        const QueryContext* context,
        IndexCandidate& candidate,
        const Headers::TableStatistics& tableStats
    ){
        //if empty default to full scan
        if (candidate.analyzeInfo.empty()){
            candidate.estimatedCost = 1.0;
            return;
        }

        //get indexStats and account for the depth of the tree in the cost
        const auto indexStats = CoreEngine::StatisticsManager::Get().GetIndexStatistics(candidate.header->id);

        //TODO fix make sure index stats are available and cached by better key
        const Headers::IndexStatistics* indexStatistic = nullptr;
        for (const auto& stats : indexStats){
            if (stats.indexId != candidate.header->id)
                continue;

            indexStatistic = &stats;
            break;
        }

        if (indexStatistic == nullptr){
            candidate.estimatedCost = 1.0;
            return;
        }

        double combinedSelectivity = 1.0;
        for (const auto& info : candidate.analyzeInfo){
            const auto& columnStats = CoreEngine::StatisticsManager::Get().GetColumnStatistics(tableStats.tableId, info.columnId);

            const auto selectivity = CostEstimator::EstimateSelectivity(context, info.range, columnStats, tableStats);

            combinedSelectivity *= selectivity;
        }

        const auto estimatedRows = static_cast<double>(tableStats.rowCount) * combinedSelectivity;

        auto cost = indexStatistic->depth * PipelineConstants::SEQUENTIAL_PAGE_COST;

        cost += estimatedRows * PipelineConstants::SEQUENTIAL_PAGE_COST;

        if (!candidate.header->isClustered)
            cost += estimatedRows * PipelineConstants::RANDOM_PAGE_COST;

        cost += estimatedRows * PipelineConstants::CPU_COST_PER_ROW;

        candidate.estimatedCost = cost;
    }

    double CostEstimator::EstimateFilterCost(
        const Expressions::Expression* filterExpression,
        const BigInt inputRows,
        const double selectivity
    ){
        const auto expressionComplexity = CostEstimator::EstimateExpressionComplexity(filterExpression);

        const auto cpuCost = static_cast<double>(expressionComplexity) * PipelineConstants::CPU_COST_PER_ROW * static_cast<double>(inputRows);

        const auto ioCost = static_cast<double>(inputRows) * selectivity;

        return cpuCost + ioCost;
    }

    double CostEstimator::EstimateProjectionCost(
        const std::vector<Expressions::Expression*>& projections,
        const BigInt inputRows
    ) {
        Int totalExpressionComplexity = 0;

        for (const auto& expression : projections)
            totalExpressionComplexity += CostEstimator::EstimateExpressionComplexity(expression);

        return static_cast<double>(totalExpressionComplexity) * PipelineConstants::CPU_COST_PER_ROW * static_cast<double>(inputRows);
    }

    double CostEstimator::EstimateSortCost(
        const BigInt inputRows,
        const std::vector<Expressions::Expression*>& sortExpressions
    ){
        if (inputRows <= 1) return 0.0;

        const auto castRows = static_cast<double>(inputRows);

        const auto comparison = castRows * std::log2(castRows);

        Int totalExpressionComplexity = 0;
        for (const auto& expression : sortExpressions){
            totalExpressionComplexity += CostEstimator::EstimateExpressionComplexity(expression);
            totalExpressionComplexity += CostEstimator::EstimateExpressionComparisonCost(expression);
        }

        const auto sortCost = static_cast<double>(totalExpressionComplexity) * comparison * PipelineConstants::CPU_COST_PER_COMPARISON;

        const auto materializationCost = castRows * static_cast<double>(sortExpressions.size()) * PipelineConstants::CPU_COST_PER_ROW;

        return sortCost + materializationCost;
    }
}
