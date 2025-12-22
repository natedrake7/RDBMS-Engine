#include "../include/CostEstimator.h"

#include <cmath>
#include "Optimizer.h"
#include "PipelineConstants.h"
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

            if ((value < histogram.rangeEnd).GetBool())
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

        return (info.totalTableRows - nonTouchedBucketRows - info.previousRows) / static_cast<double>(info.totalTableRows);
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
        const SeekRange& range,
        const Headers::TableStatistics& tableStats,
        const Headers::ColumnStatistics& columnStats
    ){
        static const auto& catalog = DatabaseEngine::SystemCatalog::Get();
        const auto histograms = catalog.SelectColumnHistogramsByColumnId(tableStats.tableId, columnStats.columnId);

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
        const SeekRange& range,
        const Headers::ColumnStatistics& columnStats,
        const Headers::TableStatistics& tableStats
    ){
        if (tableStats.rowCount <= PipelineConstants::SMALL_TABLE)
            return CostEstimator::EstimateSelectivityForSmallTable(range, columnStats);

        //todo pass column type here.
        return CostEstimator::EstimateSelectivityByHistograms(
            range,
            tableStats,
            columnStats
        );
    }

    double CostEstimator::EstimateIndexCost(
        const Headers::IndexHeader& indexHeader,
        const std::vector<IndexSeekColumnAnalysisResults>& analyzeResults,
        const Headers::TableStatistics& tableStats
    ){

        //if empty default to full scan
        if (analyzeResults.empty())
            return 1.0;

        //get indexStats and account for the depth of the tree in the cost
        const auto indexStats = DatabaseEngine::StatisticsManager::Get().GetIndexStatistics(indexHeader.id);

        //TODO fix make sure index stats are available and cached by better key
        const Headers::IndexStatistics* indexStatistic = nullptr;
        for (const auto& stats : indexStats){
            if (stats.indexId != indexHeader.id)
                continue;

            indexStatistic = &stats;
            break;
        }

        if (indexStatistic == nullptr)
            return 1.0;

        double combinedSelectivity = 1.0;

        for (const auto& info : analyzeResults){
            const auto& columnStats = DatabaseEngine::StatisticsManager::Get().GetColumnStatistics(tableStats.tableId, info.columnId);

            const auto selectivity = CostEstimator::EstimateSelectivity(info.range, columnStats, tableStats);

            combinedSelectivity *= selectivity;
        }

        const auto estimatedRows = static_cast<double>(tableStats.rowCount) * combinedSelectivity;

        auto cost = indexStatistic->depth * PipelineConstants::SEQUENTIAL_PAGE_COST;

        cost += estimatedRows * PipelineConstants::SEQUENTIAL_PAGE_COST;

        if (!indexHeader.isClustered)
            cost += estimatedRows * PipelineConstants::RANDOM_PAGE_COST;

        cost += estimatedRows * PipelineConstants::CPU_COST_PER_ROW;










        return cost;
    }
}
