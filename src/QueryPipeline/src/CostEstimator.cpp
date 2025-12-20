#include "../include/CostEstimator.h"

#include "Optimizer.h"
#include "PipelineConstants.h"
#include "../../Systemic/include/Headers.h"
#include "SystemDatabases/SystemCatalog.h"

namespace QueryPipeline{
    CostEstimator::HistogramSelectivityEstimate::HistogramSelectivityEstimate(){
        this->value = nullptr;
        this->bucketIndex = 0;
        this->bucketSize = 0;
        this->previousRows = 0;
        this->totalRows = 0;
    }

    void CostEstimator::HistogramSelectivityEstimate::CalculatePreviousRows(){
        this->previousRows = this->bucketIndex > 0
            ? this->bucketSize * (this->bucketIndex - 1)
            : 0;
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

        const auto inBucketTouchedRows = info.bucketSize * (1.0 - fraction);

        return inBucketTouchedRows + (info.totalRows - info.previousRows) / static_cast<double>(info.totalRows);
    }

    double CostEstimator::EstimateRangeEndSelectivityByHistograms(
        const Headers::ColumnHistograms& histogram,
        const HistogramSelectivityEstimate& info
    ){
        const auto fraction = CostEstimator::InterpolateBucket(histogram, *info.value);

        const auto inBucketTouchedRows = info.bucketSize * fraction;

        return (inBucketTouchedRows + info.previousRows) / static_cast<double>(info.totalRows);
    }

    double CostEstimator::EstimateRangeSelectivityByHistograms(
        const Headers::ColumnHistograms& startHistogram,
        const HistogramSelectivityEstimate& startInfo,
        const Headers::ColumnHistograms& endHistogram,
        const HistogramSelectivityEstimate& endInfo
    ){
        const auto rangeStartEstimatedRows = CostEstimator::EstimateRangeStartSelectivityByHistograms(startHistogram, startInfo);

        const auto rangeEndEstimatedRows = CostEstimator::EstimateRangeEndSelectivityByHistograms(endHistogram, endInfo);

        return rangeEndEstimatedRows - rangeStartEstimatedRows;
    }

    double CostEstimator::EstimateSelectivityByHistograms(
        const SeekRange& range,
        const Headers::TableStatistics& tableStats,
        const Headers::ColumnStatistics& columnStats,
        const DataType& columnType
    ){
        static const auto& catalog = DatabaseEngine::SystemCatalog::Get();
        const auto histograms = catalog.SelectColumnHistogramsByColumnId(columnStats.columnId, columnType);

        if (histograms.empty())
            return 1.0;

        const auto hasStart = range.HasStart();
        const auto hasEnd = range.HasEnd();

        const auto averageRowsPerBucket = static_cast<double>(tableStats.rowCount) / static_cast<double>(NUMBER_OF_HISTOGRAM_BUCKETS);

        //is equality
        if (!range.hasRange){
            return static_cast<double>(1.0 / static_cast<long double>(columnStats.distinctCount));
            //
            // const auto bucketIndex = CostEstimator::FindBucketForValue(histograms, range.start);
            //
            // const auto& bucket = histograms[bucketIndex];
            //
            // return CostEstimator::EstimateEqualSelectivityByHistograms(range, bucket);
        }

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
            startInfo.totalRows = tableStats.rowCount;
            startInfo.value = &range.start;
            startInfo.CalculatePreviousRows();

            auto endInfo = HistogramSelectivityEstimate();
            endInfo.bucketIndex = endBucketIndex;
            endInfo.bucketSize = static_cast<int>(averageRowsPerBucket);
            endInfo.totalRows = tableStats.rowCount;
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
            info.totalRows = tableStats.rowCount;
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
            info.totalRows = tableStats.rowCount;
            info.value = &range.end;
            info.CalculatePreviousRows();

            return CostEstimator::EstimateRangeEndSelectivityByHistograms(bucket, info);
        }

        return 1.0;
    }

    double CostEstimator::EstimateSelectivityForSmallTable(
        const SeekRange& range,
        const Headers::TableStatistics& tableStats,
        const Headers::ColumnStatistics& columnStats
    ){
        return 0.5;
    }

    double CostEstimator::EstimateSelectivity(
        const SeekRange& range,
        const Headers::ColumnStatistics& columnStats,
        const Headers::TableStatistics& tableStats
    ){
        if (tableStats.rowCount <= PipelineConstants::SMALL_TABLE)
            return CostEstimator::EstimateSelectivityForSmallTable(range, tableStats, columnStats);

        //todo pass column type here.
        return CostEstimator::EstimateSelectivityByHistograms(
            range,
            tableStats,
            columnStats,
            DataType::Int
        );
    }

    double CostEstimator::EstimateRangeSelectivity(
        const SeekRange& range,
        const Headers::ColumnStatistics& columnStats
    ){
        return 0.0;
    }

    double CostEstimator::EstimateCost(
        const std::vector<IndexSeekColumnAnalysisResults>& analyzeResults,
        const Headers::ColumnStatistics& columnStats,
        const Headers::TableStatistics& tableStats
    ){
        //if empty default to full scan
        if (analyzeResults.empty())
            return 1.0;

        const auto& firstIndexColumnResult = analyzeResults.front();
        const auto& range = firstIndexColumnResult.range;

        const auto selectivity = CostEstimator::EstimateSelectivity(range, columnStats, tableStats);

        const auto estimatedRows = static_cast<double>(tableStats.rowCount) * selectivity;

        auto cost = estimatedRows * PipelineConstants::CPU_COST;

        cost += estimatedRows * PipelineConstants::SEQUENTIAL_PAGE_COST;

        cost += estimatedRows * PipelineConstants::RANDOM_PAGE_COST;

        return cost;
    }
}
