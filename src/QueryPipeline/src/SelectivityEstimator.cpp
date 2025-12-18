#include "../include/SelectivityEstimator.h"

#include "Optimizer.h"
#include "../../Systemic/include/Headers.h"
#include "SystemDatabases/SystemCatalog.h"

namespace QueryPipeline{
    SelectivityEstimator::HistogramSelectivityEstimate::HistogramSelectivityEstimate(){
        this->value = nullptr;
        this->bucketIndex = 0;
        this->bucketSize = 0;
        this->previousRows = 0;
        this->totalRows = 0;
    }

    void SelectivityEstimator::HistogramSelectivityEstimate::CalculatePreviousRows(){
        this->previousRows = this->bucketIndex > 0
            ? this->bucketSize * (this->bucketIndex - 1)
            : 0;
    }

    double SelectivityEstimator::InterpolateBucket(const Headers::ColumnHistograms& bucket, const Value& value){
        const auto minInterpolated = bucket.rangeStart.Interpolate();
        const auto maxInterpolated = bucket.rangeEnd.Interpolate();
        const auto valueInterpolated = value.Interpolate();

        const auto total = maxInterpolated - minInterpolated;
        if (total <= 0.0)
            return 0.5;

        const auto position = (valueInterpolated - minInterpolated) / total;
        return std::clamp(
            static_cast<double>(position / total),
            0.0,
            1.0
        );
    }

    int SelectivityEstimator::FindBucketForValue(
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

    double SelectivityEstimator::EstimateEqualSelectivityByHistograms(
        const SeekRange& range,
        const Headers::ColumnHistograms& histogram
    ){
        auto estimatedRows = SelectivityEstimator::InterpolateBucket(histogram, range.start);

        estimatedRows /= static_cast<double>(histogram.rowCount);

        return estimatedRows;
    }

    double SelectivityEstimator::EstimateRangeStartSelectivityByHistograms(
        const Headers::ColumnHistograms& histogram,
        const HistogramSelectivityEstimate& info
    ){
        const auto fraction = SelectivityEstimator::InterpolateBucket(histogram, *info.value);

        const auto inBucketTouchedRows = info.bucketSize * (1.0 - fraction);

        return static_cast<int>(inBucketTouchedRows + (info.previousRows - info.totalRows) / static_cast<double>(info.totalRows));
    }

    double SelectivityEstimator::EstimateRangeEndSelectivityByHistograms(
        const Headers::ColumnHistograms& histogram,
        const HistogramSelectivityEstimate& info
    ){
        const auto fraction = SelectivityEstimator::InterpolateBucket(histogram, *info.value);

        const auto inBucketTouchedRows = info.bucketSize * fraction;

        return static_cast<int>(inBucketTouchedRows + info.previousRows / static_cast<double>(info.totalRows));
    }

    double SelectivityEstimator::EstimateRangeSelectivityByHistograms(
        const Headers::ColumnHistograms& startHistogram,
        const HistogramSelectivityEstimate& startInfo,
        const Headers::ColumnHistograms& endHistogram,
        const HistogramSelectivityEstimate& endInfo
    ){
        const auto rangeStartEstimatedRows = SelectivityEstimator::EstimateRangeStartSelectivityByHistograms(startHistogram, startInfo);

        const auto rangeEndEstimatedRows = SelectivityEstimator::EstimateRangeEndSelectivityByHistograms(endHistogram, endInfo);

        return rangeEndEstimatedRows - rangeStartEstimatedRows;
    }

    double SelectivityEstimator::EstimateSelectivityByHistograms(
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

        if (hasStart && hasEnd){
            const auto startBucketIndex = SelectivityEstimator::FindBucketForValue(histograms, range.start);
            const auto endBucketIndex = SelectivityEstimator::FindBucketForValue(histograms, range.end);

            if (startBucketIndex == -1 || endBucketIndex == -1)
                return 1.0;

            const auto& startBucket = histograms[startBucketIndex];
            const auto& endBucket = histograms[endBucketIndex];

            auto startInfo = HistogramSelectivityEstimate();
            startInfo.bucketIndex = startBucketIndex;
            startInfo.bucketSize = averageRowsPerBucket;
            startInfo.totalRows = tableStats.rowCount;
            startInfo.value = &range.start;
            startInfo.CalculatePreviousRows();

            auto endInfo = HistogramSelectivityEstimate();
            endInfo.bucketIndex = endBucketIndex;
            endInfo.bucketSize = averageRowsPerBucket;
            endInfo.totalRows = tableStats.rowCount;
            endInfo.value = &range.end;
            endInfo.CalculatePreviousRows();

            return SelectivityEstimator::EstimateRangeSelectivityByHistograms(startBucket, startInfo, endBucket, endInfo);
        }

        if (hasStart){
            const auto bucketIndex = SelectivityEstimator::FindBucketForValue(histograms, range.start);

            const auto& bucket = histograms[bucketIndex];


            auto info = HistogramSelectivityEstimate();
            info.bucketIndex = bucketIndex;
            info.bucketSize = averageRowsPerBucket;
            info.totalRows = tableStats.rowCount;
            info.value = &range.start;
            info.CalculatePreviousRows();

            return SelectivityEstimator::EstimateRangeStartSelectivityByHistograms(bucket, info);
        }

        if (hasEnd) {
            const auto bucketIndex = SelectivityEstimator::FindBucketForValue(histograms, range.end);

            const auto& bucket = histograms[bucketIndex];

            auto info = HistogramSelectivityEstimate();
            info.bucketIndex = bucketIndex;
            info.bucketSize = averageRowsPerBucket;
            info.totalRows = tableStats.rowCount;
            info.value = &range.start;
            info.CalculatePreviousRows();

            return SelectivityEstimator::EstimateRangeEndSelectivityByHistograms(bucket, info);
        }

        const auto bucketIndex = SelectivityEstimator::FindBucketForValue(histograms, range.start);

        const auto& bucket = histograms[bucketIndex];

        return SelectivityEstimator::EstimateEqualSelectivityByHistograms(range, bucket);
    }

    double SelectivityEstimator::EstimateSelectivity(
        const std::vector<IndexSeekColumnAnalysisResults>& analyzeResults,
        const Headers::IndexHeader& index,
        const Headers::TableStatistics& tableStats
    ){
        return 0.0;
    }

    double SelectivityEstimator::EstimateRangeSelectivity(
        const SeekRange& range,
        const Headers::ColumnStatistics& columnStats
    ){
        return 0.0;
    }
}
