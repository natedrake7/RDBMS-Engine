#pragma once

#include <cstdint>
#include <vector>

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
    struct SeekRange;
    struct IndexSeekColumnAnalysisResults;


    class CostEstimator final{
        struct HistogramSelectivityEstimate{
            int previousRows;
            int bucketSize;
            int64_t totalRows;
            int bucketIndex;
            const Value* value;

            HistogramSelectivityEstimate();
            void CalculatePreviousRows();
        };

        [[nodiscard]] static double InterpolateBucket(
            const Headers::ColumnHistograms& bucket,
            const Value& value
        );

        [[nodiscard]] static int FindBucketForValue(
            const std::vector<Headers::ColumnHistograms>& histograms,
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
            const SeekRange& range,
            const Headers::TableStatistics& tableStats,
            const Headers::ColumnStatistics& columnStats,
            const DataType& columnType
        );

        [[nodiscard]] static double EstimateSelectivityForSmallTable(
            const SeekRange& range,
            const Headers::TableStatistics& tableStats,
            const Headers::ColumnStatistics& columnStats
        );

        [[nodiscard]] static double EstimateSelectivity(
            const SeekRange& range,
            const Headers::ColumnStatistics& columnStats,
            const Headers::TableStatistics& tableStats
        );

        [[nodiscard]] static double EstimateRangeSelectivity(
            const SeekRange& range,
            const Headers::ColumnStatistics& columnStats
        );
    public:
        [[nodiscard]] static double EstimateCost(
            const std::vector<IndexSeekColumnAnalysisResults>& analyzeResults,
            const Headers::ColumnStatistics& columnStats,
            const Headers::TableStatistics& tableStats
        );
    };
}