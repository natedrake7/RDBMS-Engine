#include "../../include/Schedulers/StatisticsScheduler.h"

#include "Database.h"
#include "BufferPool/StorageManager.h"
#include "DataStructures/Dictionary.h"
#include "Guards/ReaderGuard.h"
#include "Managers/StatisticsManager.h"
#include "SystemDatabases/SystemCatalog.h"

#include <cmath>
#include <iostream>

#include "DataStorage/Table.h"
#include "Memory/Allocator.h"

namespace CoreEngine {
    std::vector<Database *> StatisticsScheduler::GetDatabases()const {
        MultiThreading::ReaderGuard lock(this->latch);
        return this->databasesDictionary->ToVector();
    }

    int StatisticsScheduler::EstimateRowsPerPage(const Int totalRows, const Int allocatedPagesPerExtent) {
        return (allocatedPagesPerExtent == 0)
            ? 0
            : static_cast<int>(std::ceil(static_cast<float>(totalRows) / static_cast<float>(allocatedPagesPerExtent)));
    }

    int StatisticsScheduler::EstimateAllocatedPagesPerExtent(const Int allocatedPagesPerExtent, const Int numberOfExtents) {
        return (numberOfExtents == 0)
            ? 0
            : static_cast<int>(std::ceil(static_cast<float>(allocatedPagesPerExtent) / static_cast<float>(numberOfExtents)));
    }

    bool StatisticsScheduler::GenerateColumnHistograms(
        const SortedDictionary<Value, BigInt, ValueComparator>& sortedValues,
        DataStructures::PolymorphicArray<Headers::ColumnHistograms>& histograms,
        const Headers::ColumnStatistics& columnStatistics,
        const BigInt totalRows
    ){
        if (totalRows < 10000)
            return false;

        const auto rowsPerBucket = static_cast<int>(std::ceil(static_cast<float>(totalRows) / static_cast<float>(NUMBER_OF_HISTOGRAM_BUCKETS)));
        int currentBucketRows = 0;

        Value bucketStart = sortedValues.begin()->first;
        const auto& lastValue = sortedValues.rbegin()->first;

        int counter = 0;
        int distinctCountPerBucket = 0;
        for (const auto& [value, freq] : sortedValues) {
            currentBucketRows += freq;
            distinctCountPerBucket++;

            if (currentBucketRows < rowsPerBucket && value != lastValue)
                continue;

            //insert
            if (histograms.Size() <= counter){
                auto histogram = Headers::ColumnHistograms(
                columnStatistics.columnId,
                bucketStart,
                value,
                currentBucketRows,
                distinctCountPerBucket
                );

                histograms.Push(std::move(histogram));
            } //or update
            else{
                auto& histogram = histograms[counter];

                histogram.rangeStart = bucketStart;
                histogram.rangeEnd = value;
                histogram.rowCount = currentBucketRows;
                histogram.distinctCount = currentBucketRows;
            }

            counter++;
            distinctCountPerBucket = 0;
            bucketStart = value;
            currentBucketRows = 0;
        }

        return true;
    }

    void StatisticsScheduler::UpdateDatabaseStatistics(const Database *database)const {
        for (const auto& table : database->GetTables()) {
            const auto cacheStats = this->statsManager->GetTableStatistics(table->GetTableId());

            auto currentTime = DataTypes::DateTime::Now();
            currentTime.AddMinutes(-1); // Update stats if older than 10-minutes test for production grade this should be dynamic

            if (
                currentTime <= cacheStats.lastModified
                && cacheStats.tableId != INVALID_TABLE_ID
            ) continue;

            std::cout << "Updating statistics for table: " << cacheStats.tableId << std::endl;
            this->UpdateTableStatistics(table);
        }
    }

    void StatisticsScheduler::UpdateTableStatistics(StorageTypes::Table *table)const {
        const auto iamPageId = table->GetAllocationPageId();

        if (iamPageId == INVALID_PAGE_ID) return;

        auto tableStatistics = Headers::TableStatistics(table->GetTableId());

        Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>> sortedValues;
        Dictionary<Int, DataStructures::PolymorphicArray<Headers::ColumnHistograms>> columnHistogramsDictionary;

        const auto _baseContext = ExecutionContext::BaseContext();

        DataStructures::PolymorphicArray<Headers::ColumnStatistics> columnStatistics(_baseContext.GetAllocator());
        for (const auto& column : table->GetColumns()) {
            const auto& columnId = column->GetColumnId();

            columnStatistics.Push(
                Headers::ColumnStatistics{
                    .columnId = columnId,
                    .distinctCount = 0,
                    .min = Value::Null(),
                    .max = Value::Null(),
                    .nullCount = 0,
                }
            );

            sortedValues.Add(columnId, {});
            auto histograms = this->catalog->SelectColumnHistogramsByColumnId(_baseContext.GetAllocator(), tableStatistics.tableId, columnId);
            columnHistogramsDictionary.Add(columnId, std::move(histograms));
        }

        auto indexStatistics = StatisticsManager::Get().GetIndexStatistics(tableStatistics.tableId);

        bool clusteredIndexUpdated = false;
        if (indexStatistics.Empty()){
            const auto indexes = SystemCatalog::Get().SelectIndexes(_baseContext.GetAllocator(), tableStatistics.tableId);

            for (const auto& index : indexes){
                auto indexStats = Headers::IndexStatistics(tableStatistics.tableId, index.id);

                const auto result = StatisticsScheduler::UpdateIndexStatistics(
                    table,
                    indexStats,
                    tableStatistics,
                    columnStatistics,
                    sortedValues
                );

                if (result)
                    clusteredIndexUpdated = true;
            }
        }
        else{
            for (auto& indexStats : indexStatistics) {
                const auto result = StatisticsScheduler::UpdateIndexStatistics(
                    table,
                    indexStats,
                    tableStatistics,
                    columnStatistics,
                    sortedValues
                );

                if (result)
                    clusteredIndexUpdated = true;
            }
        }

        if (!clusteredIndexUpdated){
            StatisticsScheduler::UpdateHeapStatistics(
                table,
                iamPageId,
                tableStatistics,
                columnStatistics,
                sortedValues
            );
        }

        for (const auto& column : columnStatistics){
            auto& histograms = columnHistogramsDictionary[column.columnId];

            if (!histograms.HasAllocator())
                histograms.SetAllocator(_baseContext.GetAllocator());

            StatisticsScheduler::GenerateColumnHistograms(
                sortedValues[column.columnId],
                histograms,
                column,
                tableStatistics.rowCount
            );
        }

        //Update catalog
        this->UpdateCache(tableStatistics, columnStatistics, indexStatistics);
        this->UpdateCatalogStatistics(
            _baseContext,
            tableStatistics,
            columnStatistics,
            indexStatistics,
            columnHistogramsDictionary
        );
    }

    bool StatisticsScheduler::UpdateIndexStatistics(
        StorageTypes::Table *table,
        Headers::IndexStatistics& indexStatistics,
        Headers::TableStatistics& tableStatistics,
        DataStructures::PolymorphicArray<Headers::ColumnStatistics>& columnStatistics,
        Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>>& sortedValues
    )  {
        indexStatistics.Reset();

        if (table->IsClustered()) {
            const auto* tree = table->GetClusteredIndexedTree();
            tree->CalculateIndexStatistics(
                indexStatistics,
                tableStatistics,
                columnStatistics,
                sortedValues
            );

            return true;
        }

        //TODO
        //get non-clustered trees

        return false;
    }

    void StatisticsScheduler::UpdateHeapStatistics(
        const StorageTypes::Table *table,
        const page_id_t iamPageId,
        Headers::TableStatistics &tableStatistics,
        DataStructures::PolymorphicArray<Headers::ColumnStatistics> &columnStatistics,
        Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>>& sortedValues
    ) {

        const auto dataKey = table->GetDataFileKey();
        const auto systemKey = table->GetSystemFileKey();

        const auto iamPage = Storage::StorageManager::Get().GetPage<Pages::AllocationPageView>(
            dataKey,
            iamPageId
        );

        DataStructures::PolymorphicArray<extent_id_t> extents;
        iamPage.GetAllocatedExtents(&extents, 0);

        int estimatedRowCount = 1000;

        int sampleRowCount = 0;
        int averageRowsPerPage = 0;
        int allocatedPagesPerExtent = 0;

        const Memory::Allocator allocator;
        for (const auto& extentId : extents) {
            const page_id_t extentFirstPageId = extentId * Constants::EXTENT_SIZE;

            const auto pageFreeSpacePage = CoreEngine::Database::GetAssociatedPfsPage(
                systemKey,
                extentFirstPageId
            );

            const auto firstDataPageId = (iamPage.PageId() != extentFirstPageId)
                       ? extentFirstPageId
                       : extentFirstPageId + 1;

            bool successfulPfsLock = false;
            auto pfsLatch = MultiThreading::ReaderGuard::TryLock(&pageFreeSpacePage.Latch(), successfulPfsLock);

            for (page_id_t extentPageId = firstDataPageId; extentPageId < extentFirstPageId + Constants::EXTENT_SIZE; extentPageId++){
                if (successfulPfsLock
                    && pageFreeSpacePage.GetPageType(extentPageId) != Constants::PageType::DATA
                ) break;

                auto page = Storage::StorageManager::Get().GetPage<Pages::PageView>(
                    dataKey,
                    extentPageId
                );

                bool successfulLock = false;
                auto lock = MultiThreading::ReaderGuard::TryLock(&page.Latch(), successfulLock);
                if (!successfulLock)
                    continue;

                const auto pageSize = page.PageSize();
                const auto fallBackPageType = page.GetPageType();
                if (pageSize == 0 || (fallBackPageType != Constants::PageType::INDEX && fallBackPageType != Constants::PageType::DATA))
                    continue;

                averageRowsPerPage += pageSize;
                allocatedPagesPerExtent++;

                for (int i = 0;i < pageSize;i++){
                    // const auto* row = page.PeekRow(&allocator, i, 0);
                    // auto materializedRow = row->Materialize(&allocator);

                    // tableStatistics.averageRowSize += row->Size();
                    // sampleRowCount++;
                    //
                    // for (int j = 0; j < columnStatistics.Size(); j++){
                    //     auto& columnStats = columnStatistics[j];
                    //     StatisticsScheduler::UpdateColumnStatistics(
                    //         columnStats,
                    //            materializedRow.GetColumnReferenceAt(j),
                    //         sortedValues[columnStats.columnId]
                    //     );
                    // }
                }
            }
    }

    averageRowsPerPage = StatisticsScheduler::EstimateRowsPerPage(averageRowsPerPage, allocatedPagesPerExtent);
    const auto numberOfExtents = extents.Size();

    const auto averageAllocatedPagesPerExtent = StatisticsScheduler::EstimateAllocatedPagesPerExtent(
        allocatedPagesPerExtent,
        numberOfExtents
    );

    tableStatistics.averageRowSize = std::ceil(
        static_cast<float>(tableStatistics.averageRowSize) / static_cast<float>(sampleRowCount)
    );
    tableStatistics.rowCount = numberOfExtents * averageAllocatedPagesPerExtent * averageRowsPerPage;
    tableStatistics.pageCount = numberOfExtents * averageAllocatedPagesPerExtent;
    tableStatistics.lastModified = DataTypes::DateTime::Now();
    }

    void StatisticsScheduler::UpdateCatalogStatistics(
        const ExecutionContext& baseContext,
        const Headers::TableStatistics &tableStatistics,
        const DataStructures::PolymorphicArray<Headers::ColumnStatistics> &columnStatistics,
        const DataStructures::PolymorphicArray<Headers::IndexStatistics>& indexStatistics,
        const Dictionary<Int, DataStructures::PolymorphicArray<Headers::ColumnHistograms>> &columnHistogramsDictionary
    )const {

        this->catalog->UpdateTableStatisticsById(
            baseContext.GetAllocator(),
            tableStatistics.tableId,
            tableStatistics.rowCount,
            tableStatistics.averageRowSize,
            tableStatistics.pageCount
        );

        for (const auto& colStats : columnStatistics) {
            this->catalog->UpdateColumnStatisticsById(
                baseContext.GetAllocator(),
                colStats.columnId,
                colStats.distinctCount,
                colStats.nullCount,
                colStats.min,
                colStats.max
            );
        }

        for (const auto& indexStats : indexStatistics) {
            this->catalog->UpdateIndexStatisticsById(
                baseContext.GetAllocator(),
                tableStatistics.tableId,
                indexStats.indexId,
                indexStats.leafPages,
                indexStats.depth,
                indexStats.averageFragmentation
            );
        }

        if (tableStatistics.rowCount < 10000)
            return;

        for (const auto& [columnId, histograms] : columnHistogramsDictionary){
            for (const auto& histogram : histograms){
                auto res = (histogram.histogramId == INVALID_HISTOGRAM_ID)
                    ? this->catalog->InsertColumnHistogramsToMasterDb(
                        baseContext,
                        columnId,
                        histogram.rangeStart,
                        histogram.rangeEnd,
                        histogram.rowCount,
                        histogram.distinctCount
                    )
                    : this->catalog->UpdateHistogramBucket(
                        baseContext.GetAllocator(),
                        columnId, histogram.histogramId,
                        histogram.rangeStart,
                        histogram.rangeEnd,
                        histogram.rowCount,
                        histogram.distinctCount
                    );
            }
        }
    }

    void StatisticsScheduler::UpdateCache(
        const Headers::TableStatistics &tableStatistics,
        const DataStructures::PolymorphicArray<Headers::ColumnStatistics> &columnStatistics,
        const DataStructures::PolymorphicArray<Headers::IndexStatistics> &indexStatistics
    ) const {
        this->statsManager->Update(tableStatistics, columnStatistics, indexStatistics);
    }

    StatisticsScheduler::StatisticsScheduler(
        const Dictionary<Int, Database *> &databasesDictionary,
        MultiThreading::Mutex &latch
    ){
        this->databasesDictionary = &databasesDictionary;
        this->latch = &latch;
        this->catalog = &SystemCatalog::Get();
        this->statsManager = &StatisticsManager::Get();
    }

    void StatisticsScheduler::UpdateStatistics()const {
        for (const auto* database : this->GetDatabases())
            this->UpdateDatabaseStatistics(database);
    }

    void StatisticsScheduler::Start(
        const std::atomic<bool> &isServerRunning,
        const Dictionary<Int, Database*> &databasesDictionary,
        MultiThreading::Mutex &latch
    ){
        using namespace std::chrono_literals;
        const StatisticsScheduler scheduler(databasesDictionary, latch);

        std::cout << "Statistics Scheduler started." << std::endl;

        while (isServerRunning) {
            std::unique_lock lock(StatisticsScheduler::_mutex);
            StatisticsScheduler::_cv.wait_for(lock, 20000ms, [&]{ return !isServerRunning.load(); });

            if (!isServerRunning)
                break;

            scheduler.UpdateStatistics();
        }
    }

    void StatisticsScheduler::Stop(){
        StatisticsScheduler::_cv.notify_all();
    }

    void StatisticsScheduler::UpdateColumnStatistics(
        Headers::ColumnStatistics &columnStatistics,
        const Value &value,
        SortedDictionary<Value, BigInt, ValueComparator>& sortedValues
    ) {
        if (value.IsNull()) {
            columnStatistics.nullCount++;
            return;
        }

        // Update distinct count - simplistic approach
        columnStatistics.distinctCount++; // In real scenario, use a hash set or similar structure

        // Update min
        if (columnStatistics.min.IsNull() || value < columnStatistics.min)
            columnStatistics.min = value;

        // Update max
        if (columnStatistics.max.IsNull() || value > columnStatistics.max)
            columnStatistics.max = value;

        BigInt frequency = 0;
        if (!sortedValues.TryGetValue(value, frequency))
            sortedValues.Add(value, 1);
        else
            sortedValues.Update(value, frequency + 1);
    }

    std::mutex& StatisticsScheduler::Mutex(){
        return StatisticsScheduler::_mutex;
    }

    std::condition_variable& StatisticsScheduler::ConditionVariable(){
        return StatisticsScheduler::_cv;
    }
}
