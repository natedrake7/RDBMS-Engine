#include "../../include/Schedulers/StatisticsScheduler.h"

#include "Database.h"
#include "BufferPool/StorageManager.h"
#include "DataStructures/Dictionary.h"
#include "Guards/ReaderGuard.h"
#include "Managers/StatisticsManager.h"
#include "Pages/IndexAllocationMapPage.h"
#include "Pages/PageFreeSpacePage.h"
#include "SystemDatabases/SystemCatalog.h"

#include <cmath>
#include <cstdint>
#include <iostream>

namespace DatabaseEngine {
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
   std::vector<Headers::ColumnHistograms>& histograms,
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

    if (currentBucketRows < rowsPerBucket && (value != lastValue).AsBool())
     continue;

   //insert
    if (histograms.size() <= counter){
      auto histogram = Headers::ColumnHistograms(
          columnStatistics.columnId,
          bucketStart,
  value,
 currentBucketRows,
         distinctCountPerBucket
      );
      histograms.push_back(std::move(histogram));
    } //or update
    else
    {
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
     currentTime.AddMinutes(-60); // Update stats if older than 10 minutes test for production grade this should be dynamic

     if (currentTime <= cacheStats.lastModified)
      continue;

     std::cout << "Updating statistics for table: " << cacheStats.tableId << std::endl;

     this->UpdateTableStatistics(table, database->GetSystemFilename(), database->GetFileName());
   }
 }

 void StatisticsScheduler::UpdateTableStatistics(
  StorageTypes::Table *table,
  const std::string& systemFilename,
  const std::string& filename
 )const {
  const auto& iamPageId = table->GetIndexAllocationMapPageId();

  if (iamPageId == INVALID_PAGE_ID)
   return;

  auto tableStatistics = Headers::TableStatistics(table->GetTableId());

  Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>> sortedValues;
  Dictionary<Int, std::vector<Headers::ColumnHistograms>> columnHistogramsDictionary;;
  std::vector<Headers::ColumnStatistics> columnStatistics;

  for (const auto& column : table->GetColumns()) {
   const auto& columnId = column->GetColumnId();

   columnStatistics.emplace_back(
    Headers::ColumnStatistics{
     .columnId = columnId,
     .distinctCount = 0,
     .min = Value::Null(),
     .max = Value::Null(),
     .nullCount = 0,
   });

   sortedValues.Add(columnId, {});
   auto histograms = this->catalog->SelectColumnHistogramsByColumnId(tableStatistics.tableId, columnId);
   columnHistogramsDictionary.Add(columnId, std::move(histograms));
  }

  auto indexStatistics = StatisticsManager::Get().GetIndexStatistics(tableStatistics.tableId);

  bool clusteredIndexUpdated = false;
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

  if (!clusteredIndexUpdated)
   StatisticsScheduler::UpdateHeapStatistics(
    table,
    iamPageId,
    systemFilename,
    filename,
    tableStatistics,
    columnStatistics,
    sortedValues
   );

  for (const auto& column : columnStatistics){
    const auto& columnId = column.columnId;

    StatisticsScheduler::GenerateColumnHistograms(
      sortedValues[columnId],
      columnHistogramsDictionary[columnId],
      column,
      tableStatistics.rowCount
     );

  }

  //Update catalog
  this->UpdateCache(tableStatistics, columnStatistics, indexStatistics);
  this->UpdateCatalogStatistics(tableStatistics, columnStatistics, indexStatistics, columnHistogramsDictionary);
 }

 bool StatisticsScheduler::UpdateIndexStatistics(
  StorageTypes::Table *table,
  Headers::IndexStatistics& indexStatistics,
  Headers::TableStatistics& tableStatistics,
  std::vector<Headers::ColumnStatistics>& columnStatistics,
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
  //get non clustered trees

  return false;
 }

 void StatisticsScheduler::UpdateHeapStatistics(
  const StorageTypes::Table *table,
  const page_id_t iamPageId,
  const std::string& systemFilename,
  const std::string& filename,
  Headers::TableStatistics &tableStatistics,
  std::vector<Headers::ColumnStatistics> &columnStatistics,
  Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>>& sortedValues
 ) {

  const auto iamPage = Storage::StorageManager::Get().GetIndexAllocationMapPage(table->GetFileName(), iamPageId, table);

  std::vector<extent_id_t> extents;
  iamPage->GetAllocatedExtents(&extents, 0);

  int estimatedRowCount = 1000;

  int sampleRowCount = 0;
  int averageRowsPerPage = 0;
  int allocatedPagesPerExtent = 0;
  for (const auto& extentId : extents) {
   const page_id_t extentFirstPageId = extentId * EXTENT_SIZE;

   const auto pageFreeSpacePage = DatabaseEngine::Database::GetAssociatedPfsPage(systemFilename, extentFirstPageId);

   const auto firstDataPageId = (iamPage->PageId() != extentFirstPageId)
                               ? extentFirstPageId
                               : extentFirstPageId + 1;

   bool successfulPfsLock = false;
   auto pfsLatch = MultiThreading::ReaderGuard::TryLock(&pageFreeSpacePage->Latch(), successfulPfsLock);

   for (page_id_t extentPageId = firstDataPageId; extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++){
     if (successfulPfsLock
      && pageFreeSpacePage->GetPageType(extentPageId) != PageType::DATA)
      break;

    auto page = Storage::StorageManager::Get().GetPage(filename, extentPageId, table);

    bool successfulLock = false;
    auto lock = MultiThreading::ReaderGuard::TryLock(&page->Latch(), successfulLock);
    if (!successfulLock)
     continue;

    const auto pageSize = page->GetPageSize();
    const auto fallBackPageType = page->GetPageType();
    if (pageSize == 0 || (fallBackPageType != PageType::INDEX && fallBackPageType != PageType::DATA))
     continue;

    averageRowsPerPage += pageSize;
    allocatedPagesPerExtent++;

    for (int i = 0;i < pageSize;i++){
      // auto row = page->GetRow(table, i);
      //
      // tableStatistics.averageRowSize += row.TotalSize();
      // sampleRowCount++;
      //
      // for (int j = 0; j < columnStatistics.size(); j++){
      //  auto& columnStats = columnStatistics[j];
      //  const auto& value = row.GetColumnByIndex(j);
      //
      //  StatisticsScheduler::UpdateColumnStatistics(
      //   columnStats,
      //   value,
      //   sortedValues[columnStats.columnId]
      //  );
      // }
    }
   }
  }

  averageRowsPerPage = StatisticsScheduler::EstimateRowsPerPage(averageRowsPerPage, allocatedPagesPerExtent);
  const auto numberOfExtents = static_cast<int>(extents.size());

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
  const Headers::TableStatistics &tableStatistics,
  const std::vector<Headers::ColumnStatistics> &columnStatistics,
  const std::vector<Headers::IndexStatistics>& indexStatistics,
  const Dictionary<Int, std::vector<Headers::ColumnHistograms>> &columnHistogramsDictionary
 )const {

  this->catalog->UpdateTableStatisticsById(
    tableStatistics.tableId,
    tableStatistics.rowCount,
    tableStatistics.averageRowSize,
    tableStatistics.pageCount
  );

  for (const auto& colStats : columnStatistics) {
   this->catalog->UpdateColumnStatisticsById(
     colStats.columnId,
     colStats.distinctCount,
     colStats.nullCount,
     colStats.min,
     colStats.max
   );
  }

  for (const auto& indexStats : indexStatistics) {
   this->catalog->UpdateIndexStatisticsById(
     tableStatistics.tableId,
     indexStats.indexId,
     indexStats.leafPages,
     indexStats.depth,
     indexStats.averageFragmentation
   );
  }

  if (tableStatistics.rowCount < 10000)
   return;

  for (const auto& [columnId, histograms] : columnHistogramsDictionary) {
   for (const auto& histogram : histograms)
   {
    auto res = (histogram.histogramId == INVALID_HISTOGRAM_ID)
       ? this->catalog->InsertColumnHistogramsToMasterDb(
           columnId,
       histogram.rangeStart,
      histogram.rangeEnd,
          histogram.rowCount,
          histogram.distinctCount
        )
      : this->catalog->UpdateHistogramBucket(
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
  const std::vector<Headers::ColumnStatistics> &columnStatistics,
  const std::vector<Headers::IndexStatistics> &indexStatistics
 ) const {
  this->statsManager->Update(tableStatistics, columnStatistics, indexStatistics);
 }

 StatisticsScheduler::StatisticsScheduler(
   const Dictionary<Int, Database *> &databasesDictionary,
   MultiThreading::ReadWriteMutex &latch
   ){
    this->databasesDictionary = &databasesDictionary;
    this->latch = &latch;
    this->catalog = &SystemCatalog::Get();
    this->statsManager = &StatisticsManager::Get();
 }

 void StatisticsScheduler::UpdateStatistics()const {
   for (const auto& database : this->GetDatabases())
     this->UpdateDatabaseStatistics(database);
 }

 void StatisticsScheduler::Start(
  const std::atomic<bool> &isServerRunning,
  const Dictionary<Int, Database*> &databasesDictionary,
  MultiThreading::ReadWriteMutex &latch
  ){
   const StatisticsScheduler scheduler(databasesDictionary, latch);

   std::cout << "Statistics Scheduler started." << std::endl;

   while (isServerRunning) {
    std::this_thread::sleep_for(10000ms);
    scheduler.UpdateStatistics();
   }
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
  if (columnStatistics.min.IsNull() || (value < columnStatistics.min).AsBool())
   columnStatistics.min = value;

  // Update max
  if (columnStatistics.max.IsNull() || (value > columnStatistics.max).AsBool())
   columnStatistics.max = value;

  BigInt frequency = 0;
  if (!sortedValues.TryGetValue(value, frequency))
   sortedValues.Add(value, 1);
  else
   sortedValues.Update(value, frequency + 1);
 }
}
