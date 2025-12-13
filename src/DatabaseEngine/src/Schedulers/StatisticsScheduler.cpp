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

 int StatisticsScheduler::EstimateRowsPerPage(const int& totalRows, const int& allocatedPagesPerExtent) {
  return (allocatedPagesPerExtent == 0)
    ? 0
    : static_cast<int>(std::ceil(static_cast<float>(totalRows) / static_cast<float>(allocatedPagesPerExtent)));
 }

 int StatisticsScheduler::EstimateAllocatedPagesPerExtent(const int& allocatedPagesPerExtent, const int& numberOfExtents) {
  return (numberOfExtents == 0)
     ? 0
     : static_cast<int>(std::ceil(static_cast<float>(allocatedPagesPerExtent) / static_cast<float>(numberOfExtents)));
 }

  void StatisticsScheduler::UpdateDatabaseStatistics(const Database *database)const {
   for (const auto& table : database->GetTables()) {
     const auto cacheStats = this->statsManager->GetTableStatistics(table->GetTableId());

     auto currentTime = DataTypes::DateTime::Now();
     currentTime.AddMinutes(-10); // Update stats if older than 10 minutes test for production grade this should be dynamic

     if (currentTime <= cacheStats.lastModified)
      continue;

     std::cout << "Updating statistics for table: " << cacheStats.tableId << std::endl;

     this->UpdateTableStatistics(table, database->GetSystemFilename(), database->GetFileName());
   }
 }

 void StatisticsScheduler::UpdateTableStatistics(
  const StorageTypes::Table *table,
  const std::string& systemFilename,
  const std::string& filename
 )const {
  const auto& iamPageId = table->GetIndexAllocationMapPageId();

  if (iamPageId == INVALID_PAGE_ID)
   return;

  const auto iamPage = Storage::StorageManager::Get().GetIndexAllocationMapPage(table->GetFileName(), iamPageId, table);

  std::vector<extent_id_t> extents;
  iamPage->GetAllocatedExtents(&extents, 0);

  int estimatedRowCount = 1000;
  Headers::TableStatistics tableStatistics = {
   .tableId = table->GetTableId(),
   .rowCount = 0,
   .averageRowSize = 0
  };

  std::vector<Headers::ColumnStatistics> columnStatistics;
  for (const auto& column : table->GetColumns()) {
   columnStatistics.emplace_back(
    Headers::ColumnStatistics{
     .columnId = column->GetColumnId(),
     .distinctCount = 0,
     .min = Value::Null(),
     .max = Value::Null(),
     .nullCount = 0,
   });
  }

  int sampleRowCount = 0;
  int averageRowsPerPage = 0;
  int allocatedPagesPerExtent = 0;
  for (const auto& extentId : extents) {
   const page_id_t extentFirstPageId = extentId * EXTENT_SIZE;

   const auto pageFreeSpacePage = DatabaseEngine::Database::GetAssociatedPfsPage(systemFilename, extentFirstPageId);

   const auto firstDataPageId = (iamPage->GetPageId() != extentFirstPageId)
                               ? extentFirstPageId
                               : extentFirstPageId + 1;

   MultiThreading::ReaderGuard pfsLatch(&pageFreeSpacePage->GetLatch());

   for (page_id_t extentPageId = firstDataPageId; extentPageId < extentFirstPageId + EXTENT_SIZE; extentPageId++){
    const auto pageType = pageFreeSpacePage->GetPageType(extentPageId);

    if (pageType != PageType::DATA && pageType != PageType::INDEX)
     break;

    auto page = Storage::StorageManager::Get().GetPage(filename, extentPageId, table);

    MultiThreading::ReaderGuard lock(&page->GetLatch());

    const auto pageSize = page->GetPageSize();
    if (pageSize == 0)
     continue;

    averageRowsPerPage += pageSize;
    allocatedPagesPerExtent++;

    for (const auto& row : *page->GetDataRowsNoLock()) {
      tableStatistics.averageRowSize += row->GetTotalRowSize();
      sampleRowCount++;

      for (int j = 0; j < columnStatistics.size(); j++) {
       const auto& value = row->GetColumnByIndex(j);
       StatisticsScheduler::UpdateColumnStatistics(columnStatistics[j], value);
      }
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

  tableStatistics.lastModified = DataTypes::DateTime::Now();
  //Update catalog
  this->UpdateCache(tableStatistics, columnStatistics);
  this->UpdateCatalogStatistics(tableStatistics, columnStatistics);
 }

 void StatisticsScheduler::UpdateColumnStatistics(
  Headers::ColumnStatistics &columnStatistics,
  const Value &value
 ) {
  if (value.IsNull()) {
   columnStatistics.nullCount++;
   return;
  }

  // Update distinct count - simplistic approach
  columnStatistics.distinctCount++; // In real scenario, use a hash set or similar structure

  // Update min
  if (columnStatistics.min.IsNull() || (value < columnStatistics.min).GetBool())
   columnStatistics.min = value;

  // Update max
  if (columnStatistics.max.IsNull() || (value > columnStatistics.max).GetBool())
   columnStatistics.max = value;
 }

 void StatisticsScheduler::UpdateCatalogStatistics(
  const Headers::TableStatistics &tableStatistics,
  const std::vector<Headers::ColumnStatistics> &columnStatistics
 )const {

  this->catalog->UpdateTableStatisticsById(
    tableStatistics.tableId,
    tableStatistics.rowCount,
    tableStatistics.averageRowSize
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
 }

 void StatisticsScheduler::UpdateCache(
  const Headers::TableStatistics &tableStatistics,
  const std::vector<Headers::ColumnStatistics> &columnStatistics
 ) const {
  this->statsManager->Update(tableStatistics, columnStatistics);
 }

 StatisticsScheduler::StatisticsScheduler(
   const Dictionary<int32_t, Database *> &databasesDictionary,
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
  const Dictionary<int32_t, Database*> &databasesDictionary,
  MultiThreading::ReadWriteMutex &latch
  ){
   const StatisticsScheduler scheduler(databasesDictionary, latch);

   std::cout << "Statistics Scheduler started." << std::endl;

   while (isServerRunning) {
    std::this_thread::sleep_for(10000ms);
    scheduler.UpdateStatistics();
   }
 }


}