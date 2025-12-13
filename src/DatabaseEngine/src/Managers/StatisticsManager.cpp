#include "../../include/Managers/StatisticsManager.h"

#include "Guards/ReaderGuard.h"
#include "Guards/WriterGuard.h"
#include "SystemDatabases/SystemCatalog.h"

namespace DatabaseEngine {

  StatisticsManager & StatisticsManager::Get() {
    static StatisticsManager instance;
    return instance;
  }

  Headers::TableStatistics StatisticsManager::GetTableStatistics(const int32_t &tableId) {
    MultiThreading::ReaderGuard lock(&this->tableStatisticsLatch);

    Headers::TableStatistics stats;
    if (this->tableStatisticsCache.TryGetValue(tableId, stats))
      return stats;

    auto catalogStats = SystemCatalog::Get().SelectTableStatisticsById(tableId);

    if (catalogStats.tableId == INVALID_TABLE_ID)
      return stats;

    auto writerLock = MultiThreading::WriterGuard::Promote(&this->tableStatisticsLatch, lock);

    if (this->tableStatisticsCache.TryGetValue(tableId, stats))
      return stats;

    this->tableStatisticsCache.Add(tableId, catalogStats);

    return catalogStats;
  }

  Headers::ColumnStatistics StatisticsManager::GetColumnStatistics(const int32_t &columnId, const DataType& type) {
    MultiThreading::ReaderGuard lock(&this->columnStatisticsLatch);

    Headers::ColumnStatistics stats;
    if (this->columnStatisticsCache.TryGetValue(columnId, stats))
      return stats;

    auto catalogStats = SystemCatalog::Get().SelectColumnStatisticsById(columnId, type);

    if (catalogStats.columnId == INVALID_TABLE_ID)
      return stats;

    auto writerLock = MultiThreading::WriterGuard::Promote(&this->columnStatisticsLatch, lock);

    if (this->columnStatisticsCache.TryGetValue(columnId, stats))
      return stats;

    this->columnStatisticsCache.Add(columnId, catalogStats);

    return catalogStats;
  }
  void StatisticsManager::Update(
    const Headers::TableStatistics &tableStatistics,
    const std::vector<Headers::ColumnStatistics> &columnStatistics
  ) {
    {
      MultiThreading::WriterGuard lock(&this->tableStatisticsLatch);
      this->tableStatisticsCache.ForceAdd(tableStatistics.tableId, tableStatistics);
    }

    {
      MultiThreading::WriterGuard lock(&this->columnStatisticsLatch);
      for (const auto& colStats : columnStatistics)
        this->columnStatisticsCache.ForceAdd(colStats.columnId, colStats);
  }
}

}