#include "../../include/Managers/StatisticsManager.h"

#include "Guards/ReaderGuard.h"
#include "Guards/WriterGuard.h"
#include "SystemDatabases/SystemCatalog.h"

namespace DatabaseEngine {

  StatisticsManager & StatisticsManager::Get() {
    static StatisticsManager instance;
    return instance;
  }

  Headers::TableStatistics StatisticsManager::GetTableStatistics(const Int tableId) {
    MultiThreading::ReaderGuard lock(&this->tableStatisticsLatch);

    Headers::TableStatistics stats;
    if (this->tableStatisticsCache.TryGetValue(tableId, stats))
      return stats;

    auto catalogStats = SystemCatalog::Get().SelectTableStatisticsById(tableId);

    if (catalogStats.tableId == INVALID_TABLE_ID)
      return stats;

    auto writerLock = MultiThreading::WriterGuard::Promote(&this->tableStatisticsLatch, lock);

    this->tableStatisticsCache.ForceAdd(tableId, catalogStats);

    return catalogStats;
  }

  Headers::ColumnStatistics StatisticsManager::GetColumnStatistics(
    const Int tableId,
    const Int columnId
  ) {
    MultiThreading::ReaderGuard lock(&this->columnStatisticsLatch);

    Headers::ColumnStatistics stats;
    if (this->columnStatisticsCache.TryGetValue(columnId, stats))
      return stats;

    auto columnHeader = SystemCatalog::Get().SelectColumnById(tableId, columnId);

    auto catalogStats = SystemCatalog::Get().SelectColumnStatisticsById(columnId, static_cast<DataType>(columnHeader.dataType));

    if (catalogStats.columnId == INVALID_TABLE_ID)
      return stats;

    auto writerLock = MultiThreading::WriterGuard::Promote(&this->columnStatisticsLatch, lock);

    this->columnStatisticsCache.ForceAdd(columnId, catalogStats);

    return catalogStats;
  }

  std::vector<Headers::IndexStatistics> StatisticsManager::GetIndexStatistics(const Int tableId) {
    MultiThreading::ReaderGuard lock(&this->indexStatisticsLatch);

    std::vector<Headers::IndexStatistics> stats;
    if (this->indexStatisticsCache.TryGetValue(tableId, stats))
      return stats;

    auto catalogStats = SystemCatalog::Get().SelectIndexStatisticsByTableId(tableId);

    if (catalogStats.empty())
      return stats;

    auto writerLock = MultiThreading::WriterGuard::Promote(&this->indexStatisticsLatch, lock);

    this->indexStatisticsCache.ForceAdd(tableId, catalogStats);

    return catalogStats;
  }

  void StatisticsManager::Update(
    const Headers::TableStatistics &tableStatistics,
    const std::vector<Headers::ColumnStatistics> &columnStatistics,
    const std::vector<Headers::IndexStatistics>& indexStatistics
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

    {
      MultiThreading::WriterGuard lock(&this->indexStatisticsLatch);
      if (!indexStatistics.empty())
        this->indexStatisticsCache.ForceAdd(tableStatistics.tableId, indexStatistics);
    }
}

}