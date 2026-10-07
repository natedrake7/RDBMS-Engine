#include <CoreEngine/Managers/StatisticsManager.h>

#include <Systemic/Guards/ReaderGuard.h>
#include <Systemic/Guards/WriterGuard.h>
#include <CoreEngine/Memory/Allocator.h>
#include <CoreEngine/SystemDatabases/SystemCatalog.h>

namespace CoreEngine {
  StatisticsManager & StatisticsManager::Get() {
    static StatisticsManager instance;
    return instance;
  }

  Catalog::TableStatistics StatisticsManager::GetTableStatistics(const Int tableId) {
    MultiThreading::ReaderGuard lock(&this->tableStatisticsLatch);

    Catalog::TableStatistics stats;
    if (this->tableStatisticsCache.TryGetValue(tableId, stats))
      return stats;

    const Memory::Allocator allocator;
    auto catalogStats = SystemCatalog::Get().SelectTableStatisticsById(&allocator, tableId);

    if (catalogStats.tableId == INVALID_TABLE_ID)
      return stats;

    // auto writerLock = MultiThreading::WriterGuard::Promote(&this->tableStatisticsLatch, lock);

    //this->tableStatisticsCache.ForceAdd(tableId, catalogStats);

    return catalogStats;
  }

  Catalog::ColumnStatistics StatisticsManager::GetColumnStatistics(
    const Int tableId,
    const Int columnId
  ) {
    MultiThreading::ReaderGuard lock(&this->columnStatisticsLatch);

    Catalog::ColumnStatistics stats;
    if (this->columnStatisticsCache.TryGetValue(columnId, stats))
      return stats;

    const Memory::Allocator allocator;
    auto columnHeader = SystemCatalog::Get().SelectColumnById(&allocator, tableId, columnId);

    auto catalogStats = SystemCatalog::Get().SelectColumnStatisticsById(&allocator, columnId, static_cast<DataType>(columnHeader.dataType));

    if (catalogStats.columnId == INVALID_TABLE_ID)
      return stats;

    // auto writerLock = MultiThreading::WriterGuard::Promote(&this->columnStatisticsLatch, lock);

    //this->columnStatisticsCache.ForceAdd(columnId, catalogStats);

    return catalogStats;
  }

  DataStructures::PolymorphicArray<Catalog::IndexStatistics> StatisticsManager::GetIndexStatistics(const Int tableId) {
    MultiThreading::ReaderGuard lock(&this->indexStatisticsLatch);

    DataStructures::PolymorphicArray<Catalog::IndexStatistics> stats;
    // if (this->indexStatisticsCache.TryGetValue(tableId, stats))
    //   return stats;

    const Memory::Allocator allocator;
    auto catalogStats = SystemCatalog::Get().SelectIndexStatisticsByTableId(&allocator, tableId);

    if (catalogStats.Empty())
      return stats;

    // auto writerLock = MultiThreading::WriterGuard::Promote(&this->indexStatisticsLatch, lock);

    //// this->indexStatisticsCache.ForceAdd(tableId, catalogStats);

    return catalogStats;
  }

  void StatisticsManager::Update(
    const Catalog::TableStatistics &tableStatistics,
    const DataStructures::PolymorphicArray<Catalog::ColumnStatistics> &columnStatistics,
    const DataStructures::PolymorphicArray<Catalog::IndexStatistics>& indexStatistics
  ) {
    {
      MultiThreading::WriterGuard lock(&this->tableStatisticsLatch);
      //this->tableStatisticsCache.ForceAdd(tableStatistics.tableId, tableStatistics);
    }

    {
      MultiThreading::WriterGuard lock(&this->columnStatisticsLatch);
      // for (const auto& colStats : columnStatistics)
        //this->columnStatisticsCache.ForceAdd(colStats.columnId, colStats);
    }

    {
      MultiThreading::WriterGuard lock(&this->indexStatisticsLatch);
      // if (!indexStatistics.empty())
        //this->indexStatisticsCache.ForceAdd(tableStatistics.tableId, indexStatistics);
    }
}

}