#pragma once
#include <CoreEngine/Indexing/BTree.h>
#include <CoreEngine/SystemDatabases/CatalogHeaders.h>
#include <Systemic/DataStructures/Dictionary.h>

#include <cstdint>

namespace CoreEngine {
  class StatisticsManager {
    Dictionary<Int32_t, Catalog::TableStatistics> tableStatisticsCache;
    mutable MultiThreading::Mutex tableStatisticsLatch;

    Dictionary<Int32_t, Catalog::ColumnStatistics> columnStatisticsCache;
    mutable MultiThreading::Mutex columnStatisticsLatch;

    Dictionary<Int32_t, std::vector<Catalog::IndexStatistics>> indexStatisticsCache;
    mutable MultiThreading::Mutex indexStatisticsLatch;

    public:
      static StatisticsManager& Get();

      Catalog::TableStatistics GetTableStatistics(Int32_t tableId);
      Catalog::ColumnStatistics GetColumnStatistics(
        Int32_t tableId,
        Int32_t columnId
      );
      DataStructures::PolymorphicArray<Catalog::IndexStatistics> GetIndexStatistics(Int32_t tableId);

      void Update(
        const Catalog::TableStatistics& tableStatistics,
        const DataStructures::PolymorphicArray<Catalog::ColumnStatistics>& columnStatistics,
        const DataStructures::PolymorphicArray<Catalog::IndexStatistics>& indexStatistics
      );
  };
}