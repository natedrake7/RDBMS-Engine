#pragma once
#include <CoreEngine/Indexing/BTree.h>
#include <CoreEngine/SystemDatabases/CatalogHeaders.h>
#include <Systemic/DataStructures/Dictionary.h>

#include <cstdint>

namespace CoreEngine {
  class StatisticsManager {
    Dictionary<Int, Catalog::TableStatistics> tableStatisticsCache;
    mutable MultiThreading::Mutex tableStatisticsLatch;

    Dictionary<Int, Catalog::ColumnStatistics> columnStatisticsCache;
    mutable MultiThreading::Mutex columnStatisticsLatch;

    Dictionary<Int, std::vector<Catalog::IndexStatistics>> indexStatisticsCache;
    mutable MultiThreading::Mutex indexStatisticsLatch;

    public:
      static StatisticsManager& Get();

      Catalog::TableStatistics GetTableStatistics(Int tableId);
      Catalog::ColumnStatistics GetColumnStatistics(
        Int tableId,
        Int columnId
      );
      DataStructures::PolymorphicArray<Catalog::IndexStatistics> GetIndexStatistics(Int tableId);

      void Update(
        const Catalog::TableStatistics& tableStatistics,
        const DataStructures::PolymorphicArray<Catalog::ColumnStatistics>& columnStatistics,
        const DataStructures::PolymorphicArray<Catalog::IndexStatistics>& indexStatistics
      );
  };
}