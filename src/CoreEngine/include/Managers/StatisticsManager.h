#pragma once
#include "../BTree.h"
#include "../../../Systemic/include/Headers.h"
#include "../../../Systemic/include/DataStructures/Dictionary.h"

#include <cstdint>

namespace CoreEngine {
  class StatisticsManager {
    Dictionary<Int, Headers::TableStatistics> tableStatisticsCache;
    mutable MultiThreading::ReadWriteMutex tableStatisticsLatch;

    Dictionary<Int, Headers::ColumnStatistics> columnStatisticsCache;
    mutable MultiThreading::ReadWriteMutex columnStatisticsLatch;

    Dictionary<Int, std::vector<Headers::IndexStatistics>> indexStatisticsCache;
    mutable MultiThreading::ReadWriteMutex indexStatisticsLatch;

    public:
      static StatisticsManager& Get();

      Headers::TableStatistics GetTableStatistics(Int tableId);
      Headers::ColumnStatistics GetColumnStatistics(
        Int tableId,
        Int columnId
      );
      DataStructures::PolymorphicArray<Headers::IndexStatistics> GetIndexStatistics(Int tableId);

      void Update(
        const Headers::TableStatistics& tableStatistics,
        const DataStructures::PolymorphicArray<Headers::ColumnStatistics>& columnStatistics,
        const DataStructures::PolymorphicArray<Headers::IndexStatistics>& indexStatistics
      );
  };
}