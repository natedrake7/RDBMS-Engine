#pragma once
#include "../BTree.h"
#include "../../../Systemic/include/Headers.h"
#include "../../../Systemic/include/DataStructures/Dictionary.h"

#include <cstdint>

namespace DatabaseEngine {
  class StatisticsManager {
    Dictionary<Int, Headers::TableStatistics> tableStatisticsCache;
    mutable MultiThreading::ReadWriteMutex tableStatisticsLatch;

    Dictionary<Int, Headers::ColumnStatistics> columnStatisticsCache;
    mutable MultiThreading::ReadWriteMutex columnStatisticsLatch;

    Dictionary<Int, std::vector<Headers::IndexStatistics>> indexStatisticsCache;
    mutable MultiThreading::ReadWriteMutex indexStatisticsLatch;

    public:
      static StatisticsManager& Get();

      Headers::TableStatistics GetTableStatistics(const Int& tableId);
      Headers::ColumnStatistics GetColumnStatistics(
        const Int& tableId,
        const Int &columnId
      );
      std::vector<Headers::IndexStatistics> GetIndexStatistics(const Int& tableId);

      void Update(
        const Headers::TableStatistics& tableStatistics,
        const std::vector<Headers::ColumnStatistics>& columnStatistics,
        const std::vector<Headers::IndexStatistics>& indexStatistics
      );
  };
}