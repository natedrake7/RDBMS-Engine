#pragma once
#include "../BTree.h"
#include "../../../Systemic/include/Headers.h"
#include "../../../Systemic/include/DataStructures/Dictionary.h"

#include <cstdint>

namespace DatabaseEngine {
  class StatisticsManager {
    Dictionary<int32_t, Headers::TableStatistics> tableStatisticsCache;
    mutable MultiThreading::ReadWriteMutex tableStatisticsLatch;

    Dictionary<int32_t, Headers::ColumnStatistics> columnStatisticsCache;
    mutable MultiThreading::ReadWriteMutex columnStatisticsLatch;

    Dictionary<int32_t, std::vector<Headers::IndexStatistics>> indexStatisticsCache;
    mutable MultiThreading::ReadWriteMutex indexStatisticsLatch;

    public:
      static StatisticsManager& Get();

      Headers::TableStatistics GetTableStatistics(const int32_t& tableId);
      Headers::ColumnStatistics GetColumnStatistics(const int32_t &columnId, const DataType& type);
      std::vector<Headers::IndexStatistics> GetIndexStatistics(const int32_t& tableId);

      void Update(
        const Headers::TableStatistics& tableStatistics,
        const std::vector<Headers::ColumnStatistics>& columnStatistics,
        const std::vector<Headers::IndexStatistics>& indexStatistics
      );
  };
}