#pragma once
#include "../../Systemic/include/Headers.h"
#include "../../Systemic/include/DataStructures/Dictionary.h"
#include "../../Systemic/include/DataStructures/SortedDictionary.h"
#include <atomic>
#include <cstdint>
#include <string>
#include <vector>


namespace Statistics {
  struct ValueFrequency;
}

namespace DatabaseEngine::StorageTypes {
  class Column;
}

namespace MultiThreading {
  class ReadWriteMutex;
}

namespace DatabaseEngine {
  class StatisticsManager;
  class SystemCatalog;
  class Database;

  namespace StorageTypes {
    class Table;
  }

  class StatisticsScheduler {
    const Dictionary<int32_t, Database*>* databasesDictionary;
    MultiThreading::ReadWriteMutex* latch;
    SystemCatalog* catalog;
    StatisticsManager* statsManager;

    [[nodiscard]] std::vector<Database *> GetDatabases()const;

    static int EstimateRowsPerPage(const int& totalRows, const int& allocatedPagesPerExtent);
    static int EstimateAllocatedPagesPerExtent(const int& allocatedPagesPerExtent, const int& numberOfExtents);

    static bool GenerateColumnHistograms(
      const SortedDictionary<Value, int64_t, ValueComparator>& sortedValues,
      std::vector<Headers::ColumnHistograms>& histograms,
      const Headers::ColumnStatistics& columnStatistics,
      const int64_t& totalRows
    );

    void UpdateDatabaseStatistics(const Database* database)const;
    void UpdateTableStatistics(
      StorageTypes::Table* table,
      const std::string& systemFilename,
      const std::string& filename
    )const;

    [[nodiscard]] static bool UpdateIndexStatistics(
      StorageTypes::Table* table,
      Headers::IndexStatistics& indexStatistics,
      Headers::TableStatistics& tableStatistics,
      std::vector<Headers::ColumnStatistics>& columnStatistics,
      Dictionary<int32_t, SortedDictionary<Value, int64_t, ValueComparator>>& sortedValues
    );

    static void UpdateHeapStatistics(
      const StorageTypes::Table* table,
      const page_id_t& iamPageId,
      const std::string& systemFilename,
      const std::string& filename,
      Headers::TableStatistics& tableStatistics,
      std::vector<Headers::ColumnStatistics>& columnStatistics,
      Dictionary<int32_t, SortedDictionary<Value, int64_t, ValueComparator>>& sortedValues
    );

    void UpdateCatalogStatistics(
      const Headers::TableStatistics& tableStatistics,
      const std::vector<Headers::ColumnStatistics>& columnStatistics,
      const std::vector<Headers::IndexStatistics>& indexStatistics,
      const Dictionary<int32_t, std::vector<Headers::ColumnHistograms>> &columnHistogramsDictionary
    )const;

    void UpdateCache(
      const Headers::TableStatistics& tableStatistics,
      const std::vector<Headers::ColumnStatistics>& columnStatistics,
      const std::vector<Headers::IndexStatistics> &indexStatistics
    )const;

    public:
      StatisticsScheduler(const Dictionary<int32_t, Database*>& databasesDictionary, MultiThreading::ReadWriteMutex& latch);
      void UpdateStatistics()const;
      static void Start(
        const std::atomic<bool> &isServerRunning,
        const Dictionary<int32_t, Database*> &databasesDictionary,
        MultiThreading::ReadWriteMutex &latch
      );

    static void UpdateColumnStatistics(
      Headers::ColumnStatistics& columnStatistics,
      const Value& value,
      SortedDictionary<Value, int64_t, ValueComparator>& sortedValues
    );
  };
}