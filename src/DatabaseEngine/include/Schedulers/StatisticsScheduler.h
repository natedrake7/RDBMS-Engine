#pragma once
#include "../../Systemic/include/Headers.h"
#include "../../Systemic/include/DataStructures/Dictionary.h"
#include "../../Systemic/include/DataStructures/SortedDictionary.h"
#include <atomic>
#include <cstdInt>
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
    const Dictionary<Int, Database*>* databasesDictionary;
    MultiThreading::ReadWriteMutex* latch;
    SystemCatalog* catalog;
    StatisticsManager* statsManager;

    [[nodiscard]] std::vector<Database *> GetDatabases()const;

    static Int EstimateRowsPerPage(Int totalRows, Int allocatedPagesPerExtent);
    static Int EstimateAllocatedPagesPerExtent(Int allocatedPagesPerExtent, Int numberOfExtents);

    static bool GenerateColumnHistograms(
      const SortedDictionary<Value, BigInt, ValueComparator>& sortedValues,
      std::vector<Headers::ColumnHistograms>& histograms,
      const Headers::ColumnStatistics& columnStatistics,
      BigInt totalRows
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
      Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>>& sortedValues
    );

    static void UpdateHeapStatistics(
      const StorageTypes::Table* table,
      page_id_t iamPageId,
      const std::string& systemFilename,
      const std::string& filename,
      Headers::TableStatistics& tableStatistics,
      std::vector<Headers::ColumnStatistics>& columnStatistics,
      Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>>& sortedValues
    );

    void UpdateCatalogStatistics(
      const Headers::TableStatistics& tableStatistics,
      const std::vector<Headers::ColumnStatistics>& columnStatistics,
      const std::vector<Headers::IndexStatistics>& indexStatistics,
      const Dictionary<Int, std::vector<Headers::ColumnHistograms>> &columnHistogramsDictionary
    )const;

    void UpdateCache(
      const Headers::TableStatistics& tableStatistics,
      const std::vector<Headers::ColumnStatistics>& columnStatistics,
      const std::vector<Headers::IndexStatistics> &indexStatistics
    )const;

    public:
      StatisticsScheduler(const Dictionary<Int, Database*>& databasesDictionary, MultiThreading::ReadWriteMutex& latch);
      void UpdateStatistics()const;
      static void Start(
        const std::atomic<bool> &isServerRunning,
        const Dictionary<Int, Database*> &databasesDictionary,
        MultiThreading::ReadWriteMutex &latch
      );

    static void UpdateColumnStatistics(
      Headers::ColumnStatistics& columnStatistics,
      const Value& value,
      SortedDictionary<Value, BigInt, ValueComparator>& sortedValues
    );
  };
}