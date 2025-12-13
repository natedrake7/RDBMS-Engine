#pragma once
#include "../../Systemic/include/Headers.h"
#include "../../Systemic/include/DataStructures/Dictionary.h"

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

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

    void UpdateDatabaseStatistics(const Database* database)const;
    void UpdateTableStatistics(
      const StorageTypes::Table* table,
      const std::string& systemFilename,
      const std::string& filename
    )const;
    static void UpdateColumnStatistics(
      Headers::ColumnStatistics& columnStatistics,
      const Value& value
    );

    void UpdateCatalogStatistics(
      const Headers::TableStatistics& tableStatistics,
      const std::vector<Headers::ColumnStatistics>& columnStatistics
    )const;

    void UpdateCache(
      const Headers::TableStatistics& tableStatistics,
      const std::vector<Headers::ColumnStatistics>& columnStatistics
    )const;

    public:
      StatisticsScheduler(const Dictionary<int32_t, Database*>& databasesDictionary, MultiThreading::ReadWriteMutex& latch);
      void UpdateStatistics()const;
      static void Start(
        const std::atomic<bool> &isServerRunning,
        const Dictionary<int32_t, Database*> &databasesDictionary,
        MultiThreading::ReadWriteMutex &latch
      );
  };
}