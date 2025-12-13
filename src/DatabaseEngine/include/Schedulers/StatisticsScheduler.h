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
  class SystemCatalog;
  class Database;

  namespace StorageTypes {
    class Table;
  }

  class StatisticsScheduler {
    const Dictionary<int32_t, Database*>* databasesDictionary;
    MultiThreading::ReadWriteMutex* latch;
    SystemCatalog* catalog;

    [[nodiscard]] std::vector<Database *> GetDatabases()const;

    void UpdateDatabaseStatistics(const Database* database);
    void UpdateTableStatistics(
      StorageTypes::Table* table,
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

    public:
      StatisticsScheduler(const Dictionary<int32_t, Database*>& databasesDictionary, MultiThreading::ReadWriteMutex& latch);
      void UpdateStatistics();
      static void Start(
        const std::atomic<bool> &isServerRunning,
        const Dictionary<int32_t, Database*> &databasesDictionary,
        MultiThreading::ReadWriteMutex &latch
      );
  };
}