#pragma once
#include "../../Systemic/include/Headers.h"
#include "../../Systemic/include/DataStructures/Dictionary.h"
#include "../../Systemic/include/DataStructures/SortedDictionary.h"
#include <atomic>
#ifdef __WIN32__
#include <cstdInt>
#endif
#include <condition_variable>
#include <string>
#include <vector>


namespace Statistics {
  struct ValueFrequency;
}

namespace CoreEngine::StorageTypes {
  class Column;
}

namespace MultiThreading {
  class ReadWriteMutex;
}

namespace CoreEngine {
    class ExecutionContext;
    class StatisticsManager;
    class SystemCatalog;
    class Database;

  namespace StorageTypes {
    class Table;
  }

    class StatisticsScheduler {
        static inline std::mutex _mutex;
        static inline std::condition_variable _cv;

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
        void UpdateTableStatistics(StorageTypes::Table* table)const;

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
            Headers::TableStatistics& tableStatistics,
            std::vector<Headers::ColumnStatistics>& columnStatistics,
            Dictionary<Int, SortedDictionary<Value, BigInt, ValueComparator>>& sortedValues
        );

        void UpdateCatalogStatistics(
            const ExecutionContext& baseContext,
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
            static void Stop();
            static void UpdateColumnStatistics(
                Headers::ColumnStatistics& columnStatistics,
                const Value& value,
                SortedDictionary<Value, BigInt, ValueComparator>& sortedValues
            );
            static std::mutex& Mutex();
            static std::condition_variable& ConditionVariable();
    };
}