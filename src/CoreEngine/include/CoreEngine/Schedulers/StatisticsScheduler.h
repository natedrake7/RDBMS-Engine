#pragma once
#include <CoreEngine/SystemDatabases/CatalogHeaders.h>
#include <Systemic/DataStructures/Dictionary.h>
#include <Systemic/DataStructures/SortedDictionary.h>
#include <atomic>
#include <condition_variable>
#include <vector>


namespace Statistics {
  struct ValueFrequency;
}

namespace CoreEngine::StorageTypes {
  class Column;
}

namespace MultiThreading {
  class Mutex;
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

        const Dictionary<Int32_t, Database*>* databasesDictionary;
        MultiThreading::Mutex* latch;
        SystemCatalog* catalog;
        StatisticsManager* statsManager;

        [[nodiscard]] std::vector<Database*> GetDatabases()const;

        static Int32_t EstimateRowsPerPage(Int32_t totalRows, Int32_t allocatedPagesPerExtent);
        static Int32_t EstimateAllocatedPagesPerExtent(Int32_t allocatedPagesPerExtent, Int32_t numberOfExtents);

        static bool GenerateColumnHistograms(
            const SortedDictionary<Value, Int64_t, ValueComparator>& sortedValues,
            DataStructures::PolymorphicArray<Catalog::ColumnHistograms>& histograms,
            const Catalog::ColumnStatistics& columnStatistics,
            Int64_t totalRows
        );

        void UpdateDatabaseStatistics(const Database* database)const;
        void UpdateTableStatistics(StorageTypes::Table* table)const;

        [[nodiscard]] static bool UpdateIndexStatistics(
            StorageTypes::Table* table,
            Catalog::IndexStatistics& indexStatistics,
            Catalog::TableStatistics& tableStatistics,
            DataStructures::PolymorphicArray<Catalog::ColumnStatistics>& columnStatistics,
            Dictionary<Int32_t, SortedDictionary<Value, Int64_t, ValueComparator>>& sortedValues
        );

        static void UpdateHeapStatistics(
            const StorageTypes::Table* table,
            page_id_t allocationPageId,
            Catalog::TableStatistics& tableStatistics,
            DataStructures::PolymorphicArray<Catalog::ColumnStatistics>& columnStatistics,
            Dictionary<Int32_t, SortedDictionary<Value, Int64_t, ValueComparator>>& sortedValues
        );

        void UpdateCatalogStatistics(
            const ExecutionContext& baseContext,
            const Catalog::TableStatistics& tableStatistics,
            const DataStructures::PolymorphicArray<Catalog::ColumnStatistics>& columnStatistics,
            const DataStructures::PolymorphicArray<Catalog::IndexStatistics>& indexStatistics,
            const Dictionary<Int32_t, DataStructures::PolymorphicArray<Catalog::ColumnHistograms>> &columnHistogramsDictionary
        )const;

        void UpdateCache(
            const Catalog::TableStatistics& tableStatistics,
            const DataStructures::PolymorphicArray<Catalog::ColumnStatistics>& columnStatistics,
            const DataStructures::PolymorphicArray<Catalog::IndexStatistics> &indexStatistics
        )const;

        public:
            StatisticsScheduler(const Dictionary<Int32_t, Database*>& databasesDictionary, MultiThreading::Mutex& latch);
            void UpdateStatistics()const;
            static void Start(
                const std::atomic<bool> &isServerRunning,
                const Dictionary<Int32_t, Database*> &databasesDictionary,
                MultiThreading::Mutex &latch
            );
            static void Stop();
            static void UpdateColumnStatistics(
                Catalog::ColumnStatistics& columnStatistics,
                const Value& value,
                SortedDictionary<Value, Int64_t, ValueComparator>& sortedValues
            );
            static std::mutex& Mutex();
            static std::condition_variable& ConditionVariable();
    };
}