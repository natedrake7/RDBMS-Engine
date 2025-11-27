#pragma once
#include "../Database.h"
#include <string>

namespace DatabaseEngine{

  class VersionDatabase {
    DatabaseHeader header;
    std::string name;
    std::string filename;
    std::string fileExtension;
    std::string systemFilename;

    MultiThreading::ReadWriteMutex lastUsedPageMutex;
    page_id_t lastUsedPageId;

    MultiThreading::ReadWriteMutex gamPageMutex;
    MultiThreading::ReadWriteMutex pfsPageMutex;

    static string CreateDatabasePath(const string & dbName);

    void PopulateFilenames(const std::string& dbName);
    void WriteHeaderToFile()const;

    bool AllocateNewExtent(
        page_id_t& newPageId,
        extent_id_t& newExtentId
    );

    Pages::PageGuard<Pages::Page> TryGetLastUndoPage(
        const DatabaseEngine::StorageTypes::Table* table,
        const row_size_t &size
    );

    Pages::PageGuard<Pages::Page> CreateUndoPage();
    Pages::PageGuard<Pages::Page> GetLastUndoPage(const DatabaseEngine::StorageTypes::Table* table, const row_size_t &size);

    public:
      explicit VersionDatabase(const std::string& filename);
      ~VersionDatabase();

      Errors::RuntimeStatus InsertRow(
        const StorageTypes::Row *row,
        Pages::RowVersionPointer& rowPointer,
        const DatabaseEngine::StorageTypes::Table* table
      );
      const StorageTypes::Row* RetrieveRow(
        const QueryPipeline::PhysicalPlan::Snapshot& snapshot,
        const Pages::RowVersionPointer& rowPointer,
        const DatabaseEngine::StorageTypes::Table* table
      )const;
      [[nodiscard]] std::vector<extent_id_t> GetAllocatedExtents(const Constants::extent_id_t& startingExtentId)const;

      [[nodiscard]] Constants::extent_id_t CleanupVersionedData(
        const Constants::transaction_id_t& transactionId,
        const Constants::extent_id_t& startingExtentId = 0
      )const;
  };
}
