#pragma once
#include "../Database.h"
#include <string>
#include "../../Systemic/include/Errors.h"

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

    static std::string CreateDatabasePath(const std::string& dbName);

    void PopulateFilenames();
    void WriteHeaderToFile()const;

    bool AllocateNewExtent(
        page_id_t& newPageId,
        extent_id_t& newExtentId
    );

    Pages::PageView TryGetLastUndoPage(
        const StorageTypes::Table* table,
        row_size_t size
    );

    Pages::PageView CreateUndoPage();
    Pages::PageView GetLastUndoPage(const StorageTypes::Table* table, row_size_t size);

    VersionDatabase();
    ~VersionDatabase();

    void ReadConfiguration(const std::string& configPath);

    [[nodiscard]] bool VersionDatabaseExists()const;

    public:
      VersionDatabase(VersionDatabase const&) = delete;
      void operator=(VersionDatabase const&) = delete;
      VersionDatabase(VersionDatabase&&) = delete;
      void operator=(VersionDatabase&&) = delete;

      static VersionDatabase& Get();

      void Initialize(const std::string& configPath);

      Errors::RuntimeStatus InsertRow(
        const Pages::RowReference& rowPtr,
        StorageTypes::RowVersionPointer& rowPointer,
        const StorageTypes::Table* table
      );
      Pages::RowReference RetrieveRowReference(
        const Snapshot& snapshot,
        const StorageTypes::RowVersionPointer& rowPointer,
        const StorageTypes::Table* table
      )const;
      [[nodiscard]] std::vector<extent_id_t> GetAllocatedExtents(extent_id_t startingExtentId)const;

      [[nodiscard]] extent_id_t CleanupVersionedData(
        transaction_id_t transactionId,
        extent_id_t startingExtentId = 0
      )const;
  };
}
