#include "../../include/SystemDatabases/VersionDatabase.h"

#include "../../include/Pages/HeaderPage.h"
#include "../../include/Pages/GlobalAllocationMapPage.h"
#include "../../include/Pages/PageFreeSpacePage.h"
#include "../../include/BufferPool/StorageManager.h"
#include "../../../Systemic/include/Guards/ReaderGuard.h"
#include "../../../Systemic/include/Guards/WriterGuard.h"

#include <nlohmann/json.hpp>

namespace DatabaseEngine {
  VersionDatabase& VersionDatabase::Get(){
    static VersionDatabase instance;
    return instance;
  }

  void VersionDatabase::Initialize(const std::string& configPath){
    this->ReadConfiguration(configPath);
    this->PopulateFilenames();

    if (!this->VersionDatabaseExists())
      DatabaseEngine::CreateDatabase(this->name);

    this->lastUsedPageId = INVALID_PAGE_ID;
    const auto headerPage = Storage::StorageManager::Get().GetHeaderPage(this->systemFilename);

    this->header = *headerPage->GetDatabaseHeader();
  }

 VersionDatabase::VersionDatabase() = default;

  VersionDatabase::~VersionDatabase() = default;

  void VersionDatabase::ReadConfiguration(const string& configPath){
    std::ifstream file(configPath);

    if (!file.is_open())
      throw std::runtime_error("System Tables file: " + configPath + " could not be opened");

    nlohmann::json jsonFile;

    try {
      file >> jsonFile;
    }
    catch (std::exception &e)
    {
      throw std::runtime_error(e.what());
    }

    this->name = jsonFile.at("version_db_name");
    this->filename = jsonFile.at("version_db_path");
  }

  bool VersionDatabase::VersionDatabaseExists() const{
    return std::filesystem::exists(this->filename);
  }

  string VersionDatabase::CreateDatabasePath(const string & dbName){ return dbName + "/" + dbName; }

  void VersionDatabase::PopulateFilenames(){
     const auto& path = Database::CreateDatabasePath(this->name);

     this->filename = path + ".db";
     this->fileExtension = ".db";
     this->systemFilename = path + "_sys" + ".db";
   }

  void VersionDatabase::WriteHeaderToFile() const
 {
   auto metaDataPage = Storage::StorageManager::Get().GetHeaderPage(this->systemFilename);

   metaDataPage->SetDbHeader(this->header);
 }

  bool VersionDatabase::AllocateNewExtent(page_id_t& newPageId, extent_id_t& newExtentId){
    {
      const MultiThreading::WriterGuard gamLock(&this->gamPageMutex);

      auto gamPage = Storage::StorageManager::Get().GetGlobalAllocationMapPage(this->systemFilename, this->header.lastGamPageId);

      if (gamPage->IsFull())
      {
        const auto nextGamPageId = Database::CalculateNextGamPageId(this->header.lastGamPageId);

        gamPage = Storage::StorageManager::Get().CreateGlobalAllocationMapPage(this->systemFilename, nextGamPageId);

        this->header.lastGamPageId = gamPage->GetPageId();
      }

      std::vector<extent_id_t> extents;

      MultiThreading::WriterGuard gamPageLock(&gamPage->Latch());
      // Step 3: allocate an extent from the current (or new) GAM page
      const auto allocatedExtentsCount = gamPage->AllocateExtentsNoLock(extents, 1);

      newExtentId = extents.front();
      newPageId   = Database::CalculateExtentFirstPageId(newExtentId);
    }

    // Step 4: ensure PFS page exists
    const auto pfsPageId = Database::GetPfsAssociatedPage(newPageId);

    {
      MultiThreading::WriterGuard pfsLock(&this->pfsPageMutex);

      if (pfsPageId > this->header.lastPageFreeSpacePageId) {
        Storage::StorageManager::Get().CreatePageFreeSpacePage(this->systemFilename, pfsPageId);
        this->header.lastPageFreeSpacePageId = pfsPageId;
      }
    }

    return true;
  }

  Pages::PageGuard<> VersionDatabase::TryGetLastUndoPage(
    const StorageTypes::Table *table,
    const row_size_t &size
  ) {
    page_id_t pageId;

    {
      MultiThreading::ReaderGuard lock(&this->lastUsedPageMutex);

      if (this->lastUsedPageId == INVALID_PAGE_ID)
        return {};

      pageId = this->lastUsedPageId;
    }

    auto lastUsedPage = Storage::StorageManager::Get().GetPage(this->filename, pageId, table);

    MultiThreading::ReaderGuard lastUsedPageLatch(&lastUsedPage->Latch());

    if (lastUsedPage->GetBytesLeft() >= size)
      return lastUsedPage;

    return {};
  }

  Pages::PageGuard<Pages::Page> VersionDatabase::CreateUndoPage(){
     extent_id_t newExtentId = 0;
     page_id_t newPageId = 0;

     this->AllocateNewExtent(newPageId, newExtentId);

     {
        MultiThreading::WriterGuard pageIdLock(&this->lastUsedPageMutex);
        this->lastUsedPageId = newPageId;
     }

     for (page_id_t pageId = newPageId; pageId < newPageId + EXTENT_SIZE; pageId++){
       auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFilename, pageId);

       MultiThreading::WriterGuard lock(&pageFreeSpacePage->Latch());

       auto undoPage = Storage::StorageManager::Get().CreatePage(this->filename, pageId);

       pageFreeSpacePage->SetPageMetaData(undoPage.Get());
     }

     return Storage::StorageManager::Get().GetPage(this->filename, newPageId, nullptr);
   }

  Pages::PageGuard<Pages::Page> VersionDatabase::GetLastUndoPage(const DatabaseEngine::StorageTypes::Table* table, const row_size_t &size) {
    const auto gamPage = Storage::StorageManager::Get().GetGlobalAllocationMapPage(this->systemFilename, this->header.lastGamPageId);

    auto cachedPage = this->TryGetLastUndoPage(table, size);
    if (cachedPage.IsValid())
      return cachedPage;

    for (const auto &extentId : gamPage->GetAllocatedExtents())
    {
      const page_id_t firstExtentPageId = Database::CalculateExtentFirstPageId(extentId);

      for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + EXTENT_SIZE; pageId++)
      {
         {
            const page_id_t correspondingPfsPageId = Database::GetPfsAssociatedPage(pageId);
            const auto pageFreeSpace = Storage::StorageManager::Get().GetPageFreeSpacePage(this->systemFilename, correspondingPfsPageId);

            MultiThreading::ReaderGuard pfsLock(&pageFreeSpace->Latch());

            const auto categorySize = Database::GetObjectSizeToCategory(size);

            if(pageFreeSpace->GetPageSizeCategory(pageId) <= categorySize)
              continue;
         }

         auto undoPage = Storage::StorageManager::Get().GetPage(this->filename, pageId, table);

         MultiThreading::ReaderGuard undoLatch(&undoPage->Latch());

         if (undoPage->GetBytesLeft() >= size) {
           {
             MultiThreading::WriterGuard lock(&this->lastUsedPageMutex);
             this->lastUsedPageId = undoPage->GetPageId();
           }

           return undoPage;
         }
      }
    }

    return this->CreateUndoPage();
  }

  Errors::RuntimeStatus VersionDatabase::InsertRow(
    const StorageTypes::Row* row,
    Pages::RowVersionPointer& rowPointer,
    const StorageTypes::Table* table
  ) {
    auto* oldRow = new StorageTypes::Row(row);

    auto page = this->GetLastUndoPage(table, row->TotalSize());

     MultiThreading::WriterGuard lock(&page->Latch());

     int offset = 0;
     page->InsertRow(oldRow, &offset);

     rowPointer.pageId = page->GetPageId();
     rowPointer.offset = offset;

     return {};
 }

  StorageTypes::Row VersionDatabase::RetrieveRow(
    const Snapshot& snapshot,
    const Pages::RowVersionPointer &rowPointer,
    const StorageTypes::Table *table
  )const {

    StorageTypes::Row row;

    {
      auto page = Storage::StorageManager::Get().GetPage(this->filename, rowPointer.pageId, table);

      MultiThreading::ReaderGuard lock(&page->Latch());

      // row = page->GetRow(rowPointer.offset);
    }

    return row.GetVisibleVersionForTransaction(snapshot);
  }

  std::vector<extent_id_t> VersionDatabase::GetAllocatedExtents(const extent_id_t& startingExtentId) const {
    auto gamPage = Storage::StorageManager::Get().GetGlobalAllocationMapPage(this->systemFilename, this->header.lastGamPageId);

    return gamPage->GetAllocatedExtents(startingExtentId);
  }

  extent_id_t VersionDatabase::CleanupVersionedData(const transaction_id_t &transactionId, const extent_id_t& startingExtentId)const {
    const auto extents = this->GetAllocatedExtents(startingExtentId);

    for (const auto &extentId : extents){
      const auto firstExtentPageId = Database::CalculateExtentFirstPageId(extentId * EXTENT_SIZE);

      bool isExtentEmpty = true;
      for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + EXTENT_SIZE; pageId++){
        auto pfsPage = Database::GetAssociatedPfsPage(this->systemFilename, pageId);

        {
          MultiThreading::ReaderGuard pfsReaderLock(&pfsPage->Latch());

          if (!pfsPage->IsPageAllocated(pageId))
            continue;
        }

        auto page = Storage::StorageManager::Get().GetPage(this->filename, pageId, nullptr);

        MultiThreading::WriterGuard pageLatch(&page->Latch());

        for (int i = 0; i < page->GetPageSize(); i++) {
          auto row = page->GetRow(nullptr, i);
          const auto& versionHeader = row.GetVersionHeader();

          if (transactionId == FIRST_TRANSACTION_ID
            || versionHeader.createdTransactionId <= transactionId) {
            page->Delete(i);
            i--;
          }
        }

        if (page->GetPageSize() == 0) {
          MultiThreading::WriterGuard pfsWriterLock(&pfsPage->Latch());
          pfsPage->SetPageFreed(pageId);

          continue;
        }

        isExtentEmpty = false;
      }

      if (isExtentEmpty) {
        auto gamPage = Storage::StorageManager::Get().GetGlobalAllocationMapPage(this->systemFilename, this->header.lastGamPageId);

        MultiThreading::WriterGuard gamLock(&gamPage->Latch());

        gamPage->DeallocateExtent(extentId);
      }
    }

    return extents.empty() ? 0 : extents.back() + 1;
  }
}