#include "VersionDatabase.h"

#include "../../Systemic/MultiThreading/Guards/ReaderGuard/ReaderGuard.h"
#include "../../Systemic/MultiThreading/Guards/WriterGuard/WriterGuard.h"
#include "../Storage/StorageManager/StorageManager.h"
#include "../Pages/Header/HeaderPage.h"
#include "../Pages/GlobalAllocationMap/GlobalAllocationMapPage.h"
#include "../Pages/PageFreeSpace/PageFreeSpacePage.h"

namespace DatabaseEngine {
 VersionDatabase::VersionDatabase(const std::string &filename){
   this->PopulateFilenames(filename);

   const auto headerPage = Storage::StorageManager::Get().GetHeaderPage(this->systemFilename);

   this->header = *headerPage->GetDatabaseHeader();
 }

  VersionDatabase::~VersionDatabase() = default;

  string VersionDatabase::CreateDatabasePath(const string & dbName){ return dbName + "/" + dbName; }

  void VersionDatabase::PopulateFilenames(const std::string& dbName){
     const auto& path = Database::CreateDatabasePath(dbName);

     this->filename = path + ".db";
     this->fileExtension = ".db";
     this->name = dbName;
     this->systemFilename = path + "_sys" + ".db";
   }

  void VersionDatabase::WriteHeaderToFile() const
 {
   auto metaDataPage = Storage::StorageManager::Get().GetHeaderPage(this->systemFilename);

   metaDataPage->SetDbHeader(this->header);
 }

  bool VersionDatabase::AllocateNewExtent(page_id_t *newPageId, extent_id_t *newExtentId) {
        auto gamPage = Storage::StorageManager::Get().GetGlobalAllocationMapPage(this->systemFilename, this->header.lastGamPageId);

        if (gamPage->IsFull())
        {
            gamPage = Storage::StorageManager::Get().CreateGlobalAllocationMapPage(this->systemFilename, gamPage->GetPageId() + GAM_NUMBER_OF_PAGES);

            *newExtentId = gamPage->AllocateExtent();

            *newPageId = Database::CalculateSystemPageOffsetByExtentId(*newExtentId);
        }
        else
        {
            *newExtentId = gamPage->AllocateExtent();
            *newPageId = Database::CalculateSystemPageOffsetByExtentId(*newExtentId);
        }

        const auto pfsPageId = Database::GetPfsAssociatedPage(*newPageId);

        if(pfsPageId > this->header.lastPageFreeSpacePageId){
            Storage::StorageManager::Get().CreatePageFreeSpacePage(this->systemFilename, pfsPageId);
            this->header.lastPageFreeSpacePageId = pfsPageId;
        }

        const auto gamPageId = Database::GetGamAssociatedPage(*newPageId);

        if(gamPageId > gamPage->GetPageId()){
            gamPage = Storage::StorageManager::Get().CreateGlobalAllocationMapPage(this->filename, gamPageId);
            this->header.lastGamPageId = gamPageId;
        }

      return true;
 }

  Pages::PageGuard<Pages::Page> VersionDatabase::CreateUndoPage(){
     extent_id_t newExtentId = 0;
     page_id_t newPageId = 0;

    this->AllocateNewExtent(&newPageId, &newExtentId);

     for (page_id_t pageId = newPageId; pageId < newPageId + EXTENT_SIZE; pageId++){
       auto pageFreeSpacePage = Database::GetAssociatedPfsPage(this->systemFilename, pageId);

       MultiThreading::WriterGuard lock(&pageFreeSpacePage->GetLatch());

       auto undoPage = Storage::StorageManager::Get().CreatePage(this->filename, pageId);

       pageFreeSpacePage->SetPageMetaData(undoPage.Get());
     }

     return Storage::StorageManager::Get().GetPage(this->filename, newPageId, nullptr);
   }

  Pages::PageGuard<Pages::Page> VersionDatabase::GetLastUndoPage(const DatabaseEngine::StorageTypes::Table* table, const row_size_t &size) {
    const auto gamPage = Storage::StorageManager::Get().GetGlobalAllocationMapPage(this->systemFilename, this->header.lastGamPageId);

    for (const auto &extentId : gamPage->GetAllocatedExtents())
    {
      const page_id_t firstExtentPageId = Database::CalculateSystemPageOffsetByExtentId(extentId);

      for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + EXTENT_SIZE; pageId++)
      {
         {
            const page_id_t correspondingPfsPageId = Database::GetPfsAssociatedPage(pageId);
            const auto pageFreeSpace = Storage::StorageManager::Get().GetPageFreeSpacePage(this->systemFilename, correspondingPfsPageId);

            MultiThreading::ReaderGuard pfsLock(&pageFreeSpace->GetLatch());

            const auto categorySize = Database::GetObjectSizeToCategory(size);

            if(pageFreeSpace->GetPageSizeCategory(pageId) <= categorySize)
              continue;
         }

         auto undoPage = Storage::StorageManager::Get().GetPage(this->filename, pageId, table);

         MultiThreading::ReaderGuard undoLatch(&undoPage->GetLatch());

         if (undoPage->GetBytesLeft() >= size)
           return undoPage;
      }
    }

    return this->CreateUndoPage();
  }

  Errors::RuntimeStatus VersionDatabase::InsertRow(
    const StorageTypes::Row *row,
    Pages::RowVersionPointer& rowPointer,
    const DatabaseEngine::StorageTypes::Table* table
  ) {
   auto* oldRow = new StorageTypes::Row(row);

   auto undoPage =  this->GetLastUndoPage(table, row->GetTotalRowSize());

   MultiThreading::WriterGuard lock(&undoPage->GetLatch());

   int offset = 0;
   undoPage->InsertRow(oldRow, &offset);

   rowPointer.pageId = undoPage->GetPageId();
   rowPointer.offset = offset;

   return {};
 }

  std::vector<extent_id_t> VersionDatabase::GetAllocatedExtents(const Constants::extent_id_t& startingExtentId) const {
    auto gamPage = Storage::StorageManager::Get().GetGlobalAllocationMapPage(this->systemFilename, this->header.lastGamPageId);

    return gamPage->GetAllocatedExtents(startingExtentId);
  }

  Constants::extent_id_t VersionDatabase::CleanupVersionedData(const Constants::transaction_id_t &transactionId, const Constants::extent_id_t& startingExtentId)const {
    const auto extents = this->GetAllocatedExtents(startingExtentId);

    for (const auto &extentId : extents){
      const page_id_t firstExtentPageId = Database::CalculateSystemPageOffsetByExtentId(extentId);

      bool isExtentEmpty = true;
      for (page_id_t pageId = firstExtentPageId; pageId < firstExtentPageId + EXTENT_SIZE; pageId++){
        auto pfsPage = Database::GetAssociatedPfsPage(this->systemFilename, pageId);

        {
          MultiThreading::ReaderGuard pfsReaderLock(&pfsPage->GetLatch());

          if (!pfsPage->IsPageAllocated(pageId))
            continue;
        }

        auto page = Storage::StorageManager::Get().GetPage(this->filename, pageId, nullptr);

        MultiThreading::WriterGuard pageLatch(&page->GetLatch());

        for (int i = 0; i < page->GetPageSize(); i++) {
          const auto* row = page->GetRow(i);

          const auto& versionHeader = row->GetVersionHeader();

          if (versionHeader.createdTransactionId <= transactionId) {
            page->Delete(i);
            i--;
          }
        }

        if (page->GetPageSize() == 0) {
          MultiThreading::WriterGuard pfsWriterLock(&pfsPage->GetLatch());
          pfsPage->SetPageFreed(pageId);

          continue;
        }

        isExtentEmpty = false;
      }

      if (isExtentEmpty) {
        auto gamPage = Storage::StorageManager::Get().GetGlobalAllocationMapPage(this->systemFilename, this->header.lastGamPageId);

        MultiThreading::WriterGuard gamLock(&gamPage->GetLatch());

        gamPage->DeallocateExtent(extentId);
      }

    }

    return extents.size() > 0 ? extents.back() + 1 : 0;
  }
}