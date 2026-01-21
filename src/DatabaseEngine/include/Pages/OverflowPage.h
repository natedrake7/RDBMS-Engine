#pragma  once
#include "Page.h"
#include "LargeObjectPage.h"
namespace Pages {

  struct OverflowRow : LargeDataObject{
    page_offset_t index;

    OverflowRow();
    block_size_t GetSize()const;
  };

  struct OverflowPointer : DataObjectPointer{
    page_offset_t index;

    OverflowPointer();
    explicit OverflowPointer(page_id_t pageId, page_offset_t index);
    ~OverflowPointer();
  };

  class OverflowPage final : public Page{
    std::vector<OverflowRow*> data;

  public:
    explicit OverflowPage();
    explicit OverflowPage(page_id_t pageId, bool isPageCreation = false);
    explicit OverflowPage(const PageHeader &pageHeader);
    void ReadFromDisk(const vector<char>& data, const DatabaseEngine::StorageTypes::Table* table, page_offset_t& offSet, fstream* filePtr) override;
    void WriteToDisk(fstream* filePtr) override;
    void UpdateBytesLeft() override;
    OverflowRow* InsertObject(const object_t* object, page_size_t size, Int& indexPos);
    [[nodiscard]] OverflowRow* GetObject(page_offset_t index)const;
    [[nodiscard]] OverflowRow* DeleteObject(page_offset_t index);
  };

} // Pages
