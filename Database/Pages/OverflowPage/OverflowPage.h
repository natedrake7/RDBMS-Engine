#pragma  once
#include "../Page.h"
#include "../LargeObject/LargeDataPage.h"
namespace Pages {

  struct OverflowRow : DataObject{
    page_offset_t index;

    OverflowRow();
    block_size_t GetSize();
  };

  struct OverflowPointer : DataObjectPointer{
    page_offset_t index;

    OverflowPointer();
    explicit OverflowPointer(const page_id_t& pageId, const page_offset_t& index);
    ~OverflowPointer();
  };

  class OverflowPage : public Page{
    std::vector<OverflowRow*> data;

  public:
    explicit OverflowPage();
    explicit OverflowPage(const page_id_t &pageId, const bool &isPageCreation = false);
    explicit OverflowPage(const PageHeader &pageHeader);
    void GetPageDataFromFile(const vector<char>& data, const DatabaseEngine::StorageTypes::Table* table, page_offset_t& offSet, fstream* filePtr) override;
    void WritePageToFile(fstream* filePtr) override;
    void UpdateBytesLeft() override;
    [[nodiscard]] OverflowRow* InsertObject(const object_t* object, const page_size_t& size, int& indexPos);
    [[nodiscard]] OverflowRow* GetObject(const page_offset_t& index);
    [[nodiscard]] OverflowRow* DeleteObject(const page_offset_t& index);
  };

} // Pages
