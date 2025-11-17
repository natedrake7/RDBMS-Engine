#include "UndoPage.h"

namespace Pages {
 UndoPage::UndoPage(const page_id_t &pageId, const bool &isPageCreation) : Page(pageId, isPageCreation) {
  this->header.pageType = PageType::UNDO;
 }

 UndoPage::UndoPage() : Page(){
  this->header.pageType = PageType::UNDO;
 }

 UndoPage::UndoPage(const PageHeader &pageHeader) : Page(pageHeader){}

 UndoPage::~UndoPage() = default;

}