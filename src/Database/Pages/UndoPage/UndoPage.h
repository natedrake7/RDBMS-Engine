#pragma once
#include "../Page.h"


namespace Pages{

  class UndoPage final : public Page{

    public:
      explicit UndoPage(const page_id_t &pageId, const bool &isPageCreation = false);
      explicit UndoPage();
      explicit UndoPage(const PageHeader &pageHeader);
       ~UndoPage()override;
  };
}