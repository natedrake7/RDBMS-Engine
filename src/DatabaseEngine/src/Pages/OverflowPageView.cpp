#include "../../include/Pages/OverflowPageView.h"
namespace Pages{
    OverflowPageView::OverflowPageView() : PageView() {
        this->type = Constants::PageType::OVERFLOWTYPE;
    }

    OverflowPageView::OverflowPageView(Frame* frame) : PageView(frame) {
        this->type = Constants::PageType::OVERFLOWTYPE;
    }
}
