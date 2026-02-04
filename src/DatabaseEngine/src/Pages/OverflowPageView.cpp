#include "../../include/Pages/OverflowPageView.h"
namespace Pages{
    OverflowPageView::OverflowPageView() : PageView() {
        this->type = Constants::PageType::OVERFLOWTYPE;
    }

    OverflowPageView::OverflowPageView(Frame* frame) : PageView(frame) {
        this->type = Constants::PageType::OVERFLOWTYPE;
    }

    OverflowPageView::OverflowPageView(OverflowPageView&& other) noexcept{
        this->headerPtr = other.headerPtr;
        this->framePtr = other.framePtr;

        other.headerPtr = nullptr;
        other.framePtr = nullptr;
    }

    OverflowPageView& OverflowPageView::operator=(OverflowPageView&& other) noexcept{
        if(this == &other)
            return *this;

        this->headerPtr = other.headerPtr;
        this->framePtr = other.framePtr;

        other.headerPtr = nullptr;
        other.framePtr = nullptr;

        return *this;
    }
}
