#include "../../include/Pages/OverflowPageView.h"
namespace Pages{
    OverflowPageView::OverflowPageView() : PageView() {}

    OverflowPageView::OverflowPageView(Frame* frame) : PageView(frame) {}

    OverflowPageView::OverflowPageView(OverflowPageView&& other) noexcept{
        this->framePtr = other.framePtr;
        other.framePtr = nullptr;
    }

    OverflowPageView& OverflowPageView::operator=(OverflowPageView&& other) noexcept{
        if(this == &other)
            return *this;

        this->framePtr = other.framePtr;
        other.framePtr = nullptr;

        return *this;
    }
}
