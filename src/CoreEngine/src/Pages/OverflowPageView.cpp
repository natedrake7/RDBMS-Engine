#include "../../include/Pages/OverflowPageView.h"
namespace Pages{
    OverflowPageView::OverflowPageView() : PageView() {}

    OverflowPageView::OverflowPageView(Frame* frame) : PageView(frame) {}

    OverflowPageView::OverflowPageView(OverflowPageView&& other) noexcept{
        this->_frame = other._frame;
        other._frame = nullptr;
    }

    OverflowPageView& OverflowPageView::operator=(OverflowPageView&& other) noexcept{
        if(this == &other)
            return *this;

        this->_frame = other._frame;
        other._frame = nullptr;

        return *this;
    }
}
