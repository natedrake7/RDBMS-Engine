#pragma once
#include "PageView.h"

namespace Pages{
    class OverflowPageView final : public PageView{
        public:
            OverflowPageView();
            explicit OverflowPageView(Frame* frame);
    };
}
