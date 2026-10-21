#pragma once
#include <CoreEngine/Pages/PageView.h>

namespace CoreEngine::Pages{
    struct OverflowPtr{
        page_id_t _pageId;
        page_offset_t _index;

        OverflowPtr() = default;
        OverflowPtr(const page_id_t pageId, const page_offset_t index)
            : _pageId(pageId), _index(index) {}
    };

    class OverflowPageView final : public PageView{
    public:

        static constexpr auto TYPE = PageLayout::PageType::OVERFLOW_TYPE;
        static constexpr bool IS_SYSTEM = false;

        static void Initialize(const Frame* frame){}

        OverflowPageView();
        explicit OverflowPageView(Frame* frame);

        OverflowPageView(OverflowPageView&& other) noexcept = default;
        OverflowPageView& operator=(OverflowPageView&& other) noexcept = default;

        [[nodiscard]] OverflowPtr Insert(const object_t* data, Int size) const;
        [[nodiscard]] const object_t* Get(Int index, Int& outSize) const;
    };
}
