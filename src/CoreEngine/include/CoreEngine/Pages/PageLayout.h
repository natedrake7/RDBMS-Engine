#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <meta>

namespace CoreEngine::Pages{
    class PageView;
    class HeaderPageView;
    class PageFreeSpaceView;
    class GlobalAllocationPageView;
}

namespace CoreEngine::PageLayout{
    enum class PageType : UnsignedSmallInt{
        DATA = 0,
        ALLOCATION = 1,
        LOB_ROOT = 2,
        INDEXED = 3,
        HEADER = 4,
        GLOBAL_ALLOCATION = 5,
        FREE_SPACE = 6,
        OVERFLOW_TYPE = 7,
        LOB_INDEX = 8,
        LOB_DATA = 9
    };

    template<typename TView>
    consteval bool DeclaresOwnMetadata(){
        return std::meta::parent_of(^^TView::TYPE) == (^^TView)
            && std::meta::parent_of(^^TView::IS_SYSTEM) == (^^TView); // a view that forgets IS_SYSTEM would land in the data file
    }

    template<typename TView>
    concept PageViewType = std::derived_from<TView, Pages::PageView> && DeclaresOwnMetadata<TView>();

    template<PageViewType TView>
    consteval bool IsSystemPage(){ return TView::IS_SYSTEM; }

    enum class PagePriority : UnsignedTinyInt{
        LOW = 0,
        MEDIUM = 1,
        HIGH = 2,
        SYSTEM = 3,
    };
}
