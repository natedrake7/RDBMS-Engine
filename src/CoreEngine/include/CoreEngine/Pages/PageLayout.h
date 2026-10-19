#pragma once
#include <Systemic/DataTypes/DataTypes.h>

namespace Pages{
    class PageFreeSpaceView;
    class HeaderPageView;
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
    constexpr bool IsSystemPage(){
        return std::is_same_v<TView, Pages::GlobalAllocationPageView>
            || std::is_same_v<TView, Pages::PageFreeSpaceView>
            || std::is_same_v<TView, Pages::HeaderPageView>;
    }

    enum class PagePriority : UnsignedTinyInt{
        LOW = 0,
        MEDIUM = 1,
        HIGH = 2,
        SYSTEM = 3,
    };
}
