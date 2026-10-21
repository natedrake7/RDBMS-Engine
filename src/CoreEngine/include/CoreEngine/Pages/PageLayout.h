#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <meta>
#include <type_traits>

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
    constexpr bool IsSystemPage(){
        return std::is_same_v<TView, Pages::GlobalAllocationPageView>
            || std::is_same_v<TView, Pages::PageFreeSpaceView>
            || std::is_same_v<TView, Pages::HeaderPageView>;
    }

    template<typename TView>
    concept PageViewType =
        std::derived_from<TView, Pages::PageView>
        && requires {
            { TView::TYPE } -> std::convertible_to<PageLayout::PageType>;
            { TView::SYSTEM_FILE } -> std::convertible_to<bool>;
        };

    template<typename TView>
    consteval bool DeclaresOwnType(){
        return std::meta::parent_of(^^TView::TYPE) == ^^TView;
    }

    enum class PagePriority : UnsignedTinyInt{
        LOW = 0,
        MEDIUM = 1,
        HIGH = 2,
        SYSTEM = 3,
    };
}
