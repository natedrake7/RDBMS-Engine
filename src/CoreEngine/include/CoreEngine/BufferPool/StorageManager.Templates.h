#pragma once
#include <CoreEngine/BufferPool/StorageManager.h>
#include <CoreEngine/Pages/PageLayout.h>
#include <CoreEngine/Pages/Additional/IndexPageStructs.h>

namespace Pages{
    class OverflowPageView;
    class LobDataView;
    class LobIndexView;
    class LobRootView;
    class IndexPageView;
}

namespace Storage{
    template <typename TView>
    constexpr CoreEngine::PageLayout::PageType StorageManager::DeducePageType(){
        if constexpr (std::is_same_v<TView, Pages::PageView>)
            return CoreEngine::PageLayout::PageType::DATA;
        else if constexpr (std::is_same_v<TView, Pages::IndexPageView>)
            return CoreEngine::PageLayout::PageType::INDEXED;
        else if constexpr (std::is_same_v<TView, Pages::LobRootView>)
            return CoreEngine::PageLayout::PageType::LOB_ROOT;
        else if constexpr (std::is_same_v<TView, Pages::LobIndexView>)
            return CoreEngine::PageLayout::PageType::LOB_INDEX;
        else if constexpr (std::is_same_v<TView, Pages::LobDataView>)
            return CoreEngine::PageLayout::PageType::LOB_DATA;
        else if constexpr (std::is_same_v<TView, Pages::OverflowPageView>)
            return CoreEngine::PageLayout::PageType::OVERFLOW_TYPE;
        else
            static_assert(DataTypes::AlwaysFalse<TView>, "Invalid Page Type");

        return CoreEngine::PageLayout::PageType::DATA;
    }

    template<typename TView>
    TView StorageManager::CreatePage(
        const buffer_pool_slot_t slotIndex,
        const page_id_t pageId
    ){
        auto* frame = this->CreateFrame(slotIndex, pageId, StorageManager::DeducePageType<TView>());
        if constexpr (std::is_same_v<TView, Pages::PageView>)
            frame->Header()->bytesLeft = CoreEngine::StorageLayout::PAGE_SIZE_WITHOUT_HEADER;
        else if constexpr (std::is_same_v<TView, Pages::IndexPageView>){
            frame->Header()->bytesLeft = CoreEngine::StorageLayout::INDEX_PAGE_DEFAULT_SIZE;
            auto* additionalHeader = reinterpret_cast<Pages::IndexPageAdditionalHeader*>(frame->_data + CoreEngine::StorageLayout::PAGE_HEADER_SIZE);
            additionalHeader->nextNode = INVALID_PAGE_ID;
            additionalHeader->previousNode = INVALID_PAGE_ID;
        }
        //LOB Pages don't track available space through pageHeader.
        else if constexpr (
            std::is_same_v<TView, Pages::LobDataView>
            || std::is_same_v<TView, Pages::LobIndexView>
            || std::is_same_v<TView, Pages::LobRootView>
        ) frame->Header()->bytesLeft = 0;
        else if constexpr (std::is_same_v<TView, Pages::OverflowPageView>)
            frame->Header()->bytesLeft = CoreEngine::StorageLayout::PAGE_SIZE_WITHOUT_HEADER;
        else
            static_assert(DataTypes::AlwaysFalse<TView>, "Invalid Page Type");

        return TView(frame);
    }

    template <typename TView>
    TView StorageManager::GetPage(
        const buffer_pool_slot_t slotIndex,
        const page_id_t pageId
    ){
        auto* frame = this->GetFrame(slotIndex, pageId);
        return TView(frame);
    }
}
