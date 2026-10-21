#pragma once
#include <CoreEngine/BufferPool/StorageManager.h>
#include <CoreEngine/Pages/PageLayout.h>
#include <CoreEngine/Pages/Additional/IndexPageStructs.h>

#include "Systemic/Guards/WriterGuard.h"


namespace CoreEngine::Pages
{
    class OverflowPageView;
    class LobDataView;
    class LobIndexView;
    class LobRootView;
    class IndexPageView;
}

namespace Storage{
    template <typename TView>
    constexpr CoreEngine::PageLayout::PageType StorageManager::DeducePageType(){
        if constexpr (std::is_same_v<TView, CoreEngine::Pages::PageView>)
            return CoreEngine::PageLayout::PageType::DATA;
        else if constexpr (std::is_same_v<TView, CoreEngine::Pages::IndexPageView>)
            return CoreEngine::PageLayout::PageType::INDEXED;
        else if constexpr (std::is_same_v<TView, CoreEngine::Pages::LobRootView>)
            return CoreEngine::PageLayout::PageType::LOB_ROOT;
        else if constexpr (std::is_same_v<TView, CoreEngine::Pages::LobIndexView>)
            return CoreEngine::PageLayout::PageType::LOB_INDEX;
        else if constexpr (std::is_same_v<TView, CoreEngine::Pages::LobDataView>)
            return CoreEngine::PageLayout::PageType::LOB_DATA;
        else if constexpr (std::is_same_v<TView, CoreEngine::Pages::OverflowPageView>)
            return CoreEngine::PageLayout::PageType::OVERFLOW_TYPE;
        else
            static_assert(DataTypes::AlwaysFalse<TView>, "Invalid Page Type");

        return CoreEngine::PageLayout::PageType::DATA;
    }

    template<typename TView, typename... Args>
    TView StorageManager::CreatePage(
        const buffer_pool_slot_t slotIndex,
        const page_id_t pageId,
        Args&&... args
    ){
        static_assert(CoreEngine::PageLayout::DeclaresOwnType<TView>(), "StorageManager::CreatePage: each page must declare its own type");
        auto* frame = this->BeginCreateFrame(slotIndex, pageId);

        {
            MultiThreading::WriterGuard pageLock(&frame->_latch);

            std::memset(frame->_data, 0, CoreEngine::StorageLayout::PAGE_SIZE);

            auto* header = frame->Header();
            header->_pageId = pageId;
            header->SetType(TView::TYPE);
            TView::Initialize(frame, std::forward<Args>(args)...);
        }

        frame->MarkDirty();
        frame->MarkHasSecondChance();
        frame->SetPriority(CoreEngine::PageLayout::PagePriority::LOW);
        frame->SetState(CoreEngine::Pages::FrameState::Ready);
        frame->_state.notify_all();

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
