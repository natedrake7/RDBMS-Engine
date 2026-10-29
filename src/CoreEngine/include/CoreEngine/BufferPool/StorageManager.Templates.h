#pragma once
#include <CoreEngine/BufferPool/StorageManager.h>
#include <CoreEngine/Pages/PageLayout.h>
#include <CoreEngine/Pages/Additional/IndexPageStructs.h>

#include "Systemic/Guards/WriterGuard.h"

namespace CoreEngine::Storage{
    template<PageLayout::PageViewType TView, typename... Args>
    TView StorageManager::CreatePage(
        const buffer_pool_slot_t slotIndex,
        const page_id_t pageId,
        Args&&... args
    ){
        static_assert(PageLayout::DeclaresOwnMetadata<TView>(), "StorageManager::CreatePage: each page must declare its own metadata(isSystem and type)");
        auto* frame = this->BeginCreateFrame(slotIndex, pageId);

        {
            MultiThreading::WriterGuard pageLock(&frame->_latch);

            std::memset(frame->_data, 0, StorageLayout::PAGE_SIZE);

            auto* header = frame->Header();
            header->_pageId = pageId;
            header->SetType(TView::TYPE);
            TView::Initialize(frame, std::forward<Args>(args)...);
        }

        frame->MarkDirty();
        frame->MarkHasSecondChance();
        frame->SetPriority(PageLayout::PagePriority::LOW);
        frame->SetState(Pages::FrameState::Ready);
        frame->_state.notify_all();

        if constexpr (!std::is_same_v<TView, Pages::PageView>)
            assert(frame->Header()->Type() == TView::TYPE && "GetPage: page read through the wrong view");

        return TView(frame);
    }

    template<PageLayout::PageViewType TView>
    TView StorageManager::GetPage(
        const buffer_pool_slot_t slotIndex,
        const page_id_t pageId
    ){
        auto* frame = this->GetFrame(slotIndex, pageId);
        return TView(frame);
    }
}
