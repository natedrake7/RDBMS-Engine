#pragma once

#include <Systemic/DataTypes/DataTypes.h>
#include <CoreEngine/BufferPool/StorageManager.h>


namespace CoreEngine::Accessors{
    struct PageAccessor{
        buffer_pool_slot_t _dataSlot;
        buffer_pool_slot_t _systemSlot;

        friend class StorageManager;                          // the only place that can mint one


        template<typename TView>
        [[nodiscard]] inline TView Get(page_id_t pageId){
            if constexpr (PageLayout::IsSystemPage<TView>())
                return Storage::StorageManager::Get().GetPage<TView>(this->_systemSlot, pageId);
            else
                return Storage::StorageManager::Get().GetPage<TView>(this->_dataSlot, pageId);
        }

        template<typename  TView, typename... Args>
        [[nodiscard]] TView Create(page_id_t pageId, Args&&... args) const;
    };
}
