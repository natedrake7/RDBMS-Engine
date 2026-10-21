#pragma once

#include <Systemic/DataTypes/DataTypes.h>
#include <CoreEngine/BufferPool/StorageManager.h>


namespace CoreEngine::Accessors{
    struct PageAccessor{
        buffer_pool_slot_t _dataSlot;
        buffer_pool_slot_t _systemSlot;

        friend class Storage::StorageManager;

        explicit PageAccessor(const buffer_pool_slot_t dataSlot, const buffer_pool_slot_t systemSlot)
            : _dataSlot(dataSlot), _systemSlot(systemSlot){}

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
