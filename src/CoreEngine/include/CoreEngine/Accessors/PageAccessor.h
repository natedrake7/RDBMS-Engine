#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <CoreEngine/BufferPool/StorageManager.h>
#include <CoreEngine/BufferPool/StorageManager.Templates.h>


namespace CoreEngine::Accessors{
    struct PageAccessor{
        buffer_pool_slot_t _dataSlot;
        buffer_pool_slot_t _systemSlot;

        friend class Storage::StorageManager;

        PageAccessor() = default;
        explicit PageAccessor(const buffer_pool_slot_t dataSlot, const buffer_pool_slot_t systemSlot)
            : _dataSlot(dataSlot), _systemSlot(systemSlot){}

        PageAccessor(const PageAccessor&) = default;
        PageAccessor& operator=(const PageAccessor&) = default;
        PageAccessor(PageAccessor&&) = default;
        PageAccessor& operator=(PageAccessor&&) = default;

        template<PageLayout::PageViewType TView>
        [[nodiscard]] inline TView GetPage(const page_id_t pageId)const{
            if constexpr (PageLayout::IsSystemPage<TView>())
                return Storage::StorageManager::Get().GetPage<TView>(this->_systemSlot, pageId);
            else
                return Storage::StorageManager::Get().GetPage<TView>(this->_dataSlot, pageId);
        }

        [[nodiscard]] Pages::PageFreeSpaceView GetAssociatedPfsPage(const page_id_t pageId) const{
            return this->GetPage<Pages::PageFreeSpaceView>(StorageLayout::PfsPageId(pageId));
        }

        template<typename  TView, typename... Args>
        TView CreatePage(page_id_t pageId, Args&&... args) const{
            if constexpr (PageLayout::IsSystemPage<TView>())
                return Storage::StorageManager::Get().CreatePage<TView>(this->_systemSlot, pageId, std::forward<Args>(args)...);
            else
                return Storage::StorageManager::Get().CreatePage<TView>(this->_dataSlot, pageId, std::forward<Args>(args)...);
        }

        void OpenDataFile(const DataTypes::StringView& filename, const ::Storage::FileOpenMode mode)const{
            Storage::StorageManager::Get().OpenFile(this->_dataSlot, filename, mode);
        }

        void OpenSystemFile(const DataTypes::StringView& filename, const ::Storage::FileOpenMode mode)const{
                Storage::StorageManager::Get().OpenFile(this->_systemSlot, filename, mode);
        }

        template<bool Flush>
        void ReleasePageTables() const{
            Storage::StorageManager::Get().ReleasePageTables<Flush>(*this);
        }
    };

    static_assert(std::is_trivially_copyable_v<PageAccessor>, "PageAccessor must be lightweight and trivially copiable");
    static_assert(sizeof(PageAccessor) < sizeof(void*), "PageAccessor must be more lightweight than a pointer");
}
