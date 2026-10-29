#pragma once
#include <CoreEngine/Pages/PageView.h>
#include <CoreEngine/Pages/Additional/LobPageStructs.h>

namespace CoreEngine::Pages{
    class LobIndexView final: public PageView{
        [[nodiscard]] LobIndexHeader* GetAdditionalHeader() const{
            return reinterpret_cast<LobIndexHeader*>(this->_frame->_data + StorageLayout::PAGE_HEADER_SIZE);
        }

        [[nodiscard]] object_t* Payload()const{
            return this->_frame->_data + StorageLayout::PAGE_HEADER_SIZE + sizeof(LobIndexHeader);
        }

    public:
        static constexpr auto TYPE = PageLayout::PageType::LOB_INDEX;
        static constexpr bool IS_SYSTEM = false;

        static void Initialize(const Frame* frame, const page_id_t rootPageId){
            frame->Header()->_bytesLeft = 0;

            auto* additionalHeader = reinterpret_cast<LobIndexHeader*>(frame->_data + StorageLayout::PAGE_HEADER_SIZE);
            additionalHeader->_rootPageId = rootPageId;
            additionalHeader->_childCount = 0;
        }

        LobIndexView() = default;
        explicit LobIndexView(Frame* framePtr);

        LobIndexView(LobIndexView&&) noexcept = default;
        LobIndexView& operator=(LobIndexView&&) noexcept = default;

        [[nodiscard]] UnsignedInt ChildCount() const{
            return this->GetAdditionalHeader()->_childCount;
        }

        [[nodiscard]] page_id_t RootPageId()const{
            return this->GetAdditionalHeader()->_rootPageId;
        }

        [[nodiscard]] const object_t* InlineData()const{
            return this->Payload();
        }

        [[nodiscard]] page_id_t Child(UnsignedInt slot)const;
        void AppendChild(page_id_t child) const;

        void BulkAppendChildren(const page_id_t* children, UnsignedInt count) const;

        [[nodiscard]] bool IsConsistent() const;
    };
}
