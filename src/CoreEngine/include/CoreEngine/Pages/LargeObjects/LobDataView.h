#pragma once
#include <CoreEngine/Pages/PageView.h>
#include <CoreEngine/Pages/Additional/LobPageStructs.h>

namespace CoreEngine::Pages{
    class LobDataView final: public PageView{
        [[nodiscard]] LobDataHeader* GetAdditionalHeader() const{
            return reinterpret_cast<LobDataHeader*>(this->_frame->_data + StorageLayout::PAGE_HEADER_SIZE);
        }

        [[nodiscard]] object_t* Payload()const{
            return this->_frame->_data + StorageLayout::PAGE_HEADER_SIZE + sizeof(LobDataHeader);
        }

        public:
            static constexpr auto TYPE = PageLayout::PageType::LOB_DATA;
            static constexpr bool IS_SYSTEM = false;

            static void Initialize(const Frame* frame, const page_id_t rootPageId, const UInt32_t pageIndex){
                frame->Header()->_bytesLeft = 0;

                auto* additionalHeader = reinterpret_cast<LobDataHeader*>(frame->_data + StorageLayout::PAGE_HEADER_SIZE);
                additionalHeader->_rootPageId = rootPageId;
                additionalHeader->_pageIndex = pageIndex;
            }

            LobDataView() = default;
            explicit LobDataView(Frame* framePtr);

            LobDataView(LobDataView&& other) noexcept = default;
            LobDataView& operator=(LobDataView&& other) noexcept = default;\

            [[nodiscard]] page_id_t RootPageId()const{
                return this->GetAdditionalHeader()->_rootPageId;
            }

            [[nodiscard]] UInt32_t PageIndex()const{
                return this->GetAdditionalHeader()->_pageIndex;
            }

            [[nodiscard]] const object_t* InlineData()const{
                return this->Payload();
            }

            void WriteInline(UInt32_t offset, const object_t* data, UInt32_t size) const;
    };
}
