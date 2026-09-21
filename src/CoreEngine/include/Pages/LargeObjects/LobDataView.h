#pragma once
#include "../PageView.h"
#include "Pages/Additional/LobPageStructs.h"

namespace Pages{
    class LobDataView final : public PageView{
        [[nodiscard]] LobDataHeader* GetAdditionalHeader() const{
            return reinterpret_cast<LobDataHeader*>(this->_frame->_data + Constants::PAGE_HEADER_SIZE);
        }

        [[nodiscard]] object_t* Payload()const{
            return this->_frame->_data + Constants::PAGE_HEADER_SIZE + sizeof(LobDataHeader);
        }

        public:
            LobDataView() = default;
            explicit LobDataView(Frame* framePtr);

            LobDataView(LobDataView&& other) noexcept = default;
            LobDataView& operator=(LobDataView&& other) noexcept = default;

            void Initialize(page_id_t rootPageId, UnsignedInt pageIndex)const;

            [[nodiscard]] page_id_t RootPageId()const{
                return this->GetAdditionalHeader()->_rootPageId;
            }

            [[nodiscard]] UnsignedInt PageIndex()const{
                return this->GetAdditionalHeader()->_pageIndex;
            }

            [[nodiscard]] const object_t* InlineData()const{
                return this->Payload();
            }

            void WriteInline(UnsignedInt offset, const object_t* data, UnsignedInt size) const;
    };
}
