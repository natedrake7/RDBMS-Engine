#pragma once
#include "../PageView.h"
#include "../Additional/LobPageStructs.h"

namespace Pages{
    class LobIndexView : public PageView{
        [[nodiscard]] LobIndexHeader* GetAdditionalHeader() const{
            return reinterpret_cast<LobIndexHeader*>(this->_frame->_data + Constants::PAGE_HEADER_SIZE);
        }

        [[nodiscard]] object_t* Payload()const{
            return this->_frame->_data + Constants::PAGE_HEADER_SIZE + sizeof(LobIndexHeader);
        }

    public:
        LobIndexView() = default;
        explicit LobIndexView(Frame* framePtr);

        LobIndexView(LobIndexView&&) noexcept = default;
        LobIndexView& operator=(LobIndexView&&) noexcept = default;

        void Initialize(page_id_t rootPageId) const;

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
