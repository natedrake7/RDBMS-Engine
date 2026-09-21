#pragma once

#include "../PageView.h"
#include "../Additional/LobPageStructs.h"

namespace Pages{
    class LobRootView : public PageView{
        [[nodiscard]] LobRootHeader* GetAdditionalHeader() const{
            return reinterpret_cast<LobRootHeader*>(this->_frame->_data + Constants::PAGE_HEADER_SIZE);
        }

        [[nodiscard]] object_t* Payload()const{
            return this->_frame->_data + Constants::PAGE_HEADER_SIZE + sizeof(LobRootHeader);
        }

        public:
            LobRootView() = default;
            explicit LobRootView(Frame* framePtr);

            LobRootView(LobRootView&&) noexcept = default;
            LobRootView& operator=(LobRootView&&) noexcept = default;

            void Initialize(UnsignedBigInt totalLength) const;

            [[nodiscard]] UnsignedBigInt TotalLength() const{
                return this->GetAdditionalHeader()->_totalLength;
            }

            [[nodiscard]] UnsignedInt ChildCount() const{
                return this->GetAdditionalHeader()->_childCount;
            }

            [[nodiscard]] UnsignedTinyInt Level()const{
                return this->GetAdditionalHeader()->_level;
            }

            [[nodiscard]] page_id_t CompanionRoot()const{
                return this->GetAdditionalHeader()->_companionRoot;
            }

            [[nodiscard]] const object_t* InlineData()const{
                return this->Payload();
            }

            void WriteInline(UnsignedInt offset, const object_t* data, UnsignedInt size) const;

            [[nodiscard]] page_id_t Child(UnsignedInt slot)const;
            void AppendChild(page_id_t child) const;

            void BulkAppendChildren(const page_id_t* children, UnsignedInt count) const;

            [[nodiscard]] bool IsConsistent() const;
    };
}
