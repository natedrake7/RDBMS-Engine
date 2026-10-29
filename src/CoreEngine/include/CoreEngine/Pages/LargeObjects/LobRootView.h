#pragma once

#include <CoreEngine/Pages/PageView.h>
#include <CoreEngine/Pages/Additional/LobPageStructs.h>

namespace CoreEngine::Pages{
    class LobRootView final: public PageView{
        [[nodiscard]] LobRootHeader* GetAdditionalHeader() const{
            return reinterpret_cast<LobRootHeader*>(this->_frame->_data + StorageLayout::PAGE_HEADER_SIZE);
        }

        [[nodiscard]] object_t* Payload()const{
            return this->_frame->_data + StorageLayout::PAGE_HEADER_SIZE + sizeof(LobRootHeader);
        }

        public:
            static constexpr auto TYPE = PageLayout::PageType::LOB_ROOT;
            static constexpr bool IS_SYSTEM = false;

            static void Initialize(const Frame* frame, const UnsignedBigInt totalLength){
                frame->Header()->_bytesLeft = 0;
                auto* additionalHeader = reinterpret_cast<LobRootHeader*>(frame->_data + StorageLayout::PAGE_HEADER_SIZE);

                additionalHeader->_totalLength = totalLength;
                additionalHeader->_companionRoot = INVALID_PAGE_ID;
                additionalHeader->_childCount = 0;
                additionalHeader->_level = LobLayout::LevelFor(totalLength);
                additionalHeader->_flags = 0;
            }

            LobRootView() = default;
            explicit LobRootView(Frame* framePtr);

            LobRootView(LobRootView&&) noexcept = default;
            LobRootView& operator=(LobRootView&&) noexcept = default;

            [[nodiscard]] UnsignedBigInt TotalLength() const{
                return this->GetAdditionalHeader()->_totalLength;
            }

            // The root is created before the length is known, so the level must follow the final length
            void SetTotalLength(const UnsignedBigInt totalLength) const{
                auto* additionalHeader = this->GetAdditionalHeader();
                additionalHeader->_totalLength = totalLength;
                additionalHeader->_level = LobLayout::LevelFor(totalLength);
                this->_frame->MarkDirty();
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
