#pragma once
#include <CoreEngine/Pages/PageView.h>

namespace CoreEngine{
    struct DatabaseHeader;

    namespace StorageTypes{
        struct TableHeader;
    }
}

namespace CoreEngine::Pages{
    struct Frame;

    class HeaderPageView final : public PageView{
        [[nodiscard]] object_t* DatabaseHeaderData() const;
        [[nodiscard]] object_t* GetTableHeaderData(UnsignedInt ordinal) const;

        public:
            static constexpr auto TYPE = PageLayout::PageType::HEADER;
            static constexpr bool IS_SYSTEM = true;

            static void Initialize(const Frame* frame){}

            explicit HeaderPageView(Frame* framePtr);

            HeaderPageView(HeaderPageView&& other) noexcept = default;
            HeaderPageView& operator=(HeaderPageView&& other) noexcept = default;

            [[nodiscard]] DatabaseHeader GetDatabaseHeader() const;

            [[nodiscard]] StorageTypes::TableHeader* GetTableHeaderPtr(Int ordinalPosition) const;

            void SetDatabaseHeader(const DatabaseHeader& header) const;
            void SetTableHeader(const StorageTypes::TableHeader& header, SmallInt ordinalPosition) const;
    };
}
