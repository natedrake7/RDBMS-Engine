#pragma once
#include "PageView.h"

namespace CoreEngine{
    struct DatabaseHeader;

    namespace StorageTypes{
        struct TableHeader;
    }
}

namespace Pages{
    struct Frame;

    class HeaderPageView final : public PageView{
        CoreEngine::DatabaseHeader* databaseHeaderPtr;

        [[nodiscard]] inline object_t* GetTableHeaderDataOffset(Int ordinalPosition) const;

        public:
            explicit HeaderPageView(Frame* framePtr);

            HeaderPageView(HeaderPageView&& other) noexcept;
            HeaderPageView& operator=(HeaderPageView&& other) noexcept;

            [[nodiscard]] CoreEngine::DatabaseHeader* GetDatabaseHeaderPtr() const;

            // [[nodiscard]] const std::vector<CoreEngine::StorageTypes::TableHeader>& GetTableHeaders()const;
            [[nodiscard]] CoreEngine::StorageTypes::TableHeader* GetTableHeaderPtr(Int ordinalPosition) const;

            void SetDatabaseHeader(const CoreEngine::DatabaseHeader& header) const;
            void SetTableHeader(const CoreEngine::StorageTypes::TableHeader& header, SmallInt ordinalPosition) const;
    };
}
