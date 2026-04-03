#pragma once
#include "PageView.h"
#include "../../include/DataStorage/Table.h"

namespace CoreEngine{
    struct DatabaseHeader;
}

namespace Pages{
    struct Frame;

    class HeaderPageView final : public PageView{
        CoreEngine::DatabaseHeader* databaseHeaderPtr;
        std::vector<CoreEngine::StorageTypes::TableHeader> tablesHeaders;

        void ReadTableHeadersFromDisk();

        public:
            explicit HeaderPageView(Frame* framePtr);

            HeaderPageView(HeaderPageView&& other) noexcept;
            HeaderPageView& operator=(HeaderPageView&& other) noexcept;

            [[nodiscard]] CoreEngine::DatabaseHeader* GetDatabaseHeaderPtr() const;

            [[nodiscard]] const std::vector<CoreEngine::StorageTypes::TableHeader>& GetTableHeaders()const;
            [[nodiscard]] const CoreEngine::StorageTypes::TableHeader& GetTableHeader(Int indexPosition) const;

            void SetDatabaseHeader(const CoreEngine::DatabaseHeader& header) const;
            void SetTableHeader(const CoreEngine::StorageTypes::TableHeader& header);

            void WriteTableHeadersToDisk() const;
    };
}
