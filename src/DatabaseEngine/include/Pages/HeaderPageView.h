#pragma once
#include "PageView.h"
#include "../../include/DataStorage/Table.h"

namespace DatabaseEngine{
    struct DatabaseHeader;
}

namespace Pages{
    struct Frame;

    class HeaderPageView final : public PageView{
        DatabaseEngine::DatabaseHeader* databaseHeaderPtr;
        std::vector<DatabaseEngine::StorageTypes::TableHeader> tablesHeaders;

        void ReadTableHeadersFromDisk();

        public:
            explicit HeaderPageView(Frame* framePtr);

            HeaderPageView(HeaderPageView&& other) noexcept;
            HeaderPageView& operator=(HeaderPageView&& other) noexcept;

            [[nodiscard]] DatabaseEngine::DatabaseHeader* GetDatabaseHeaderPtr() const;

            const std::vector<DatabaseEngine::StorageTypes::TableHeader>& GetTableHeaders() const;
            [[nodiscard]] const DatabaseEngine::StorageTypes::TableHeader& GetTableHeader(Int indexPosition) const;

            void SetDatabaseHeader(const DatabaseEngine::DatabaseHeader& header) const;
            void SetTableHeader(const DatabaseEngine::StorageTypes::TableHeader& header);

            void WriteTableHeadersToDisk() const;
    };
}
