#pragma once
#include "PageView.h"

namespace DatabaseEngine::StorageTypes
{
    struct TableHeader;
}

namespace DatabaseEngine
{
    struct DatabaseHeader;
}

namespace Pages
{
    struct Frame;

    class HeaderPageView final : public PageView{
        DatabaseEngine::DatabaseHeader* databaseHeaderPtr;

        std::vector<DatabaseEngine::StorageTypes::TableHeader> tablesHeaders;

        void ReadTableHeadersFromDisk();

        public:
            explicit HeaderPageView(Frame* framePtr);
            ~HeaderPageView() override;

            DatabaseEngine::DatabaseHeader* GetDatabaseHeaderPtr() const;

            const DatabaseEngine::StorageTypes::TableHeader& GetTableHeader(Int indexPosition) const;

            void SetDatabaseHeader(const DatabaseEngine::DatabaseHeader& header) const;
            void SetTableHeader(Int indexPosition, const DatabaseEngine::StorageTypes::TableHeader& header);

            void WriteTableHeadersToDisk() const;

    };
}
