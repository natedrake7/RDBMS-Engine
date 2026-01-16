#pragma once
#include "Page.h"

#include <iosfwd>
#include <vector>

namespace DatabaseEngine {
    struct DatabaseHeader;

    namespace StorageTypes {
        class Table;
        struct TableHeader;
    }
}

namespace Pages {
    class HeaderPage final : public Page{
        DatabaseEngine::DatabaseHeader* databaseHeader;
        std::vector<DatabaseEngine::StorageTypes::TableHeader> tablesHeaders;

    public:
        explicit HeaderPage(const int &pageId);
        explicit HeaderPage();
        explicit HeaderPage(const PageHeader &pageHeader);
        ~HeaderPage() override;
        void WriteToDisk(std::fstream* filePtr) override;
        void ReadFromDisk(const std::vector<char>& data, const DatabaseEngine::StorageTypes::Table* table, page_offset_t& offSet, std::fstream* filePtr) override;
        void SetDbHeader(const DatabaseEngine::DatabaseHeader& header);
        void SetTableHeader(const DatabaseEngine::StorageTypes::Table* table);
        [[nodiscard]] const DatabaseEngine::DatabaseHeader* GetDatabaseHeader() const;
        [[nodiscard]] const std::vector<DatabaseEngine::StorageTypes::TableHeader>& GetTablesFullHeaders() const;
    };
}
