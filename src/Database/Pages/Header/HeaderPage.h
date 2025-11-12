#pragma once
#include "../Page.h"

namespace DatabaseEngine {
    struct DatabaseHeader;
    namespace StorageTypes {
        class Table;
        struct TableHeader;
    }
}

namespace Pages {
    class HeaderPage final : public Page
    {
        DatabaseEngine::DatabaseHeader* databaseHeader;
        vector<DatabaseEngine::StorageTypes::TableHeader> tablesHeaders;

    public:
        explicit HeaderPage(const int &pageId);
        explicit HeaderPage();
        explicit HeaderPage(const PageHeader &pageHeader);
        ~HeaderPage() override;
        void WritePageToFile(fstream* filePtr) override;
        void ReadFromDisk(const vector<char>& data, const DatabaseEngine::StorageTypes::Table* table, page_offset_t& offSet, fstream* filePtr) override;
        void SetDbHeader(const DatabaseEngine::DatabaseHeader& databaseHeader);
        void SetTableHeader(const DatabaseEngine::StorageTypes::Table* table);
        [[nodiscard]] const DatabaseEngine::DatabaseHeader* GetDatabaseHeader() const;
        [[nodiscard]] const vector<DatabaseEngine::StorageTypes::TableHeader>& GetTablesFullHeaders() const;
    };
}
