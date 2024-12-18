#pragma once
#include "../Page.h"

namespace DatabaseEngine {
    struct DatabaseHeader;
    namespace StorageTypes {
        class Table;
        struct TableFullHeader;
    }
}

namespace Pages {
    class HeaderPage final : public Page
    {
        DatabaseEngine::DatabaseHeader* databaseHeader;
        vector<DatabaseEngine::StorageTypes::TableFullHeader> tablesHeaders;

    public:
        explicit HeaderPage(const int &pageId);
        explicit HeaderPage();
        explicit HeaderPage(const PageHeader &pageHeader);
        ~HeaderPage() override;
        void WritePageToFile(fstream* filePtr) override;
        void GetPageDataFromFile(const vector<char>& data, const DatabaseEngine::StorageTypes::Table* table, page_offset_t& offSet, fstream* filePtr) override;
        void SetHeaders(const DatabaseEngine::DatabaseHeader& databaseHeader, const vector<DatabaseEngine::StorageTypes::Table*>& tables);
        const DatabaseEngine::DatabaseHeader* GetDatabaseHeader() const;
        const vector<DatabaseEngine::StorageTypes::TableFullHeader>& GetTablesFullHeaders() const;
    };
}
