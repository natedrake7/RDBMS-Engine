#pragma once
#include "../Page.h"

class Table;
struct TableFullHeader;

namespace DatabaseEngine {
    struct DatabaseHeader;
}

namespace Pages {
    class HeaderPage final : public Page
    {
        DatabaseEngine::DatabaseHeader* databaseHeader;
        vector<TableFullHeader> tablesHeaders;

    public:
        explicit HeaderPage(const int &pageId);
        explicit HeaderPage();
        explicit HeaderPage(const PageHeader &pageHeader);
        ~HeaderPage() override;
        void WritePageToFile(fstream* filePtr) override;
        void GetPageDataFromFile(const vector<char>& data, const Table* table, page_offset_t& offSet, fstream* filePtr) override;
        void SetHeaders(const DatabaseEngine::DatabaseHeader& databaseHeader, const vector<Table*>& tables);
        const DatabaseEngine::DatabaseHeader* GetDatabaseHeader() const;
        const vector<TableFullHeader>& GetTablesFullHeaders() const;
    };
}
