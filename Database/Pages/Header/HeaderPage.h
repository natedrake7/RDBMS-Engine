#pragma once
#include "../Page.h"

class Table;
struct TableFullHeader;
struct DatabaseHeader;

class HeaderPage final : public Page
{
    DatabaseHeader databaseHeader;
    vector<TableFullHeader> tablesHeaders;

public:
    explicit HeaderPage(const int &pageId);
    explicit HeaderPage();
    explicit HeaderPage(const PageHeader &pageHeader);
    ~HeaderPage() override;
    void WritePageToFile(fstream* filePtr) override;
    void GetPageDataFromFile(const vector<char>& data, const Table* table, page_offset_t& offSet, fstream* filePtr) override;
    void SetHeaders(const DatabaseHeader& databaseHeader, const vector<Table*>& tables);
    const DatabaseHeader& GetDatabaseHeader() const;
    const vector<TableFullHeader>& GetTablesFullHeaders() const;
};