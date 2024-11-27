#pragma once
#include "../Page.h"

class Table;
struct TableFullMetaData;
struct DatabaseMetaData;

class MetaDataPage : public Page
{
    DatabaseMetaData databaseMetaData;
    vector<TableFullMetaData> tablesMetaData;

public:
    explicit MetaDataPage(const int &pageId);
    explicit MetaDataPage();
    explicit MetaDataPage(const PageMetaData &pageMetaData);
    ~MetaDataPage() override;
    void WritePageToFile(fstream* filePtr) override;
    void GetPageDataFromFile(const vector<char>& data, const Table* table, page_offset_t& offSet, fstream* filePtr) override;
    void SetMetaData(const DatabaseMetaData& databaseMetaData, const vector<Table*>& tables);
    const DatabaseMetaData& GetDatabaseMetaData() const;
    const vector<TableFullMetaData>& GetTableFullMetaData() const;
};