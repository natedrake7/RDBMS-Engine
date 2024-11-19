#pragma once
#include <vector>
#include "../Table/Table.h"
#include "../Row/Row.h"
#include "../Database.h"

class Row;
class Table;
struct TableFullMetaData;
struct DatabaseMetaData;

using namespace std;

constexpr size_t PAGE_SIZE = 8 * 1024;

typedef struct PageMetadata{
    uint16_t pageId;
    uint16_t pageSize;
    uint16_t nextPageId;
    uint16_t bytesLeft;

    PageMetadata();
    ~PageMetadata();
}PageMetaData;

class Page {
    vector<Row*> rows;

    protected:
        bool isDirty;
        string filename;
        PageMetaData metadata;

    public:
        explicit Page(const int& pageId);
        virtual ~Page();
        void InsertRow(Row* row, const Table& table);
        void DeleteRow(Row* row);
        void UpdateRow(Row* row);
        virtual void GetPageDataFromFile(const vector<char>& data, const Table* table);
        virtual void WritePageToFile(fstream* filePtr);
        void SetNextPageId(const int& nextPageId);
        void SetFileName(const string& filename);
        const string& GetFileName() const;
        const uint16_t& GetPageId() const;
        const bool& GetPageDirtyStatus() const;
        const uint16_t& GetBytesLeft() const;
        const uint16_t& GetNextPageId() const;
        vector<Row> GetRows(const Table& table) const;
};

class MetaDataPage final : public Page {
    DatabaseMetaData databaseMetaData;
    vector<TableFullMetaData> tablesMetaData;

    public:
        explicit MetaDataPage(const int &pageId);
        ~MetaDataPage() override;
        void WritePageToFile(fstream* filePtr) override;
        void GetPageDataFromFile(const vector<char>& data, const Table* table) override;
        void SetMetaData(const DatabaseMetaData& databaseMetaData, const vector<Table*>& tables);
        const DatabaseMetaData& GetDatabaseMetaData() const;
        const vector<TableFullMetaData>& GetTableFullMetaData() const;
};



