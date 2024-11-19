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

constexpr size_t PAGE_SIZE = 4096;

typedef struct PageMetadata{
    int pageId;
    int pageSize;
    int nextPageId;
    size_t bytesLeft;

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
        void InsertRow(Row* row);
        void DeleteRow(Row* row);
        void UpdateRow(Row* row);
        virtual void GetPageDataFromFile(const vector<char>& data);
        virtual void WritePageToFile(fstream* filePtr);
        void SetNextPageId(const int& nextPageId);
        void SetFileName(const string& filename);
        const string& GetFileName() const;
        const int& GetPageId() const;
        const bool& GetPageDirtyStatus() const;
        const size_t& GetBytesLeft() const;
        const int& GetNextPageId() const;
        vector<Row> GetRows() const;
};

class MetaDataPage final : public Page {
    DatabaseMetaData databaseMetaData;
    vector<TableFullMetaData> tablesMetaData;

    public:
        explicit MetaDataPage(const int &pageId);
        ~MetaDataPage() override;
        void WritePageToFile(fstream* filePtr) override;
        void GetPageDataFromFile(const vector<char>& data) override;
        void SetMetaData(const DatabaseMetaData& databaseMetaData, const vector<Table*>& tables);
        const DatabaseMetaData& GetDatabaseMetaData() const;
        const vector<TableFullMetaData>& GetTableFullMetaData() const;
};



