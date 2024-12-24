#pragma once
#include "../Page.h"
#include <fstream>

using namespace std;

namespace Indexing
{
    class BPlusTree;
    struct Node;
    struct Key;
    struct QueryData;
}

namespace DatabaseEngine::StorageTypes
{
    class Table;
}

namespace Storage
{
    class PageManager;
}

namespace Pages
{
    typedef struct IndexPageAdditionalHeader{
        page_id_t nextPageId;

        IndexPageAdditionalHeader();
        IndexPageAdditionalHeader(const page_id_t& nextPageId);
        ~IndexPageAdditionalHeader();
    }IndexPageAdditionalHeader;

    class IndexPage final : public Page
    {
        IndexPageAdditionalHeader additionalHeader;
        object_t* treeData;
        vector<column_index_t> indexedColumns;

    protected:
        void GetAdditionalHeaderFromFile(const vector<char> &data, page_offset_t &offSet);
        void WriteAdditionalHeaderToFile(fstream* filePtr);
        page_size_t CalculateTreeDataSize() const;

    public:
        IndexPage(const page_id_t &pageId, const bool &isPageCreation);
        explicit IndexPage(const PageHeader &pageHeader);
        ~IndexPage() override;

        void GetPageDataFromFile(const vector<char> &data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t &offSet, fstream *filePtr) override;
        void WritePageToFile(fstream *filePtr) override;

        void WriteTreeDataToPage(Indexing::Node* node);

        void SetNextPageId(const page_id_t& nextPageId);

        const page_id_t& GetNextPageId() const;

        const object_t* GetTreeData() const;
    };
}
