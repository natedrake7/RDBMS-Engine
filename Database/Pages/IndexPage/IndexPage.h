#pragma once
#include "../Page.h"

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
    class IndexPage final : public Page
    {
        Indexing::BPlusTree *tree;
        vector<column_index_t> indexedColumns;

    public:
        IndexPage(const page_id_t &pageId, const bool &isPageCreation);
        explicit IndexPage(const PageHeader &pageHeader);
        ~IndexPage() override;

        void GetPageDataFromFile(const vector<char> &data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t &offSet, fstream *filePtr) override;
        void WritePageToFile(fstream *filePtr) override;

        Indexing::Node *FindAppropriateNodeForInsert(const DatabaseEngine::StorageTypes::Table *table, const Indexing::Key &key, int *indexPosition);

        void RangeQuery(const Indexing::Key &minKey, const Indexing::Key &maxKey, vector<Indexing::QueryData> &result);

        [[nodiscard]] const vector<column_index_t> &GetIndexedColumns() const;
    };
}
