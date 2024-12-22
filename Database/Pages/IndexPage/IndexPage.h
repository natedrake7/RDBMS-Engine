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
    class IndexPage final : public Page
    {
        Indexing::BPlusTree *tree;
        vector<column_index_t> indexedColumns;

    protected:
        void WriteNodeToDisk(Indexing::Node *node, fstream *filePtr);
        Indexing::Node *GetNodeFromDisk(const vector<char> &data, page_offset_t &offSet);

    public:
        IndexPage(const page_id_t &pageId, const bool &isPageCreation);
        explicit IndexPage(const PageHeader &pageHeader);
        ~IndexPage() override;

        void GetPageDataFromFile(const vector<char> &data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t &offSet, fstream *filePtr) override;
        void WritePageToFile(fstream *filePtr) override;

        Indexing::Node *FindAppropriateNodeForInsert(const DatabaseEngine::StorageTypes::Table *table, const Indexing::Key &key, int *indexPosition, vector<pair<Indexing::Node *, Indexing::Node *>> *splitLeaves);

        void RangeQuery(const Indexing::Key &minKey, const Indexing::Key &maxKey, vector<Indexing::QueryData> &result);

        [[nodiscard]] const vector<column_index_t> &GetIndexedColumns() const;

        [[nodiscard]] const int &GetBranchingFactor() const;

        void SetRoot(Indexing::Node *&node);
    };
}
