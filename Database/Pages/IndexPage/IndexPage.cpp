#include "IndexPage.h"
#include "../../../AdditionalLibraries/B+Tree/BPlusTree.h"
#include "../../Table/Table.h"
#include "../../Row/Row.h"
#include "../../Block/Block.h"

using namespace Indexing;
using namespace DatabaseEngine::StorageTypes;

namespace Pages
{
    IndexPage::IndexPage(const page_id_t &pageId, const bool &isPageCreation) : Page(pageId, isPageCreation)
    {
        this->tree = nullptr;
        this->header.pageType = PageType::INDEX;
    }

    IndexPage::IndexPage(const PageHeader &pageHeader) : Page(pageHeader)
    {
        this->tree = nullptr;
    }

    IndexPage::~IndexPage()
    {
        delete this->tree;
    }

    void IndexPage::GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t &offSet, fstream *filePtr)
    {
    }

    void IndexPage::WritePageToFile(fstream *filePtr)
    {
    }

    Node *IndexPage::FindAppropriateNodeForInsert(const DatabaseEngine::StorageTypes::Table *table, const Key &key, int *indexPosition)
    {
        if (!this->tree)
        {
            table->GetIndexedColumnKeys(&this->indexedColumns);
            this->tree = new BPlusTree(table);
        }

        return this->tree->FindAppropriateNodeForInsert(key, indexPosition);
    }

    void IndexPage::RangeQuery(const Key &minKey, const Key &maxKey, vector<QueryData> &result)
    {
        if (!this->tree)
            return;

        this->tree->RangeQuery(minKey, maxKey, result);
    }

    const vector<column_index_t> &IndexPage::GetIndexedColumns() const { return this->indexedColumns; }
}
