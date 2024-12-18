#include "IndexPage.h"
#include "../../../AdditionalLibraries/B+Tree/BPlusTree.h"

using namespace Indexing;
using namespace DatabaseEngine::StorageTypes;

namespace Pages {
    IndexPage::IndexPage(const page_id_t& pageId, const bool& isPageCreation) : Page(pageId, isPageCreation)
    {
        this->tree = nullptr;
        this->header.pageType = PageType::INDEX;
    }

    IndexPage::IndexPage(const PageHeader& pageHeader) : Page(pageHeader)
    {
        this->tree = nullptr;
    }

    IndexPage::~IndexPage()
    {
        delete this->tree;
    }

    void IndexPage::GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t &offSet,fstream *filePtr)
    {
    
    }

    void IndexPage::WritePageToFile(fstream *filePtr)
    {
    
    }

    void IndexPage::InsertRow(const Table* table, const int& key, const page_id_t& pageId, const page_size_t& pageOffset)
    {
        if (!this->tree)
            this->tree = new BPlusTree(table);

        this->tree->Insert(key, BPlusTreeData(pageId, pageOffset));
    }
}

