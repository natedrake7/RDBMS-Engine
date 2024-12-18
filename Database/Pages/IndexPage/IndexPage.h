#pragma once
#include "../Page.h"

namespace Indexing {
    class BPlusTree;
}

namespace Pages {
    class IndexPage final : public Page{
        Indexing::BPlusTree* tree;

    public:
        IndexPage(const page_id_t& pageId, const bool& isPageCreation);
        explicit IndexPage(const PageHeader& pageHeader);
        ~IndexPage() override;

        void GetPageDataFromFile(const vector<char>& data, const Table* table, page_offset_t& offSet, fstream* filePtr) override;
        void WritePageToFile(fstream* filePtr) override;
        void InsertRow(const Table* table, const int& key, const page_id_t& pageId, const page_offset_t& pageOffset);
    }; 
}




