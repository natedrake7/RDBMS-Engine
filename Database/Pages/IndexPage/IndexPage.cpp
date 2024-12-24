#include "IndexPage.h"
#include "../../../AdditionalLibraries/B+Tree/BPlusTree.h"
#include "../../Table/Table.h"
#include "../../Row/Row.h"
#include <cstring>

using namespace Indexing;
using namespace DatabaseEngine::StorageTypes;

namespace Pages
{
    IndexPageAdditionalHeader::IndexPageAdditionalHeader()
    {
        this->nextPageId = 0;
    }

    IndexPageAdditionalHeader::IndexPageAdditionalHeader(const page_id_t& nextPageId)
    {
        this->nextPageId = nextPageId;
    }

    IndexPageAdditionalHeader::~IndexPageAdditionalHeader() = default;

    IndexPage::IndexPage(const page_id_t &pageId, const bool &isPageCreation) : Page(pageId, isPageCreation)
    {
        this->treeData = new object_t[PAGE_SIZE - this->header.GetPageHeaderSize() - sizeof(this->additionalHeader)];
        this->header.pageType = PageType::INDEX;
    }

    IndexPage::IndexPage(const PageHeader &pageHeader) : Page(pageHeader)
    {
        this->treeData = new object_t[PAGE_SIZE - this->header.GetPageHeaderSize() - sizeof(this->additionalHeader)];
    }

    IndexPage::~IndexPage()
    {
       delete this->treeData;
    }

    void IndexPage::GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t &offSet, fstream *filePtr)
    {
        this->GetAdditionalHeaderFromFile(data, offSet);

        page_size_t treeDataSize = this->CalculateTreeDataSize();

        if(treeDataSize == 0)
            return;

        this->treeData = new object_t[PAGE_SIZE - this->header.GetPageHeaderSize() - sizeof(this->additionalHeader)];

        memcpy(treeData, data.data() + offSet, treeDataSize);
    }

    void IndexPage::WritePageToFile(fstream *filePtr)
    {
        this->WritePageHeaderToFile(filePtr);
        this->WriteAdditionalHeaderToFile(filePtr);

        filePtr->write(reinterpret_cast<const char*>(this->treeData), this->CalculateTreeDataSize());
    }

    page_size_t IndexPage::CalculateTreeDataSize() const
    {
        return PAGE_SIZE - this->header.bytesLeft - this->header.GetPageHeaderSize() - sizeof(this->additionalHeader);
    }

    void IndexPage::WriteTreeDataToPage(Node* node)
    {
        memcpy(this->treeData - this->header.bytesLeft, &node->isLeaf, sizeof(bool));
        this->header.bytesLeft -= sizeof(bool);

        if (node->isLeaf)
        {
            memcpy(this->treeData - this->header.bytesLeft, &node->data.pageId, sizeof(page_id_t));
            this->header.bytesLeft -= sizeof(page_id_t);

            memcpy(this->treeData - this->header.bytesLeft, &node->data.extentId, sizeof(extent_id_t));
            this->header.bytesLeft -= sizeof(extent_id_t);
        }

        const uint16_t numberOfKeys = node->keys.size();

        memcpy(this->treeData - this->header.bytesLeft, &numberOfKeys, sizeof(uint16_t));
        this->header.bytesLeft -= sizeof(uint16_t);

        for (const auto &key : node->keys)
        {
            memcpy(this->treeData - this->header.bytesLeft, &key.size, sizeof(key_size_t));
            this->header.bytesLeft -= sizeof(key_size_t);

            memcpy(this->treeData - this->header.bytesLeft, key.value, key.size);
            this->header.bytesLeft -= key.size;
        }

        const uint16_t numberOfChildren = node->children.size();

        memcpy(this->treeData - this->header.bytesLeft, &numberOfChildren, sizeof(uint16_t));
        this->header.bytesLeft -= sizeof(uint16_t);

        this->header.pageSize++;
        this->isDirty = true;
    }

    void IndexPage::GetAdditionalHeaderFromFile(const vector<char>& data, page_offset_t& offSet)
    {
        memcpy(&this->additionalHeader, data.data() + offSet, sizeof(IndexPageAdditionalHeader));
        offSet += sizeof(IndexPageAdditionalHeader);
    }

    void IndexPage::WriteAdditionalHeaderToFile(fstream* filePtr)
    {
        filePtr->write(reinterpret_cast<const char*>(&this->additionalHeader), sizeof(IndexPageAdditionalHeader));
    }

    void IndexPage::SetNextPageId(const page_id_t& nextPageId) { this->additionalHeader.nextPageId = nextPageId; }

    const page_id_t& IndexPage::GetNextPageId() const { return this->additionalHeader.nextPageId; }

    const object_t*  IndexPage::GetTreeData() const { return this->treeData; }
}
