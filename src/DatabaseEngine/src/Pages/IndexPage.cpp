#include "../../include/Pages/IndexPage.h"
#include "../../include/BTree.h"
#include "../../include/DataStorage/Table.h"
#include <cstring>
#include <ostream>
#include <stdexcept>


namespace Pages {

void IndexPage::WriteAdditionalHeaderToFile(fstream * filePtr) const
{
    filePtr->write(reinterpret_cast<const char*>(&this->additionalHeader.treeType), sizeof(TreeType));
    filePtr->write(reinterpret_cast<const char*>(&this->additionalHeader.treeId), sizeof(page_id_t));
    filePtr->write(reinterpret_cast<const char*>(&this->additionalHeader.numberOfSubKeys), sizeof(uint8_t));
    filePtr->write(reinterpret_cast<const char*>(&this->additionalHeader.isLeaf), sizeof(bool));
    filePtr->write(reinterpret_cast<const char*>(&this->additionalHeader.isRoot), sizeof(bool));
    filePtr->write(reinterpret_cast<const char*>(&this->additionalHeader.isEmpty), sizeof(bool));
}

void IndexPage::ReadAdditionalHeaderFromFile(const vector<char>& data, page_offset_t & offSet)
{
    memcpy(&this->additionalHeader.treeType, data.data() + offSet, sizeof(TreeType));
    offSet += sizeof(TreeType);
    memcpy(&this->additionalHeader.treeId, data.data() + offSet, sizeof(page_id_t));
    offSet += sizeof(page_id_t);
    memcpy(&this->additionalHeader.numberOfSubKeys, data.data() + offSet, sizeof(uint8_t));
    offSet += sizeof(uint8_t);
    memcpy(&this->additionalHeader.isLeaf, data.data() + offSet, sizeof(bool));
    offSet += sizeof(bool);
    memcpy(&this->additionalHeader.isRoot, data.data() + offSet, sizeof(bool));
    offSet += sizeof(bool);
    memcpy(&this->additionalHeader.isEmpty, data.data() + offSet, sizeof(bool));
    offSet += sizeof(bool);
}

IndexPage::IndexPage(const page_id_t &pageId, const bool &isPageCreation) : Page(pageId, isPageCreation) 
{
    this->header.pageType = PageType::INDEX;
    this->nextNode = INVALID_PAGE_ID;
    this->previousNode = INVALID_PAGE_ID;
    this->header.bytesLeft  = Constants::INDEX_PAGE_DEFAULT_SIZE;
}

IndexPage::IndexPage(const PageHeader &pageHeader) : Page(pageHeader) {
    this->nextNode = INVALID_PAGE_ID;
    this->previousNode = INVALID_PAGE_ID;
}

IndexPage::~IndexPage() {
    for (const auto& key : this->keys)
        delete key;
}

void IndexPage::ReadFromDisk(
    const std::vector<char> &data,
    const DatabaseEngine::StorageTypes::Table *table,
    page_offset_t &offSet,
    fstream *filePtr
){
    this->ReadAdditionalHeaderFromFile(data, offSet);
    const auto indexedColumnTypes = table->GetColumnTypeByTreeId(this->additionalHeader.treeId);

    uint16_t numOfKeys = 0;
    memcpy(&numOfKeys, data.data() + offSet, sizeof(uint16_t));
    offSet += sizeof(uint16_t);

    for (int i = 0;i < numOfKeys; i++) {
        auto* key = new DataTypes::Indexing::Key();

        for (int j = 0; j < this->additionalHeader.numberOfSubKeys; j++){
            key_size_t keySize;
            memcpy(&keySize, data.data() + offSet, sizeof(key_size_t));
            offSet += sizeof(key_size_t);

            vector<object_t> keyValue(keySize);
            memcpy(keyValue.data(), data.data() + offSet, keySize);
            offSet += keySize;

            const auto dataType = j < indexedColumnTypes.size()
                    ? indexedColumnTypes[j]
                    : DataType::RowIdentifier;

            key->InsertKey(DataTypes::Indexing::Key(keyValue.data(), keySize, dataType));
        }

        this->keys.push_back(key);
    }

    if (!this->additionalHeader.isLeaf) {
        uint16_t numberOfChildren;
        memcpy(&numberOfChildren, data.data() + offSet, sizeof(uint16_t));
        offSet += sizeof(uint16_t);

        if(numberOfChildren == 0)
          return;

        this->children.resize(numberOfChildren);

        memcpy(this->children.data(), data.data() + offSet, numberOfChildren * sizeof(page_id_t));
        return;
    }

    //is leaf
    memcpy(&this->previousNode, data.data() + offSet, sizeof(page_id_t));
    offSet += sizeof(page_id_t);

    memcpy(&this->nextNode, data.data() + offSet, sizeof(page_id_t));
    offSet += sizeof(page_id_t);

    const auto& columns = table->GetColumns();

    if (this->additionalHeader.treeType == TreeType::Clustered) {
        this->rows.reserve(this->header.pageSize);

        for (int i = 0;i < this->header.pageSize; i++) {
            auto* row = Page::ReadRowFromDisk(data, table, offSet, this->header.pageId, i);

            this->rows.push_back(row);
        }

        return;
    }

    this->nonClusteredData.reserve(this->header.pageSize);

    for (int i = 0;i < this->header.pageSize; i++) {
        auto item = Headers::RowIdentifier();

        memcpy(&item, data.data() + offSet, sizeof(Headers::RowIdentifier));
        offSet += sizeof(Headers::RowIdentifier);

        this->nonClusteredData.push_back(std::move(item));
    }
}

void IndexPage::WriteToDisk(fstream *filePtr){
    if (!this->keys.empty())
        this->additionalHeader.numberOfSubKeys = this->keys.front()->subKeys.size();
    
    this->WritePageHeaderToDisk(filePtr);
    this->WriteAdditionalHeaderToFile(filePtr);

    const uint16_t numOfKeys = this->keys.size();
    filePtr->write(reinterpret_cast<const char*>(&numOfKeys), sizeof(uint16_t));

    for (const auto& key : this->keys) {
        for (const auto& subKey: key->subKeys)
        {
            const auto size = subKey.value.GetSize();

            filePtr->write(reinterpret_cast<const char*>(&size), sizeof(key_size_t));
            filePtr->write(reinterpret_cast<const char*>(subKey.value.GetRawData()), size);
        }
    }

    if (!this->additionalHeader.isLeaf) {
        const uint16_t childrenSize = this->children.size();

        filePtr->write(reinterpret_cast<const char*>(&childrenSize), sizeof(uint16_t));
        filePtr->write(reinterpret_cast<const char*>(this->children.data()), childrenSize * sizeof(page_id_t));
        return;
    }

    //leaf nodes
    filePtr->write(reinterpret_cast<const char*>(&this->previousNode), sizeof(page_id_t));
    filePtr->write(reinterpret_cast<const char*>(&this->nextNode), sizeof(page_id_t));

    if (this->additionalHeader.treeType == TreeType::Clustered) {
        for (const auto& row : this->rows)
            Page::WriteRowToDisk(filePtr, row);

        return;
    }

    for (const auto& data : this->nonClusteredData)
        filePtr->write(reinterpret_cast<const char*>(&data), sizeof(Headers::RowIdentifier));
}

void IndexPage::SetTreeType(const TreeType & treeType) { this->additionalHeader.treeType = treeType; }

void IndexPage::SetTreeId(const page_id_t & treeId) { this->additionalHeader.treeId = treeId; }

const page_id_t & IndexPage::GetTreeId() const { return this->additionalHeader.treeId; }

void IndexPage::UpdateBytesLeft()
{
    this->header.bytesLeft = Constants::INDEX_PAGE_DEFAULT_SIZE;

    for (const auto& key : this->keys)
        this->header.bytesLeft -= key->size;

    if (!this->additionalHeader.isLeaf) {
        this->header.bytesLeft -= this->children.size() * sizeof(page_id_t);

        this->isDirty = true;
        return;
    }

    if (this->additionalHeader.treeType == TreeType::Clustered) {
        for (const auto& row: this->rows)
            this->header.bytesLeft -= row->TotalSize();
    }
    else
        this->header.bytesLeft -= this->nonClusteredData.size() * (sizeof(page_id_t) + sizeof(page_offset_t));

    this->header.bytesLeft -= 2 * sizeof(page_id_t);

    this->isDirty = true;
}

vector<DataTypes::Indexing::Key*>* IndexPage::GetKeysUnsafe(){ return &this->keys; }

vector<Headers::RowIdentifier> * IndexPage::NonClusteredDataNoLock(){ return &this->nonClusteredData; }

vector<page_id_t> * IndexPage::GetChildren(){ return &this->children; }

void IndexPage::ResizeNodes(const int& splitFactor)
{
    //next
    if(this->header.pageSize <= splitFactor)
        throw invalid_argument("IndexPage::split factor cannot be equal or less to pageSize");

    // auto it = this->nodes.begin();
    // int counter = 0;
    // while (it != this->nodes.end()) {
    //     if (counter >= splitFactor) {
    //         it = this->nodes.erase(it);  // erase returns iterator to next element
    //         continue;
    //     }
    //
    //     ++it;
    //     ++counter;
    // }
    //
    //
    // this->header.pageSize = this->nodes.size();

    this->isDirty = true;
}

bool IndexPage::isEmpty() const{
    return this->additionalHeader.isEmpty;
}

const bool & IndexPage::IsLeaf() const{ return this->additionalHeader.isLeaf; }

const bool & IndexPage::IsRoot() const{ return this->additionalHeader.isRoot; }

void IndexPage::SetIsLeaf(const bool& isLeaf) {
    this->additionalHeader.isLeaf = isLeaf;
    this->additionalHeader.isEmpty = false;
    this->isDirty = true;
}

void IndexPage::SetIsRoot(const bool &isRoot) {
    this->additionalHeader.isRoot = isRoot;
    this->additionalHeader.isEmpty = false;
    this->isDirty = true;
}

void IndexPage::InsertChild(const page_id_t &child){
    this->children.push_back(child);
}

void IndexPage::SetPreviousPage(const page_id_t &previousPage){ this->previousNode = previousPage; }

void IndexPage::SetNextPage(const page_id_t &nextPage){ this->nextNode = nextPage; }

const page_id_t & IndexPage::GetPreviousPage()const{ return this->previousNode; }

const page_id_t & IndexPage::GetNextPage()const{ return this->nextNode; }

bool IndexPage::HasRightSibling() const{ return this->nextNode != INVALID_PAGE_ID; }

bool IndexPage::HasLeftSibling() const{ return this->previousNode != INVALID_PAGE_ID; }

void IndexPage::UpdatePageSize()
{
    this->header.pageSize = (this->additionalHeader.treeType == TreeType::Clustered)
            ? this->rows.size()
            : this->nonClusteredData.size();

    this->additionalHeader.isEmpty = false;
    this->isDirty = true;
}

void IndexPage::MarkEmpty(){
  this->header.bytesLeft = Constants::INDEX_PAGE_DEFAULT_SIZE;

  this->header.pageSize = 0;

  for(const auto& key : this->keys)
    delete key;

  this->keys.clear();

  this->nonClusteredData.clear();

  for(const auto& row : this->rows)
    delete row;

  this->rows.clear();

  this->children.clear();

  this->additionalHeader.isEmpty = true;

  this->isDirty = true;
}

IndexPageAdditionalHeader::IndexPageAdditionalHeader(){
    this->treeId = 0;
    this->treeType = TreeType::NonClustered;
    this->numberOfSubKeys = 0;
    this->isLeaf = false;
    this->isRoot = false;
    this->isEmpty = true;
}

IndexPageAdditionalHeader::~IndexPageAdditionalHeader() = default;
} // namespace Pages
