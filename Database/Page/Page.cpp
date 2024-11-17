#include "Page.h"

PageMetadata::PageMetadata()
{
    this->pageId = -1;
    this->pageSize = 0;
    this->bytesLeft = PAGE_SIZE;
    this->nextPageId = -1;
}

PageMetadata::~PageMetadata() = default;

Page::Page(const int& pageId)
{
    this->metadata.pageId = pageId;
    this->isDirty = false;
}

Page::~Page()
{
    for(const auto& row: this->rows)
        delete row;
}

void Page::InsertRow(Row* row)
{
    this->rows.push_back(row);
    this->isDirty = true;
}

void Page::DeleteRow(Row* row)
{
    this->isDirty = true;
}

void Page::UpdateRow(Row* row)
{
    this->isDirty = true;
}

void Page::SetPageDataFromFile(const vector<char>& data)
{
    memcpy(&this->metadata, data.data(), sizeof(PageMetadata));

    size_t dataOffset = sizeof(PageMetadata);
    int bytesToRead;
    
    for (int i = 0; i < this->metadata.pageSize; i++)
    {
        for(int j = 0; j < 6; j++)
        {
            memcpy(&bytesToRead, data.data() + dataOffset, sizeof(int));

            // Row* row = new Row();
            unsigned char *bytes = new unsigned char[bytesToRead];

            //add bytes to block and to row

            dataOffset += sizeof(int);
        
            memcpy(bytes, data.data() + dataOffset, bytesToRead);

            dataOffset += bytesToRead;
        }
    }

}

const int& Page::GetPageId() const { return this->metadata.pageId; }

const bool& Page::GetPageDirtyStatus() const { return this->isDirty; }
