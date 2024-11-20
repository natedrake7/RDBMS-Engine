#include "Page.h"

PageMetadata::PageMetadata()
{
    this->pageId = 0;
    this->pageSize = 0;
    this->nextPageId = 0;
    this->bytesLeft = PAGE_SIZE - 4 * sizeof(uint16_t);
}

PageMetadata::~PageMetadata() = default;

Page::Page(const int& pageId)
{
    this->metadata.pageId = pageId;
    this->isDirty = false;
}

Page::Page()
{
    this->isDirty = false;
}

Page::~Page()
{
    for(const auto& row: this->rows)
        delete row;
}

void Page::InsertRow(Row* row, const Table& table)
{
    this->rows.push_back(row);
    this->metadata.bytesLeft -= (row->GetRowSize() + table.GetNumberOfColumns() * sizeof(uint32_t));
    this->metadata.pageSize++;
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

void Page::GetPageMetaDataFromFile(const vector<char> &data, const Table *table, uint32_t &offSet)
{
    memcpy(&this->metadata.pageId, data.data() + offSet, sizeof(uint16_t));
    offSet += sizeof(uint16_t);

    memcpy(&this->metadata.nextPageId, data.data() + offSet, sizeof(uint16_t));
    offSet += sizeof(uint16_t);

    memcpy(&this->metadata.pageSize, data.data() + offSet, sizeof(uint16_t));
    offSet += sizeof(uint16_t);

    memcpy(&this->metadata.bytesLeft, data.data() + offSet, sizeof(uint16_t));
    offSet += sizeof(uint16_t);
}

void Page::GetPageDataFromFile(const vector<char>& data, const Table* table, uint32_t& offSet)
{
    this->GetPageMetaDataFromFile(data, table, offSet);

    const auto& columns = table->GetColumns();
    const int columnsSize = columns.size();

    for (int i = 0; i < this->metadata.pageSize; i++)
    {
         Row* row = new Row(*table);
         for(int j = 0; j < columnsSize; j++)
         {
             uint32_t bytesToRead = 0;
             memcpy(&bytesToRead, data.data() + offSet, sizeof(uint32_t));

             unsigned char* bytes = new unsigned char[bytesToRead];

             offSet += sizeof(uint32_t);

             memcpy(bytes, data.data() + offSet, bytesToRead);
             offSet += bytesToRead;

             if(columns[j]->GetColumnType() == ColumnType::String)
             {
                 //read from LargeDataObjectPage
                 //how to identify this is an offset(maybe type is string and parse as an Int)
             }

             Block* block = new Block(bytes, bytesToRead, columns[j]);

             row->InsertColumnData(block, j);

             delete[] bytes;
         }
         this->rows.push_back(row);
    }
}

void Page::WritePageMetaDataToFile(fstream* filePtr)
{
    filePtr->write(reinterpret_cast<char*>(&this->metadata.pageId), sizeof(uint16_t));
    filePtr->write(reinterpret_cast<char*>(&this->metadata.nextPageId), sizeof(uint16_t));
    filePtr->write(reinterpret_cast<char*>(&this->metadata.pageSize), sizeof(uint16_t));
    filePtr->write(reinterpret_cast<char*>(&this->metadata.bytesLeft), sizeof(uint16_t));
}

void Page::WritePageToFile(fstream *filePtr)
{
    this->WritePageMetaDataToFile(filePtr);

    for(const auto& row: this->rows)
    {
        for(const auto& block : row->GetData())
        {
            const auto& dataSize = block->GetBlockSize();
            filePtr->write(reinterpret_cast<const char *>(&dataSize), sizeof(uint32_t));

            const auto& data = block->GetBlockData();
            filePtr->write(reinterpret_cast<const char *>(data), dataSize);
        }
    }
}

void Page::SetNextPageId(const int &nextPageId) { this->metadata.nextPageId = nextPageId; }

void Page::SetFileName(const string &filename) { this->filename = filename; }

const string& Page::GetFileName() const { return this->filename; }

const uint16_t& Page::GetPageId() const { return this->metadata.pageId; }

const bool& Page::GetPageDirtyStatus() const { return this->isDirty; }

const uint16_t& Page::GetBytesLeft() const { return this->metadata.bytesLeft; }

const uint16_t& Page::GetNextPageId() const { return this->metadata.nextPageId; }

vector<Row> Page::GetRows(const Table& table) const
{
    vector<Row> copiedRows;
    for(const auto& row: this->rows)
    {
        vector<Block*> copyBlocks;
        for(const auto& block : row->GetData())
        {
            Block* blockCopy = new Block(block);
            copyBlocks.push_back(blockCopy);
        }
        copiedRows.emplace_back(table, copyBlocks);
    }

    return copiedRows;
}