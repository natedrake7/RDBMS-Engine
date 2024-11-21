#include "Page.h"

PageMetadata::PageMetadata()
{
    this->extentId = 0;
    this->pageId = 0;
    this->pageSize = 0;
    this->nextPageId = 0;
    this->bytesLeft = PAGE_SIZE - 2 * sizeof(page_id_t) - 2 * sizeof(page_size_t);
}

PageMetadata::~PageMetadata() = default;

Page::Page(const page_id_t& pageId)
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
    this->metadata.bytesLeft -= (row->GetRowSize() + table.GetNumberOfColumns() * sizeof(block_size_t));
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

void Page::GetPageMetaDataFromFile(const vector<char> &data, page_offset_t &offSet)
{
    memcpy(&this->metadata.pageId, data.data() + offSet, sizeof(page_id_t));
    offSet += sizeof(page_id_t);

    memcpy(&this->metadata.nextPageId, data.data() + offSet, sizeof(page_id_t));
    offSet += sizeof(page_id_t);

    memcpy(&this->metadata.pageSize, data.data() + offSet, sizeof(page_size_t));
    offSet += sizeof(page_size_t);

    memcpy(&this->metadata.bytesLeft, data.data() + offSet, sizeof(page_size_t));
    offSet += sizeof(page_size_t);
}

void Page::GetPageDataFromFile(const vector<char>& data, const Table* table, page_offset_t& offSet, fstream* filePtr)
{
    this->GetPageMetaDataFromFile(data, offSet);

    const auto& columns = table->GetColumns();
    const int columnsSize = columns.size();

    for (int i = 0; i < this->metadata.pageSize; i++)
    {
         Row* row = new Row(*table);
         for(int j = 0; j < columnsSize; j++)
         {
             block_size_t bytesToRead = 0;
             memcpy(&bytesToRead, data.data() + offSet, sizeof(block_size_t));

             unsigned char* bytes = new unsigned char[bytesToRead];

             offSet += sizeof(block_size_t);

             memcpy(bytes, data.data() + offSet, bytesToRead);
             offSet += bytesToRead;

             //all strings are by default 2 characters because of the null terminating character
             //so if it is 1,it is a boolean indicating that the string is stored in another table
             bool isLargeObject = false;
             if(columns[j]->GetColumnType() == ColumnType::String
                 && bytesToRead == 1)
             {
                 delete[] bytes;

                 memcpy(&isLargeObject, data.data() + offSet, sizeof(bool));
                 offSet += sizeof(bool);

                 bytes = new unsigned char[sizeof(DataObjectPointer)];

                 memcpy(bytes, data.data() + offSet, sizeof(DataObjectPointer));

                 offSet += sizeof(DataObjectPointer);
                 bytesToRead = sizeof(DataObjectPointer);

                 DataObjectPointer objectPointer;
                 memcpy(&objectPointer, bytes, sizeof(DataObjectPointer));


                 //get LargePage and assign a pointer to the object, if more than 1 pages are required for the object
                 //get also pointers to the other objects

                 // LargeDataPage* largeDataPage = table->GetLargeDataPage(objectPointer.pageId);
             }

             Block* block = new Block(bytes, bytesToRead, columns[j]);

             block->SetIsLargeObject(isLargeObject);

             row->InsertColumnData(block, j);

             delete[] bytes;
         }
         this->rows.push_back(row);
    }
}

// unsigned char* Page::RetrieveDataFromLOBPage(DataObjectPointer& objectPointer
//                                             , fstream* filePtr
//                                             , const vector<char>& data
//                                             , uint32_t& offSet)
// {
//     filePtr->clear();
//     filePtr->seekg(0, ios::beg);
//     filePtr->seekg(objectPointer.objectOffset);
//
//     memcpy(&objectPointer, data.data() + offSet, sizeof(DataObjectPointer));
//     offSet += sizeof(DataObjectPointer);
//
//     char* bytes = new char[objectPointer.objectSize];
//     filePtr->read(bytes, objectPointer.objectSize);
//
//     return reinterpret_cast<unsigned char*>(bytes);
// }


void Page::WritePageMetaDataToFile(fstream* filePtr)
{
    filePtr->write(reinterpret_cast<char*>(&this->metadata.pageId), sizeof(page_id_t));
    filePtr->write(reinterpret_cast<char*>(&this->metadata.nextPageId), sizeof(page_id_t));
    filePtr->write(reinterpret_cast<char*>(&this->metadata.pageSize), sizeof(page_size_t));
    filePtr->write(reinterpret_cast<char*>(&this->metadata.bytesLeft), sizeof(page_size_t));
}

void Page::WritePageToFile(fstream *filePtr)
{
    this->WritePageMetaDataToFile(filePtr);

    for(const auto& row: this->rows)
    {
        for(const auto& block : row->GetData())
        {
            block_size_t dataSize = block->GetBlockSize();

            if(block->IsLargeObject())
            {
                dataSize = 1;

                filePtr->write(reinterpret_cast<const char *>(&dataSize), sizeof(block_size_t));
                filePtr->write(reinterpret_cast<const char *>(&block->IsLargeObject()), sizeof(bool));
                filePtr->write(reinterpret_cast<const char *>(block->GetBlockData()), sizeof(DataObjectPointer));

                continue;
            }

            filePtr->write(reinterpret_cast<const char *>(&dataSize), sizeof(block_size_t));

            const auto& data = block->GetBlockData();
            filePtr->write(reinterpret_cast<const char *>(data), dataSize);
        }
    }
}

void Page::SetNextPageId(const page_id_t &nextPageId) { this->metadata.nextPageId = nextPageId; }

void Page::SetFileName(const string &filename) { this->filename = filename; }

const string& Page::GetFileName() const { return this->filename; }

const page_id_t& Page::GetPageId() const { return this->metadata.pageId; }

const bool& Page::GetPageDirtyStatus() const { return this->isDirty; }

const page_size_t& Page::GetBytesLeft() const { return this->metadata.bytesLeft; }

const page_id_t& Page::GetNextPageId() const { return this->metadata.nextPageId; }

page_size_t Page::GetPageSize() const { return this->metadata.pageSize; }

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

