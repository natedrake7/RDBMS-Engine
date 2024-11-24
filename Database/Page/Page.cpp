#include "Page.h"

PageMetadata::PageMetadata()
{
    this->pageType = PageType::DATA;
    this->pageId = 0;
    this->pageSize = 0;
    this->nextPageId = 0;
    this->bytesLeft = PAGE_SIZE - 2 * sizeof(page_id_t) - 2 * sizeof(page_size_t) - sizeof(PageType);
}

PageMetadata::~PageMetadata() = default;

Page::Page(const page_id_t& pageId)
{
    this->metadata.pageId = pageId;
    this->isDirty = false;
    this->metadata.pageType = PageType::DATA;
}

Page::Page()
{
    this->isDirty = false;
    this->metadata.pageType = PageType::DATA;
}

Page::Page(const PageMetaData &pageMetaData)
{
    this->metadata = pageMetaData;
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

void Page::GetPageDataFromFile(const vector<char>& data, const Table* table, page_offset_t& offSet, fstream* filePtr)
{
    const auto& columns = table->GetColumns();
    const int columnsSize = columns.size();

    for (int i = 0; i < this->metadata.pageSize; i++)
    {
        Row* row = new Row(*table);
        RowMetaData* rowMetaData = row->GetMetaData();
        
        memcpy(&rowMetaData->rowSize, data.data() + offSet, sizeof(uint32_t));
        offSet += sizeof(uint32_t);

        memcpy(&rowMetaData->maxRowSize, data.data() + offSet, sizeof(size_t));
        offSet += sizeof(size_t);

        rowMetaData->nullBitMap->GetDataFromFile(data, offSet);
        rowMetaData->largeObjectBitMap->GetDataFromFile(data, offSet);
        
         for(int j = 0; j < columnsSize; j++)
         {
             if (rowMetaData->nullBitMap->Get(j))
             {
                 Block* block = new Block(nullptr, 0, columns[j]);

                 row->InsertColumnData(block, j);

                 continue;
             }
             
             block_size_t bytesToRead;

             memcpy(&bytesToRead, data.data() + offSet, sizeof(block_size_t));
             offSet += sizeof(block_size_t);

             object_t* bytes = new unsigned char[bytesToRead];
             memcpy(bytes, data.data() + offSet, bytesToRead);

             offSet += bytesToRead;
             
             Block* block = new Block(bytes, bytesToRead, columns[j]);

             row->InsertColumnData(block, j);

             delete[] bytes;
         }
         this->rows.push_back(row);
    }
}

void Page::WritePageMetaDataToFile(fstream* filePtr)
{
    filePtr->write(reinterpret_cast<char*>(&this->metadata.pageId), sizeof(page_id_t));
    filePtr->write(reinterpret_cast<char*>(&this->metadata.nextPageId), sizeof(page_id_t));
    filePtr->write(reinterpret_cast<char*>(&this->metadata.pageSize), sizeof(page_size_t));
    filePtr->write(reinterpret_cast<char*>(&this->metadata.bytesLeft), sizeof(page_size_t));
    filePtr->write(reinterpret_cast<char*>(&this->metadata.pageType), sizeof(PageType));
}

void Page::WritePageToFile(fstream *filePtr)
{
    this->WritePageMetaDataToFile(filePtr);

    for(const auto& row: this->rows)
    {
        RowMetaData* rowMetaData = row->GetMetaData();

        filePtr->write(reinterpret_cast<const char* >(&rowMetaData->rowSize), sizeof(uint32_t));
        filePtr->write(reinterpret_cast<const char* >(&rowMetaData->maxRowSize), sizeof(size_t));
        rowMetaData->nullBitMap->WriteDataToFile(filePtr);
        rowMetaData->largeObjectBitMap->WriteDataToFile(filePtr);

        uint16_t counter = 0;
        for(const auto& block : row->GetData())
        {
            block_size_t dataSize = block->GetBlockSize();

            filePtr->write(reinterpret_cast<const char *>(&dataSize), sizeof(block_size_t));

            const auto& data = block->GetBlockData();
            
            if (rowMetaData->largeObjectBitMap->Get(counter))
                filePtr->write(reinterpret_cast<const char *>(data), sizeof(DataObjectPointer));
            else
                filePtr->write(reinterpret_cast<const char *>(data), dataSize);
            
            counter++;
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
    for(const auto row: this->rows)
    {
        RowMetaData* rowMetaData = row->GetMetaData();
        
        vector<Block*> copyBlocks;
        
        for(const auto& block : row->GetData())
        {
            Block* blockCopy = new Block(block);
            if (rowMetaData->largeObjectBitMap->Get(block->GetColumnIndex()))
            {
                DataObjectPointer objectPointer;
                memcpy(&objectPointer, block->GetBlockData(), sizeof(DataObjectPointer));

                char* largeValue = row->GetLargeObjectValue(objectPointer);
                blockCopy->SetData(largeValue, strlen(largeValue) + 1);

                delete[] largeValue;
            }

            copyBlocks.push_back(blockCopy);
        }
        copiedRows.emplace_back(table, copyBlocks);
    }

    return copiedRows;
}
