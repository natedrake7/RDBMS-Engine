#include "Page.h"

PageHeader::PageHeader()
{
    this->pageType = PageType::DATA;
    this->pageId = 0;
    this->pageSize = 0;
    this->bytesLeft = PAGE_SIZE - GetPageHeaderSize();
}

PageHeader::~PageHeader() = default;

page_size_t PageHeader::GetPageHeaderSize() { return sizeof(page_id_t) + 2 * sizeof(page_size_t) + sizeof(PageType); }

Page::Page(const page_id_t& pageId, const bool& isPageCreation)
{
    this->header.pageId = pageId;
    this->isDirty = isPageCreation;
    this->header.pageType = PageType::DATA;
}

Page::Page()
{
    this->isDirty = false;
    this->header.pageType = PageType::DATA;
}

Page::Page(const PageHeader &pageHeader)
{
    this->header = pageHeader;
    this->isDirty = false;
}

Page::~Page()
{
    for(const auto& row: this->rows)
        delete row;

    cout<< "Deleting Page: "<< this->header.pageId << endl;
}

void Page::InsertRow(Row* row)
{
    this->rows.push_back(row);
    this->header.bytesLeft -=  row->GetTotalRowSize();
    this->header.pageSize++;
    this->isDirty = true;
}

void Page::DeleteRow(Row* row)
{
    this->isDirty = true;
}

void Page::GetPageDataFromFile(const vector<char>& data, const Table* table, page_offset_t& offSet, fstream* filePtr)
{
    const auto& columns = table->GetColumns();

    for (int i = 0; i < this->header.pageSize; i++)
    {
        Row* row = new Row(*table);
        RowHeader* rowHeader = row->GetHeader();
        
        memcpy(&rowHeader->rowSize, data.data() + offSet, sizeof(row_size_t));
        offSet += sizeof(row_size_t);

        memcpy(&rowHeader->maxRowSize, data.data() + offSet, sizeof(size_t));
        offSet += sizeof(size_t);

        rowHeader->nullBitMap->GetDataFromFile(data, offSet);
        rowHeader->largeObjectBitMap->GetDataFromFile(data, offSet);

         for(int j = 0; j < columns.size(); j++)
         {
             if (rowHeader->nullBitMap->Get(j))
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

void Page::WritePageHeaderToFile(fstream* filePtr)
{
    filePtr->write(reinterpret_cast<char*>(&this->header.pageId), sizeof(page_id_t));
    filePtr->write(reinterpret_cast<char*>(&this->header.pageSize), sizeof(page_size_t));
    filePtr->write(reinterpret_cast<char*>(&this->header.bytesLeft), sizeof(page_size_t));
    filePtr->write(reinterpret_cast<char*>(&this->header.pageType), sizeof(PageType));
}

void Page::WritePageToFile(fstream *filePtr)
{
    this->WritePageHeaderToFile(filePtr);

    for(const auto& row: this->rows)
    {
        RowHeader* rowHeader = row->GetHeader();

        filePtr->write(reinterpret_cast<const char* >(&rowHeader->rowSize), sizeof(row_size_t));
        filePtr->write(reinterpret_cast<const char* >(&rowHeader->maxRowSize), sizeof(size_t));
        rowHeader->nullBitMap->WriteDataToFile(filePtr);
        rowHeader->largeObjectBitMap->WriteDataToFile(filePtr);

        column_index_t columnIndex = 0;
        for(const auto& block : row->GetData())
        {
            if(rowHeader->nullBitMap->Get(columnIndex))
            {
                columnIndex++;
                continue;
            }
            
            block_size_t dataSize = block->GetBlockSize();

            filePtr->write(reinterpret_cast<const char *>(&dataSize), sizeof(block_size_t));

            const auto& data = block->GetBlockData();
            
            filePtr->write(reinterpret_cast<const char *>(data), dataSize);
            
            columnIndex++;
        }
    }
}

void Page::SetFileName(const string &filename) { this->filename = filename; }

void Page::SetPageId(const page_id_t &pageId) { this->header.pageId = pageId; }

const string& Page::GetFileName() const { return this->filename; }

const page_id_t& Page::GetPageId() const { return this->header.pageId; }

const bool& Page::GetPageDirtyStatus() const { return this->isDirty; }

const page_size_t& Page::GetBytesLeft() const { return this->header.bytesLeft; }

page_size_t Page::GetPageSize() const { return this->header.pageSize; }

const PageType & Page::GetPageType() const { return this->header.pageType; }

void Page::UpdateRows(const vector<Block> *updates, const vector<RowCondition *> *conditions)
{
    for (const auto& row : this->rows)
    {
        bool updateRow = false;
        if(conditions != nullptr) 
        {
            for(const auto& condition: *conditions)
                if(*condition != row->GetData()[condition->GetColumnIndex()])
                {
                    updateRow = true;
                    break;
                }
            
            if(updateRow)
                continue;
        }

        
    }

    this->isDirty = true;
}

void Page::GetRows(vector<Row>* copiedRows, const Table& table, const vector<RowCondition*>* conditions) const
{
    copiedRows->clear();
    for(const auto& row: this->rows)
    {
        if(conditions != nullptr)
        {
            bool skipRow = false;
            for(const auto& condition: *conditions)
                if(*condition != row->GetData()[condition->GetColumnIndex()])
                {
                    skipRow = true;
                    break;
                }
            
            if(skipRow)
                continue;
        }

        RowHeader* rowHeader = row->GetHeader();
        
        vector<Block*> copyBlocks;
        
        for(const auto& block : row->GetData())
        {
            Block* blockCopy = new Block(block);
            if (rowHeader->largeObjectBitMap->Get(block->GetColumnIndex()))
            {
                DataObjectPointer objectPointer;
                memcpy(&objectPointer, block->GetBlockData(), sizeof(DataObjectPointer));

                char* largeValue = row->GetLargeObjectValue(objectPointer);
                blockCopy->SetData(largeValue, strlen(largeValue) + 1);

                delete[] largeValue;
            }

            copyBlocks.push_back(blockCopy);
        }
        copiedRows->emplace_back(table, copyBlocks);
    }
}
