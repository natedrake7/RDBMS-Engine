#include "Page.h"
#include "../Database.h"
#include "../Table/Table.h"
#include "../Row/Row.h"
#include "./LargeObject/LargeDataPage.h"
#include "../../AdditionalLibraries/BitMap/BitMap.h"
#include "../Block/Block.h"

using namespace DatabaseEngine::StorageTypes;

namespace Pages
{
    PageHeader::PageHeader()
    {
        this->pageType = PageType::DATA;
        this->pageId = 0;
        this->pageSize = 0;
        this->bytesLeft = PAGE_SIZE - GetPageHeaderSize();
    }

    PageHeader::~PageHeader() = default;

    page_size_t PageHeader::GetPageHeaderSize() { return sizeof(page_id_t) + 2 * sizeof(page_size_t) + sizeof(PageType); }

    Page::Page(const page_id_t &pageId, const bool &isPageCreation)
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
        for (const auto &row : this->rows)
            delete row;
    }

    void Page::InsertRow(Row *row, int* indexPosition)
    {
        this->rows.push_back(row);
        
        if(indexPosition != nullptr)
            *indexPosition = this->rows.size() - 1;

        this->header.bytesLeft -= row->GetTotalRowSize();
        this->header.pageSize++;
        this->isDirty = true;
    }

    void Page::InsertRow(Row *row, const int& indexPosition)
    {
        this->rows.insert(this->rows.begin() + indexPosition, row);
        this->header.bytesLeft -= row->GetTotalRowSize();
        this->header.pageSize++;
        this->isDirty = true;
    }

    void Page::DeleteRow(Row *row)
    {
        this->isDirty = true;
    }

    void Page::GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t &offSet, fstream *filePtr)
    {
        const auto &columns = table->GetColumns();

        for (int i = 0; i < this->header.pageSize; i++)
        {
            Row *row = new Row(*table);
            RowHeader *rowHeader = row->GetHeader();

            memcpy(&rowHeader->rowSize, data.data() + offSet, sizeof(row_size_t));
            offSet += sizeof(row_size_t);

            memcpy(&rowHeader->maxRowSize, data.data() + offSet, sizeof(size_t));
            offSet += sizeof(size_t);

            rowHeader->nullBitMap->GetDataFromFile(data, offSet);
            rowHeader->largeObjectBitMap->GetDataFromFile(data, offSet);

            for (int j = 0; j < columns.size(); j++)
            {
                if (rowHeader->nullBitMap->Get(j))
                {
                    Block *block = new Block(nullptr, 0, columns[j]);

                    row->InsertColumnData(block, j);

                    continue;
                }

                block_size_t bytesToRead;

                memcpy(&bytesToRead, data.data() + offSet, sizeof(block_size_t));
                offSet += sizeof(block_size_t);

                object_t *bytes = new unsigned char[bytesToRead];
                memcpy(bytes, data.data() + offSet, bytesToRead);

                offSet += bytesToRead;

                Block *block = new Block(bytes, bytesToRead, columns[j]);

                row->InsertColumnData(block, j);

                delete[] bytes;
            }
            this->rows.push_back(row);
        }
    }

    void Page::WritePageHeaderToFile(fstream *filePtr) const
    {
        filePtr->write(reinterpret_cast<const char *>(&this->header.pageId), sizeof(page_id_t));
        filePtr->write(reinterpret_cast<const char *>(&this->header.pageSize), sizeof(page_size_t));
        filePtr->write(reinterpret_cast<const char *>(&this->header.bytesLeft), sizeof(page_size_t));
        filePtr->write(reinterpret_cast<const char *>(&this->header.pageType), sizeof(PageType));
    }

    void Page::WritePageToFile(fstream *filePtr)
    {
        this->WritePageHeaderToFile(filePtr);

        for (const auto &row : this->rows)
        {
            RowHeader *rowHeader = row->GetHeader();

            filePtr->write(reinterpret_cast<const char *>(&rowHeader->rowSize), sizeof(row_size_t));
            filePtr->write(reinterpret_cast<const char *>(&rowHeader->maxRowSize), sizeof(size_t));
            rowHeader->nullBitMap->WriteDataToFile(filePtr);
            rowHeader->largeObjectBitMap->WriteDataToFile(filePtr);

            column_index_t columnIndex = 0;
            for (const auto &block : row->GetData())
            {
                if (rowHeader->nullBitMap->Get(columnIndex))
                {
                    columnIndex++;
                    continue;
                }

                block_size_t dataSize = block->GetBlockSize();

                filePtr->write(reinterpret_cast<const char *>(&dataSize), sizeof(block_size_t));

                const auto &data = block->GetBlockData();

                filePtr->write(reinterpret_cast<const char *>(data), dataSize);

                columnIndex++;
            }
        }
    }

    void Page::SetFileName(const string &filename) { this->filename = filename; }

    void Page::SetPageId(const page_id_t &pageId) { this->header.pageId = pageId; }

    void Page::UpdatePageSize() { this->header.pageSize = this->rows.size(); }

    void Page::UpdateBytesLeft()
    {
        this->header.bytesLeft = PAGE_SIZE - this->header.GetPageHeaderSize();

        for (const auto &row : this->rows)
            this->header.bytesLeft -= row->GetTotalRowSize();

        this->isDirty = true;
    }

    void Page::UpdateBytesLeft(const row_size_t& previousRowSize, const row_size_t& currentRowSize)
    {
        this->header.bytesLeft += static_cast<int64_t>(currentRowSize) -static_cast<int64_t>(previousRowSize);

        this->isDirty = true;
    }

    const string &Page::GetFileName() const { return this->filename; }

    const page_id_t &Page::GetPageId() const { return this->header.pageId; }

    const bool &Page::GetPageDirtyStatus() const { return this->isDirty; }

    const page_size_t &Page::GetBytesLeft() const { return this->header.bytesLeft; }

    page_size_t Page::GetPageSize() const { return this->header.pageSize; }

    const PageType &Page::GetPageType() const { return this->header.pageType; }

    void Page::UpdateRows(const vector<Block> *updates, const vector<Field> *conditions)
    {
        for (const auto &row : this->rows)
        {
            bool updateRow = false;
            if (conditions != nullptr)
            {
                // for (const auto &condition : *conditions)
                //     if (*condition != row->GetData()[condition->GetColumnIndex()])
                //     {
                //         updateRow = true;
                //         break;
                //     }

                if (updateRow)
                    continue;
            }
        }

        this->isDirty = true;
    }

    void Page::GetRows(vector<Row> *copiedRows, const Table &table, const size_t &rowsToSelect, const vector<Field> *conditions) const
    {
        for (const auto &row : this->rows)
        {
            if (copiedRows->size() >= rowsToSelect)
                return;

            if (conditions != nullptr)
            {
                bool skipRow = false;
                // for (const auto &condition : *conditions)
                //     if (condition != row->GetData()[condition->GetColumnIndex()])
                //     {
                //         skipRow = true;
                //         break;
                //     }

                if (skipRow)
                    continue;
            }

            RowHeader *rowHeader = row->GetHeader();

            vector<Block *> copyBlocks;

            for (const auto &block : row->GetData())
            {
                Block *blockCopy = new Block(block);
                if (rowHeader->largeObjectBitMap->Get(block->GetColumnIndex()))
                {
                    DataObjectPointer objectPointer;
                    memcpy(&objectPointer, block->GetBlockData(), sizeof(DataObjectPointer));

                    uint32_t objectSize;
                    unsigned char *largeValue = row->GetLargeObjectValue(objectPointer, &objectSize);
                    blockCopy->SetData(largeValue, objectSize);

                    delete[] largeValue;
                }

                copyBlocks.push_back(blockCopy);
            }
            copiedRows->emplace_back(table, copyBlocks, rowHeader->nullBitMap);
        }
    }

    void Page::GetRowByIndex(vector<Row>*& rows, const Table &table, const int &indexPosition, const vector<column_index_t>& selectedColumnIndices) const
    {
        const auto &row = this->rows[indexPosition];

        const RowHeader *rowHeader = row->GetHeader();

        vector<Block *> copyBlocks;

        const auto& rowData = row->GetData();
        for(const auto& columnIndex: selectedColumnIndices)
        {
            const auto& block = rowData[columnIndex];

            Block *blockCopy = new Block(block);
            if (rowHeader->largeObjectBitMap->Get(block->GetColumnIndex()))
            {
                DataObjectPointer objectPointer;
                memcpy(&objectPointer, block->GetBlockData(), sizeof(DataObjectPointer));

                uint32_t objectSize;
                const unsigned char *largeValue = row->GetLargeObjectValue(objectPointer, &objectSize);
                blockCopy->SetData(largeValue, objectSize);

                delete[] largeValue;
            }

            copyBlocks.push_back(blockCopy);
        }

        rows->emplace_back(table, copyBlocks, rowHeader->nullBitMap);
    }

    void Page::SplitPageRowByBranchingFactor(Page *nextLeafPage, const int &branchingFactor, const Table &table)
    {
        if (table.GetTableType() != TableType::CLUSTERED)
            return;

        vector<Row *> *nextLeafPageDataRows = nextLeafPage->GetDataRowsUnsafe();

        nextLeafPageDataRows->assign(this->rows.begin() + branchingFactor, this->rows.end());

        nextLeafPage->UpdatePageSize();
        nextLeafPage->UpdateBytesLeft();

        this->rows.resize(branchingFactor);

        this->UpdatePageSize();
        this->UpdateBytesLeft();
    }

    vector<DatabaseEngine::StorageTypes::Row *>* Page::GetDataRowsUnsafe() { return &this->rows; }

    void Page::UpdateRows(const vector<Block*> *updates, const vector<Field> *conditions)
    {
        //add condition checking prior to update
        for (auto &row : this->rows)
        {
            RowHeader *rowHeader = row->GetHeader();
            
            vector<Block*> rowData = row->GetData();

            for(const auto& update: *updates)
            {
                const auto& columnIndex = update->GetColumnIndex();

                if (rowHeader->largeObjectBitMap->Get(columnIndex))
                {
                    //set lob objects

                    continue;
                }

                if(row)

                rowData[columnIndex]->SetData(update->GetBlockData(), update->GetBlockSize());
            }

            for (const auto &block : row->GetData())
            {
                Block *blockCopy = new Block(block);
                if (rowHeader->largeObjectBitMap->Get(block->GetColumnIndex()))
                {
                    DataObjectPointer objectPointer;
                    memcpy(&objectPointer, block->GetBlockData(), sizeof(DataObjectPointer));

                    uint32_t objectSize;
                    unsigned char *largeValue = row->GetLargeObjectValue(objectPointer, &objectSize);
                    blockCopy->SetData(largeValue, objectSize);

                    delete[] largeValue;
                }
            }
        }
    }
}
