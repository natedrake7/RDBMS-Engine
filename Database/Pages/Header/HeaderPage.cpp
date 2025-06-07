#include "HeaderPage.h"
#include "../../Database.h"
#include "../../Table/Table.h"
#include "../../../AdditionalLibraries/BitMap/BitMap.h"
#include "../../Column/Column.h"

#include <cstring>

using namespace DatabaseEngine;
using namespace DatabaseEngine::StorageTypes;
using namespace ByteMaps;

namespace Pages
{
    HeaderPage::HeaderPage(const int &pageId) : Page(pageId)
    {
        this->databaseHeader = new DatabaseHeader();
        this->isDirty = true;
        this->header.pageType = PageType::METADATA;
    }

    HeaderPage::HeaderPage() : Page()
    {
        this->databaseHeader = new DatabaseHeader();
        ;
        this->isDirty = true;
        this->header.pageType = PageType::METADATA;
    }

    HeaderPage::HeaderPage(const PageHeader &pageHeader) : Page(pageHeader)
    {
        this->databaseHeader = new DatabaseHeader();
    }

    HeaderPage::~HeaderPage()
    {
        delete this->databaseHeader;
    }

    void HeaderPage::WritePageToFile(fstream *filePtr)
    {
        this->WritePageHeaderToFile(filePtr);

        filePtr->write(reinterpret_cast<const char *>(&this->databaseHeader->numberOfTables), sizeof(table_number_t));
        filePtr->write(reinterpret_cast<const char *>(&this->databaseHeader->lastPageFreeSpacePageId), sizeof(page_id_t));
        filePtr->write(reinterpret_cast<const char *>(&this->databaseHeader->lastGamPageId), sizeof(page_id_t));

        for (const auto &tableHeader : this->tablesHeaders)
        {
            filePtr->write(reinterpret_cast<const char *>(&tableHeader.tableId), sizeof(table_id_t));
            filePtr->write(reinterpret_cast<const char *>(&tableHeader.indexAllocationMapPageId), sizeof(page_id_t));
            filePtr->write(reinterpret_cast<const char *>(&tableHeader.numberOfColumns), sizeof(column_number_t));
            filePtr->write(reinterpret_cast<const char *>(&tableHeader.clusteredIndexPageId), sizeof(page_id_t));

            // tableHeader.columnsNullBitMap->WriteDataToFile(filePtr);

//            const uint8_t numberOfClusteredIndexedColumns = tableHeader.clusteredColumnIndexes.size();
//            filePtr->write(reinterpret_cast<const char *>(&numberOfClusteredIndexedColumns), sizeof(uint8_t));
//            filePtr->write(reinterpret_cast<const char *>(tableHeader.clusteredColumnIndexes.data()), numberOfClusteredIndexedColumns * sizeof(column_index_t));
//
//            const uint8_t numberOfNonClusteredIndexes = tableHeader.nonClusteredColumnIndexes.size();
//            filePtr->write(reinterpret_cast<const char *>(&numberOfNonClusteredIndexes), sizeof(uint8_t));
//
//            for(const auto& nonClusteredIndexes: tableHeader.nonClusteredColumnIndexes)
//            {
//                const uint8_t numberOfNonClusteredIndexedColumns = nonClusteredIndexes.size();
//                filePtr->write(reinterpret_cast<const char *>(&numberOfNonClusteredIndexedColumns), sizeof(uint8_t));
//
//                filePtr->write(reinterpret_cast<const char *>(nonClusteredIndexes.data()), numberOfNonClusteredIndexedColumns * sizeof(column_index_t));
//            }

            const uint8_t numOfNonClusteredIndexes = tableHeader.nonClusteredIndexes.size();

            filePtr->write(reinterpret_cast<const char *>(&numOfNonClusteredIndexes), sizeof(uint8_t));

            for(const auto& nonClusteredIndexPageId: tableHeader.nonClusteredIndexPageIds)
                filePtr->write(reinterpret_cast<const char *>(&nonClusteredIndexPageId), sizeof(page_id_t));

        }
    }

    void HeaderPage::GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t &offSet, fstream *filePtr)
    {
        memcpy(&this->databaseHeader->numberOfTables, data.data() + offSet, sizeof(table_number_t));
        offSet += sizeof(table_number_t);

        memcpy(&this->databaseHeader->lastPageFreeSpacePageId, data.data() + offSet, sizeof(page_id_t));
        offSet += sizeof(page_id_t);

        memcpy(&this->databaseHeader->lastGamPageId, data.data() + offSet, sizeof(page_id_t));
        offSet += sizeof(page_id_t);

        for (int i = 0; i < this->databaseHeader->numberOfTables; i++)
        {
            TableHeader tableHeader;

            memcpy(&tableHeader.tableId, data.data() + offSet, sizeof(table_id_t));
            offSet += sizeof(table_id_t);

            memcpy(&tableHeader.indexAllocationMapPageId, data.data() + offSet, sizeof(page_id_t));
            offSet += sizeof(page_id_t);

            memcpy(&tableHeader.numberOfColumns, data.data() + offSet, sizeof(column_number_t));
            offSet += sizeof(column_number_t);

            memcpy(&tableHeader.clusteredIndexPageId, data.data() + offSet, sizeof(page_id_t));
            offSet += sizeof(page_id_t);

            // tableHeader.columnsNullBitMap = new BitMap(tableHeader.numberOfColumns);
            // tableHeader.columnsNullBitMap->GetDataFromFile(data, offSet);

//            uint8_t numberOfClusteredIndexedColumns;
//            memcpy(&numberOfClusteredIndexedColumns, data.data() + offSet, sizeof(uint8_t));
//            offSet += sizeof(uint8_t);
//
//            for(int j = 0; j < numberOfClusteredIndexedColumns; j++)
//            {
//                column_index_t columnIndex;
//                memcpy(&columnIndex, data.data() + offSet, sizeof(column_index_t));
//                offSet += sizeof(column_index_t);
//
//                tableHeader.clusteredColumnIndexes.push_back(columnIndex);
//            }
//
            uint8_t numberOfNonClusteredIndexes;
            memcpy(&numberOfNonClusteredIndexes, data.data() + offSet, sizeof(uint8_t));
            offSet += sizeof(uint8_t);
//
//            tableHeader.nonClusteredColumnIndexes.resize(numberOfNonClusteredIndexes);
//            for(int j = 0; j < numberOfNonClusteredIndexes; j++)
//            {
//                uint8_t numberOfNonClusteredIndexedColumns;
//                memcpy(&numberOfNonClusteredIndexedColumns, data.data() + offSet, sizeof(uint8_t));
//                offSet += sizeof(uint8_t);
//
//                for(int k = 0; k < numberOfNonClusteredIndexedColumns; k++)
//                {
//                    column_index_t columnIndex;
//                    memcpy(&columnIndex, data.data() + offSet, sizeof(column_index_t));
//                    offSet += sizeof(column_index_t);
//
//                    tableHeader.nonClusteredColumnIndexes[j].push_back(columnIndex);
//                }
//            }

            for (int j = 0; j < numberOfNonClusteredIndexes; j++)
            {
                page_id_t pageId = 0;
                memcpy(&pageId, data.data() + offSet, sizeof(page_id_t));

                tableHeader.nonClusteredIndexPageIds.push_back(pageId);
                offSet += sizeof(page_id_t);
            }

            this->tablesHeaders.push_back(tableHeader);
        }
    }

    void HeaderPage::SetDbHeader(const DatabaseHeader &databaseHeader)
    {
        *this->databaseHeader = databaseHeader;
        this->isDirty = true;

        this->tablesHeaders.clear();
    }

    void HeaderPage::SetTableHeader(const Table* table)
    {
        const TableHeader& header = table->GetTableHeader();

        this->tablesHeaders.push_back(header);

        this->isDirty = true;
    }

    const DatabaseHeader *HeaderPage::GetDatabaseHeader() const { return this->databaseHeader; }

    const vector<TableHeader> &HeaderPage::GetTablesFullHeaders() const { return this->tablesHeaders; }
}
