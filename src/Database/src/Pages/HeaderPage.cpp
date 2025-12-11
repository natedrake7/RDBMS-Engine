#include "../../include/Pages/HeaderPage.h"
#include "../../include/Database.h"
#include "../../include/DataStorage/Table.h"
#include <cstring>

namespace Pages
{
    HeaderPage::HeaderPage(const int &pageId) : Page(pageId)
    {
        this->databaseHeader = new DatabaseEngine::DatabaseHeader();
        this->isDirty = true;
        this->header.pageType = Constants::PageType::METADATA;
        this->priority = Constants::PagePriority::SYSTEM;
    }

    HeaderPage::HeaderPage() : Page()
    {
        this->databaseHeader = new DatabaseEngine::DatabaseHeader();
        this->isDirty = true;
        this->header.pageType = Constants::PageType::METADATA;
        this->priority = Constants::PagePriority::SYSTEM;
    }

    HeaderPage::HeaderPage(const PageHeader &pageHeader) : Page(pageHeader)
    {
        this->databaseHeader = new DatabaseEngine::DatabaseHeader();
        this->priority = Constants::PagePriority::SYSTEM;
    }

    HeaderPage::~HeaderPage()
    {
        delete this->databaseHeader;
    }

    void HeaderPage::WriteToDisk(fstream *filePtr)
    {
        this->WritePageHeaderToDisk(filePtr);

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

    void HeaderPage::ReadFromDisk(const vector<char> &data, const DatabaseEngine::StorageTypes::Table *table, page_offset_t &offSet, fstream *filePtr)
    {
        memcpy(&this->databaseHeader->numberOfTables, data.data() + offSet, sizeof(table_number_t));
        offSet += sizeof(table_number_t);

        memcpy(&this->databaseHeader->lastPageFreeSpacePageId, data.data() + offSet, sizeof(page_id_t));
        offSet += sizeof(page_id_t);

        memcpy(&this->databaseHeader->lastGamPageId, data.data() + offSet, sizeof(page_id_t));
        offSet += sizeof(page_id_t);

        for (int i = 0; i < this->databaseHeader->numberOfTables; i++)
        {
            DatabaseEngine::StorageTypes::TableHeader tableHeader;

            memcpy(&tableHeader.tableId, data.data() + offSet, sizeof(table_id_t));
            offSet += sizeof(table_id_t);

            memcpy(&tableHeader.indexAllocationMapPageId, data.data() + offSet, sizeof(page_id_t));
            offSet += sizeof(page_id_t);

            memcpy(&tableHeader.numberOfColumns, data.data() + offSet, sizeof(column_number_t));
            offSet += sizeof(column_number_t);

            memcpy(&tableHeader.clusteredIndexPageId, data.data() + offSet, sizeof(page_id_t));
            offSet += sizeof(page_id_t);

            uint8_t numberOfNonClusteredIndexes;
            memcpy(&numberOfNonClusteredIndexes, data.data() + offSet, sizeof(uint8_t));
            offSet += sizeof(uint8_t);

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

    void HeaderPage::SetDbHeader(const DatabaseEngine::DatabaseHeader &header)
    {
        *this->databaseHeader = header;
        this->isDirty = true;

        this->tablesHeaders.clear();
    }

    void HeaderPage::SetTableHeader(const DatabaseEngine::StorageTypes::Table* table)
    {
        const DatabaseEngine::StorageTypes::TableHeader& header = table->GetHeader();

        this->tablesHeaders.push_back(header);

        this->isDirty = true;
    }

    const DatabaseEngine::DatabaseHeader *HeaderPage::GetDatabaseHeader() const { return this->databaseHeader; }

    const vector<DatabaseEngine::StorageTypes::TableHeader> &HeaderPage::GetTablesFullHeaders() const { return this->tablesHeaders; }
}
