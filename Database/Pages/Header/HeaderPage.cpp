#include "HeaderPage.h"
#include "../../Database.h"
#include "../../Table/Table.h"
#include "../../../AdditionalLibraries/BitMap/BitMap.h"
#include "../../Column/Column.h"

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

        filePtr->write(reinterpret_cast<const char *>(&this->databaseHeader->databaseNameSize), sizeof(header_literal_t));
        filePtr->write(this->databaseHeader->databaseName.c_str(), this->databaseHeader->databaseNameSize);
        filePtr->write(reinterpret_cast<const char *>(&this->databaseHeader->numberOfTables), sizeof(table_number_t));
        filePtr->write(reinterpret_cast<const char *>(&this->databaseHeader->lastTableId), sizeof(table_id_t));
        filePtr->write(reinterpret_cast<const char *>(&this->databaseHeader->lastPageFreeSpacePageId), sizeof(page_id_t));
        filePtr->write(reinterpret_cast<const char *>(&this->databaseHeader->lastGamPageId), sizeof(page_id_t));

        for (const auto &tableFullHeader : this->tablesHeaders)
        {
            filePtr->write(reinterpret_cast<const char *>(&tableFullHeader.tableHeader.tableId), sizeof(table_id_t));
            filePtr->write(reinterpret_cast<const char *>(&tableFullHeader.tableHeader.tableNameSize), sizeof(header_literal_t));
            filePtr->write(tableFullHeader.tableHeader.tableName.c_str(), tableFullHeader.tableHeader.tableNameSize);
            filePtr->write(reinterpret_cast<const char *>(&tableFullHeader.tableHeader.indexAllocationMapPageId), sizeof(page_id_t));
            filePtr->write(reinterpret_cast<const char *>(&tableFullHeader.tableHeader.maxRowSize), sizeof(row_size_t));
            filePtr->write(reinterpret_cast<const char *>(&tableFullHeader.tableHeader.numberOfColumns), sizeof(column_number_t));
            filePtr->write(reinterpret_cast<const char *>(&tableFullHeader.tableHeader.clusteredIndexPageId), sizeof(page_id_t));

            tableFullHeader.tableHeader.columnsNullBitMap->WriteDataToFile(filePtr);

            const uint8_t numberOfClusteredIndexedColumns = tableFullHeader.tableHeader.clusteredColumnIndexes.size();
            filePtr->write(reinterpret_cast<const char *>(&numberOfClusteredIndexedColumns), sizeof(uint8_t));
            filePtr->write(reinterpret_cast<const char *>(tableFullHeader.tableHeader.clusteredColumnIndexes.data()), numberOfClusteredIndexedColumns * sizeof(column_index_t));

            const uint8_t numberOfNonClusteredIndexes = tableFullHeader.tableHeader.nonClusteredColumnIndexes.size();
            filePtr->write(reinterpret_cast<const char *>(&numberOfNonClusteredIndexes), sizeof(uint8_t));

            for(const auto& nonClusteredIndexes: tableFullHeader.tableHeader.nonClusteredColumnIndexes)
            {
                const uint8_t numberOfNonClusteredIndexedColumns = nonClusteredIndexes.size();
                filePtr->write(reinterpret_cast<const char *>(&numberOfNonClusteredIndexedColumns), sizeof(uint8_t));

                filePtr->write(reinterpret_cast<const char *>(nonClusteredIndexes.data()), numberOfNonClusteredIndexedColumns * sizeof(column_index_t));  
            }


            for(const auto& nonClusteredIndexPageId: tableFullHeader.tableHeader.nonClusteredIndexPageIds)
                filePtr->write(reinterpret_cast<const char *>(&nonClusteredIndexPageId), sizeof(page_id_t));

            for(const auto& nonClusteredIndexPageId: tableFullHeader.tableHeader.nonClusteredIndexesIds)
                filePtr->write(reinterpret_cast<const char *>(&nonClusteredIndexPageId), sizeof(uint8_t));

            for (const auto &columnMetaData : tableFullHeader.columnsHeaders)
            {
                filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnNameSize), sizeof(header_literal_t));
                filePtr->write(columnMetaData.columnName.c_str(), columnMetaData.columnNameSize);
                filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnTypeLiteralSize), sizeof(header_literal_t));
                filePtr->write(columnMetaData.columnTypeLiteral.c_str(), columnMetaData.columnTypeLiteralSize);
                filePtr->write(reinterpret_cast<const char *>(&columnMetaData.recordSize), sizeof(row_size_t));
                filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnType), sizeof(ColumnType));
                filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnIndex), sizeof(column_index_t));
            }
        }
    }

    void HeaderPage::GetPageDataFromFile(const vector<char> &data, const Table *table, page_offset_t &offSet, fstream *filePtr)
    {
        memcpy(&this->databaseHeader->databaseNameSize, data.data() + offSet, sizeof(header_literal_t));
        offSet += sizeof(header_literal_t);

        this->databaseHeader->databaseName.resize(this->databaseHeader->databaseNameSize);
        memcpy(&this->databaseHeader->databaseName[0], data.data() + offSet, this->databaseHeader->databaseNameSize);
        offSet += this->databaseHeader->databaseNameSize;

        memcpy(&this->databaseHeader->numberOfTables, data.data() + offSet, sizeof(table_number_t));
        offSet += sizeof(table_number_t);

        memcpy(&this->databaseHeader->lastTableId, data.data() + offSet, sizeof(table_id_t));
        offSet += sizeof(table_id_t);

        memcpy(&this->databaseHeader->lastPageFreeSpacePageId, data.data() + offSet, sizeof(page_id_t));
        offSet += sizeof(page_id_t);

        memcpy(&this->databaseHeader->lastGamPageId, data.data() + offSet, sizeof(page_id_t));
        offSet += sizeof(page_id_t);

        for (int i = 0; i < this->databaseHeader->numberOfTables; i++)
        {
            TableFullHeader tableFullHeader;

            memcpy(&tableFullHeader.tableHeader.tableId, data.data() + offSet, sizeof(table_id_t));
            offSet += sizeof(table_id_t);

            memcpy(&tableFullHeader.tableHeader.tableNameSize, data.data() + offSet, sizeof(header_literal_t));
            offSet += sizeof(header_literal_t);
            tableFullHeader.tableHeader.tableName.resize(tableFullHeader.tableHeader.tableNameSize);

            memcpy(&tableFullHeader.tableHeader.tableName[0], data.data() + offSet, tableFullHeader.tableHeader.tableNameSize);
            offSet += tableFullHeader.tableHeader.tableNameSize;

            memcpy(&tableFullHeader.tableHeader.indexAllocationMapPageId, data.data() + offSet, sizeof(page_id_t));
            offSet += sizeof(page_id_t);

            memcpy(&tableFullHeader.tableHeader.maxRowSize, data.data() + offSet, sizeof(row_size_t));
            offSet += sizeof(row_size_t);

            memcpy(&tableFullHeader.tableHeader.numberOfColumns, data.data() + offSet, sizeof(column_number_t));
            offSet += sizeof(column_number_t);

            memcpy(&tableFullHeader.tableHeader.clusteredIndexPageId, data.data() + offSet, sizeof(page_id_t));
            offSet += sizeof(page_id_t);

            tableFullHeader.tableHeader.columnsNullBitMap = new BitMap(tableFullHeader.tableHeader.numberOfColumns);
            tableFullHeader.tableHeader.columnsNullBitMap->GetDataFromFile(data, offSet);

            uint8_t numberOfClusteredIndexedColumns;
            memcpy(&numberOfClusteredIndexedColumns, data.data() + offSet, sizeof(uint8_t));
            offSet += sizeof(uint8_t);

            for(int j = 0; j < numberOfClusteredIndexedColumns; j++)
            {
                column_index_t columnIndex;
                memcpy(&columnIndex, data.data() + offSet, sizeof(column_index_t));
                offSet += sizeof(column_index_t);

                tableFullHeader.tableHeader.clusteredColumnIndexes.push_back(columnIndex);
            }

            uint8_t numberOfNonClusteredIndexes;
            memcpy(&numberOfNonClusteredIndexes, data.data() + offSet, sizeof(uint8_t));
            offSet += sizeof(uint8_t);

            tableFullHeader.tableHeader.nonClusteredColumnIndexes.resize(numberOfNonClusteredIndexes);
            for(int j = 0; j < numberOfNonClusteredIndexes; j++)
            {
                uint8_t numberOfNonClusteredIndexedColumns;
                memcpy(&numberOfNonClusteredIndexedColumns, data.data() + offSet, sizeof(uint8_t));
                offSet += sizeof(uint8_t);

                for(int k = 0; k < numberOfNonClusteredIndexedColumns; k++)
                {
                    column_index_t columnIndex;
                    memcpy(&columnIndex, data.data() + offSet, sizeof(column_index_t));
                    offSet += sizeof(column_index_t);
                    
                    tableFullHeader.tableHeader.nonClusteredColumnIndexes[j].push_back(columnIndex);
                }
            }

            for (int j = 0; j < numberOfNonClusteredIndexes; j++)
            {
                page_id_t pageId = 0;
                memcpy(&pageId, data.data() + offSet, sizeof(page_id_t));

                tableFullHeader.tableHeader.nonClusteredIndexPageIds.push_back(pageId);
                offSet += sizeof(page_id_t);
            }

            for (int j = 0; j < numberOfNonClusteredIndexes; j++)
            {
                uint8_t indexId = 0;
                memcpy(&indexId, data.data() + offSet, sizeof(uint8_t));

                tableFullHeader.tableHeader.nonClusteredIndexesIds.push_back(indexId);
                offSet += sizeof(uint8_t);
            }

            for (int j = 0; j < tableFullHeader.tableHeader.numberOfColumns; j++)
            {
                ColumnHeader columnHeader;
                memcpy(&columnHeader.columnNameSize, data.data() + offSet, sizeof(header_literal_t));
                offSet += sizeof(header_literal_t);
                columnHeader.columnName.resize(columnHeader.columnNameSize);

                memcpy(&columnHeader.columnName[0], data.data() + offSet, columnHeader.columnNameSize);
                offSet += columnHeader.columnNameSize;

                memcpy(&columnHeader.columnTypeLiteralSize, data.data() + offSet, sizeof(header_literal_t));
                offSet += sizeof(header_literal_t);
                columnHeader.columnTypeLiteral.resize(columnHeader.columnTypeLiteralSize);

                memcpy(&columnHeader.columnTypeLiteral[0], data.data() + offSet, columnHeader.columnTypeLiteralSize);
                offSet += columnHeader.columnTypeLiteralSize;

                memcpy(&columnHeader.recordSize, data.data() + offSet, sizeof(row_size_t));
                offSet += sizeof(row_size_t);

                memcpy(&columnHeader.columnType, data.data() + offSet, sizeof(ColumnType));
                offSet += sizeof(ColumnType);

                memcpy(&columnHeader.columnIndex, data.data() + offSet, sizeof(column_index_t));
                offSet += sizeof(column_index_t);

                tableFullHeader.columnsHeaders.push_back(columnHeader);
            }
            this->tablesHeaders.push_back(tableFullHeader);
        }
    }

    void HeaderPage::SetDbHeader(const DatabaseHeader &databaseHeader)
    {
        *this->databaseHeader = databaseHeader;
        this->databaseHeader->databaseNameSize = this->databaseHeader->databaseName.size();
        this->isDirty = true;

        this->tablesHeaders.clear();
    }

    void HeaderPage::SetTableHeader(const Table* table)
    {
        TableFullHeader tableFullHeader;
        tableFullHeader.tableHeader = table->GetTableHeader();
        tableFullHeader.tableHeader.tableNameSize = tableFullHeader.tableHeader.tableName.size();

        const auto &columns = table->GetColumns();

        for (const auto &column : columns)
        {
            ColumnHeader columnHeader = column->GetColumnHeader();
            columnHeader.columnTypeLiteralSize = columnHeader.columnTypeLiteral.size();
            columnHeader.columnNameSize = columnHeader.columnName.size();

            tableFullHeader.columnsHeaders.push_back(columnHeader);
        }

        this->tablesHeaders.push_back(tableFullHeader);

        this->isDirty = true;
    }

    const DatabaseHeader *HeaderPage::GetDatabaseHeader() const { return this->databaseHeader; }

    const vector<TableFullHeader> &HeaderPage::GetTablesFullHeaders() const { return this->tablesHeaders; }
}
