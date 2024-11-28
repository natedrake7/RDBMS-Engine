#include "HeaderPage.h"

HeaderPage::HeaderPage(const int &pageId) : Page(pageId)
{
    this->isDirty = false;
    this->header.pageType = PageType::METADATA;
}

HeaderPage::HeaderPage() : Page()
{
    this->isDirty = true;
    this->header.pageType = PageType::METADATA;
}

HeaderPage::HeaderPage(const PageHeader& pageHeader) : Page(pageHeader) { }

HeaderPage::~HeaderPage() = default;

void HeaderPage::WritePageToFile(fstream *filePtr)
{
    filePtr->write(reinterpret_cast<const char*>(&this->databaseHeader.databaseNameSize), sizeof(metadata_literal_t));
    filePtr->write(this->databaseHeader.databaseName.c_str(), this->databaseHeader.databaseNameSize);
    filePtr->write(reinterpret_cast<const char*>(&this->databaseHeader.lastExtentId), sizeof(extent_id_t));
    filePtr->write(reinterpret_cast<const char*>(&this->databaseHeader.lastLargeExtentId), sizeof(extent_id_t));
    filePtr->write(reinterpret_cast<const char*>(&this->databaseHeader.numberOfTables), sizeof(table_number_t));
    filePtr->write(reinterpret_cast<const char*>(&this->databaseHeader.lastLargePageId), sizeof(page_id_t));
    filePtr->write(reinterpret_cast<const char*>(&this->databaseHeader.lastTableId), sizeof(table_id_t));

    for(const auto& tableFullHeader: this->tablesHeaders)
    {
        filePtr->write(reinterpret_cast<const char *>(&tableFullHeader.tableHeader.tableId), sizeof(table_id_t));
        filePtr->write(reinterpret_cast<const char *>(&tableFullHeader.tableHeader.tableNameSize), sizeof(metadata_literal_t));
        filePtr->write(tableFullHeader.tableHeader.tableName.c_str(), tableFullHeader.tableHeader.tableNameSize);
        filePtr->write(reinterpret_cast<const char *>(&tableFullHeader.tableHeader.firstPageId), sizeof(page_id_t));
        filePtr->write(reinterpret_cast<const char *>(&tableFullHeader.tableHeader.lastPageId), sizeof(page_id_t));
        filePtr->write(reinterpret_cast<const char *>(&tableFullHeader.tableHeader.maxRowSize), sizeof(row_size_t));
        filePtr->write(reinterpret_cast<const char *>(&tableFullHeader.tableHeader.numberOfColumns), sizeof(column_number_t));
        filePtr->write(reinterpret_cast<const char *>(&tableFullHeader.tableHeader.lastExtentId), sizeof(extent_id_t));
        tableFullHeader.tableHeader.columnsNullBitMap->WriteDataToFile(filePtr);

        for(const auto& columnMetaData: tableFullHeader.columnsHeaders)
        {
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnNameSize), sizeof(metadata_literal_t));
            filePtr->write(columnMetaData.columnName.c_str(), columnMetaData.columnNameSize);
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnTypeLiteralSize), sizeof(metadata_literal_t));
            filePtr->write(columnMetaData.columnTypeLiteral.c_str(), columnMetaData.columnTypeLiteralSize);
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.recordSize), sizeof(row_size_t));
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnType), sizeof(ColumnType));
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnIndex), sizeof(column_index_t));
        }
    }
}

void HeaderPage::GetPageDataFromFile(const vector<char> &data, const Table* table, page_offset_t& offSet, fstream* filePtr)
{
    offSet = 0;
    memcpy(&this->databaseHeader.databaseNameSize, data.data(), sizeof(metadata_literal_t));
    offSet += sizeof(metadata_literal_t);

    this->databaseHeader.databaseName.resize(this->databaseHeader.databaseNameSize);
    memcpy(&this->databaseHeader.databaseName[0], data.data() + offSet, this->databaseHeader.databaseNameSize);
    offSet += this->databaseHeader.databaseNameSize;

    memcpy(&this->databaseHeader.lastExtentId, data.data() + offSet, sizeof(extent_id_t));
    offSet += sizeof(extent_id_t);

    memcpy(&this->databaseHeader.lastLargeExtentId, data.data() + offSet, sizeof(extent_id_t));
    offSet += sizeof(extent_id_t);

    memcpy(&this->databaseHeader.numberOfTables, data.data() + offSet, sizeof(table_number_t));
    offSet += sizeof(table_number_t);

    memcpy(&this->databaseHeader.lastLargePageId, data.data() + offSet, sizeof(page_id_t));
    offSet += sizeof(page_id_t);

    memcpy(&this->databaseHeader.lastTableId, data.data() + offSet, sizeof(table_id_t));
    offSet += sizeof(table_id_t);

    for(int i = 0;i < this->databaseHeader.numberOfTables; i++)
    {
        TableFullHeader tableFullHeader;
        
        memcpy(&tableFullHeader.tableHeader.tableId, data.data() + offSet, sizeof(table_id_t));
        offSet += sizeof(table_id_t);
        
        memcpy(&tableFullHeader.tableHeader.tableNameSize, data.data() + offSet, sizeof(metadata_literal_t));
        offSet += sizeof(metadata_literal_t);
        tableFullHeader.tableHeader.tableName.resize(tableFullHeader.tableHeader.tableNameSize);

        memcpy(&tableFullHeader.tableHeader.tableName[0], data.data() + offSet, tableFullHeader.tableHeader.tableNameSize);
        offSet += tableFullHeader.tableHeader.tableNameSize;

        memcpy(&tableFullHeader.tableHeader.firstPageId, data.data() + offSet, sizeof(page_id_t));
        offSet += sizeof(page_id_t);

        memcpy(&tableFullHeader.tableHeader.lastPageId, data.data() + offSet, sizeof(page_id_t));
        offSet += sizeof(page_id_t);

        memcpy(&tableFullHeader.tableHeader.maxRowSize, data.data() + offSet, sizeof(row_size_t));
        offSet += sizeof(row_size_t);

        memcpy(&tableFullHeader.tableHeader.numberOfColumns, data.data() + offSet, sizeof(column_number_t));
        offSet += sizeof(column_number_t);

        memcpy(&tableFullHeader.tableHeader.lastExtentId, data.data() + offSet, sizeof(extent_id_t));
        offSet += sizeof(extent_id_t);

        tableFullHeader.tableHeader.columnsNullBitMap = new BitMap(tableFullHeader.tableHeader.numberOfColumns);
        tableFullHeader.tableHeader.columnsNullBitMap->GetDataFromFile(data, offSet);
        
        for(int j = 0; j < tableFullHeader.tableHeader.numberOfColumns; j++)
        {
            ColumnHeader columnHeader;
            memcpy(&columnHeader.columnNameSize, data.data() + offSet, sizeof(metadata_literal_t));
            offSet += sizeof(metadata_literal_t);
            columnHeader.columnName.resize(columnHeader.columnNameSize);

            memcpy(&columnHeader.columnName[0], data.data() + offSet, columnHeader.columnNameSize);
            offSet += columnHeader.columnNameSize;

            memcpy(&columnHeader.columnTypeLiteralSize, data.data() + offSet, sizeof(metadata_literal_t));
            offSet += sizeof(metadata_literal_t);
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

void HeaderPage::SetHeaders(const DatabaseHeader& databaseHeader, const vector<Table*>& tables)
{
    this->databaseHeader = databaseHeader;
    this->databaseHeader.databaseNameSize = this->databaseHeader.databaseName.size();

    for(const auto& table: tables)
    {
        TableFullHeader tableFullHeader;
        tableFullHeader.tableHeader = table->GetTableHeader();
        tableFullHeader.tableHeader.tableNameSize = tableFullHeader.tableHeader.tableName.size();

        const auto& columns = table->GetColumns();

        for(const auto& column: columns)
        {
            ColumnHeader columnHeader = column->GetColumnHeader();
            columnHeader.columnTypeLiteralSize = columnHeader.columnTypeLiteral.size();
            columnHeader.columnNameSize = columnHeader.columnName.size();

            tableFullHeader.columnsHeaders.push_back(columnHeader);
        }

        tablesHeaders.push_back(tableFullHeader);
    }

    this->isDirty = true;
}

const DatabaseHeader& HeaderPage::GetDatabaseHeader() const { return this->databaseHeader; }

const vector<TableFullHeader>& HeaderPage::GetTablesFullHeaders() const { return this->tablesHeaders; }