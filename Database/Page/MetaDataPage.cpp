#include "MetaDataPage.h"

MetaDataPage::MetaDataPage(const int &pageId): Page(pageId)
{
    this->isDirty = false;
    this->metadata.pageType = PageType::METADATA;
}

MetaDataPage::MetaDataPage() : Page()
{
    this->isDirty = false;
    this->metadata.pageType = PageType::METADATA;
}

MetaDataPage::MetaDataPage(const PageMetaData& pageMetaData) : Page(pageMetaData) { }

MetaDataPage::~MetaDataPage() = default;

void MetaDataPage::WritePageToFile(fstream *filePtr)
{
    filePtr->write(reinterpret_cast<const char*>(&this->databaseMetaData.databaseNameSize), sizeof(metadata_literal_t));
    filePtr->write(this->databaseMetaData.databaseName.c_str(), this->databaseMetaData.databaseNameSize);
    filePtr->write(reinterpret_cast<const char*>(&this->databaseMetaData.lastPageId), sizeof(page_id_t));
    filePtr->write(reinterpret_cast<const char*>(&this->databaseMetaData.numberOfTables), sizeof(table_number_t));
    filePtr->write(reinterpret_cast<const char*>(&this->databaseMetaData.lastLargePageId), sizeof(page_id_t));

    for(const auto& tableMetaData: this->tablesMetaData)
    {
        filePtr->write(reinterpret_cast<const char *>(&tableMetaData.tableMetaData.tableNameSize), sizeof(metadata_literal_t));
        filePtr->write(tableMetaData.tableMetaData.tableName.c_str(), tableMetaData.tableMetaData.tableNameSize);
        filePtr->write(reinterpret_cast<const char *>(&tableMetaData.tableMetaData.firstPageId), sizeof(page_id_t));
        filePtr->write(reinterpret_cast<const char *>(&tableMetaData.tableMetaData.lastPageId), sizeof(page_id_t));
        filePtr->write(reinterpret_cast<const char *>(&tableMetaData.tableMetaData.maxRowSize), sizeof(row_size_t));
        filePtr->write(reinterpret_cast<const char *>(&tableMetaData.tableMetaData.numberOfColumns), sizeof(column_number_t));

        for(const auto& columnMetaData: tableMetaData.columnsMetaData)
        {
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnNameSize), sizeof(metadata_literal_t));
            filePtr->write(columnMetaData.columnName.c_str(), columnMetaData.columnNameSize);
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnTypeLiteralSize), sizeof(metadata_literal_t));
            filePtr->write(columnMetaData.columnTypeLiteral.c_str(), columnMetaData.columnTypeLiteralSize);
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.recordSize), sizeof(row_size_t));
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnType), sizeof(ColumnType));
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnIndex), sizeof(column_index_t));
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.allowNulls), sizeof(bool));
        }
    }
}

void MetaDataPage::GetPageDataFromFile(const vector<char> &data, const Table* table, page_offset_t& offSet, fstream* filePtr)
{
    offSet = 0;
    memcpy(&this->databaseMetaData.databaseNameSize, data.data(), sizeof(metadata_literal_t));
    offSet += sizeof(metadata_literal_t);

    this->databaseMetaData.databaseName.resize(this->databaseMetaData.databaseNameSize);
    memcpy(&this->databaseMetaData.databaseName[0], data.data() + offSet, this->databaseMetaData.databaseNameSize);
    offSet += this->databaseMetaData.databaseNameSize;

    memcpy(&this->databaseMetaData.lastPageId, data.data() + offSet, sizeof(page_id_t));
    offSet += sizeof(page_id_t);

    memcpy(&this->databaseMetaData.numberOfTables, data.data() + offSet, sizeof(table_number_t));
    offSet += sizeof(table_number_t);

    memcpy(&this->databaseMetaData.lastLargePageId, data.data() + offSet, sizeof(page_id_t));
    offSet += sizeof(page_id_t);


    for(int i = 0;i < this->databaseMetaData.numberOfTables; i++)
    {
        TableFullMetaData tableFullMetaData;

        memcpy(&tableFullMetaData.tableMetaData.tableNameSize, data.data() + offSet, sizeof(metadata_literal_t));
        offSet += sizeof(metadata_literal_t);
        tableFullMetaData.tableMetaData.tableName.resize(tableFullMetaData.tableMetaData.tableNameSize);

        memcpy(&tableFullMetaData.tableMetaData.tableName[0], data.data() + offSet, tableFullMetaData.tableMetaData.tableNameSize);
        offSet += tableFullMetaData.tableMetaData.tableNameSize;

        memcpy(&tableFullMetaData.tableMetaData.firstPageId, data.data() + offSet, sizeof(page_id_t));
        offSet += sizeof(page_id_t);

        memcpy(&tableFullMetaData.tableMetaData.lastPageId, data.data() + offSet, sizeof(page_id_t));
        offSet += sizeof(page_id_t);

        memcpy(&tableFullMetaData.tableMetaData.maxRowSize, data.data() + offSet, sizeof(row_size_t));
        offSet += sizeof(row_size_t);

        memcpy(&tableFullMetaData.tableMetaData.numberOfColumns, data.data() + offSet, sizeof(column_number_t));
        offSet += sizeof(column_number_t);

        for(int j = 0; j < tableFullMetaData.tableMetaData.numberOfColumns; j++)
        {
            ColumnMetadata columnMetaData;
            memcpy(&columnMetaData.columnNameSize, data.data() + offSet, sizeof(metadata_literal_t));
            offSet += sizeof(metadata_literal_t);
            columnMetaData.columnName.resize(columnMetaData.columnNameSize);

            memcpy(&columnMetaData.columnName[0], data.data() + offSet, columnMetaData.columnNameSize);
            offSet += columnMetaData.columnNameSize;

            memcpy(&columnMetaData.columnTypeLiteralSize, data.data() + offSet, sizeof(metadata_literal_t));
            offSet += sizeof(metadata_literal_t);
            columnMetaData.columnTypeLiteral.resize(columnMetaData.columnTypeLiteralSize);

            memcpy(&columnMetaData.columnTypeLiteral[0], data.data() + offSet, columnMetaData.columnTypeLiteralSize);
            offSet += columnMetaData.columnTypeLiteralSize;

            memcpy(&columnMetaData.recordSize, data.data() + offSet, sizeof(row_size_t));
            offSet += sizeof(row_size_t);

            memcpy(&columnMetaData.columnType, data.data() + offSet, sizeof(ColumnType));
            offSet += sizeof(ColumnType);

            memcpy(&columnMetaData.columnIndex, data.data() + offSet, sizeof(column_index_t));
            offSet += sizeof(column_index_t);

            memcpy(&columnMetaData.allowNulls, data.data() + offSet, sizeof(bool));
            offSet += sizeof(bool);

            tableFullMetaData.columnsMetaData.push_back(columnMetaData);
        }
        this->tablesMetaData.push_back(tableFullMetaData);
    }
}

void MetaDataPage::SetMetaData(const DatabaseMetaData& databaseMetaData, const vector<Table*>& tables)
{
    this->databaseMetaData = databaseMetaData;
    this->databaseMetaData.databaseNameSize = this->databaseMetaData.databaseName.size();

    for(const auto& table: tables)
    {
        TableFullMetaData tableFullMetaData;
        tableFullMetaData.tableMetaData = table->GetTableMetadata();
        tableFullMetaData.tableMetaData.tableNameSize = tableFullMetaData.tableMetaData.tableName.size();

        const auto& columns = table->GetColumns();

        for(const auto& column: columns)
        {
            ColumnMetadata columnMetadata = column->GetColumnMetadata();
            columnMetadata.columnTypeLiteralSize = columnMetadata.columnTypeLiteral.size();
            columnMetadata.columnNameSize = columnMetadata.columnName.size();

            tableFullMetaData.columnsMetaData.push_back(columnMetadata);
        }

        tablesMetaData.push_back(tableFullMetaData);
    }

    this->isDirty = true;
}

const DatabaseMetaData& MetaDataPage::GetDatabaseMetaData() const { return this->databaseMetaData; }

const vector<TableFullMetaData>& MetaDataPage::GetTableFullMetaData() const { return this->tablesMetaData; }