#include "MetaDataPage.h"

MetaDataPage::MetaDataPage(const int &pageId): Page(pageId)
{
    this->isDirty = false;
}

MetaDataPage::~MetaDataPage() = default;

void MetaDataPage::WritePageToFile(fstream *filePtr)
{
    filePtr->write(reinterpret_cast<const char*>(&this->databaseMetaData.databaseNameSize), sizeof(int));
    filePtr->write(this->databaseMetaData.databaseName.c_str(), this->databaseMetaData.databaseNameSize);
    filePtr->write(reinterpret_cast<const char*>(&this->databaseMetaData.lastPageId), sizeof(uint16_t));
    filePtr->write(reinterpret_cast<const char*>(&this->databaseMetaData.numberOfTables), sizeof(int));

    for(const auto& tableMetaData: this->tablesMetaData)
    {
        filePtr->write(reinterpret_cast<const char *>(&tableMetaData.tableMetaData.tableNameSize), sizeof(int));
        filePtr->write(tableMetaData.tableMetaData.tableName.c_str(), tableMetaData.tableMetaData.tableNameSize);
        filePtr->write(reinterpret_cast<const char *>(&tableMetaData.tableMetaData.firstPageId), sizeof(uint16_t));
        filePtr->write(reinterpret_cast<const char *>(&tableMetaData.tableMetaData.lastPageId), sizeof(uint16_t));
        filePtr->write(reinterpret_cast<const char *>(&tableMetaData.tableMetaData.maxRowSize), sizeof(size_t));
        filePtr->write(reinterpret_cast<const char *>(&tableMetaData.tableMetaData.numberOfColumns), sizeof(uint16_t));

        for(const auto& columnMetaData: tableMetaData.columnsMetaData)
        {
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnNameSize), sizeof(int));
            filePtr->write(columnMetaData.columnName.c_str(), columnMetaData.columnNameSize);
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnTypeLiteralSize), sizeof(int));
            filePtr->write(columnMetaData.columnTypeLiteral.c_str(), columnMetaData.columnTypeLiteralSize);
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.recordSize), sizeof(uint32_t));
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnType), sizeof(ColumnType));
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnIndex), sizeof(uint16_t));
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.allowNulls), sizeof(bool));
        }
    }
}

void MetaDataPage::GetPageDataFromFile(const vector<char> &data, const Table* table, uint32_t& offSet)
{
    memcpy(&this->databaseMetaData.databaseNameSize, data.data(), sizeof(int));
    offSet += sizeof(int);

    this->databaseMetaData.databaseName.resize(this->databaseMetaData.databaseNameSize);
    memcpy(&this->databaseMetaData.databaseName[0], data.data() + offSet, this->databaseMetaData.databaseNameSize);
    offSet += this->databaseMetaData.databaseNameSize;

    memcpy(&this->databaseMetaData.lastPageId, data.data() + offSet, sizeof(uint16_t));
    offSet += sizeof(uint16_t);

    memcpy(&this->databaseMetaData.numberOfTables, data.data() + offSet, sizeof(int));
    offSet += sizeof(int);

    for(int i = 0;i < this->databaseMetaData.numberOfTables; i++)
    {
        TableFullMetaData tableFullMetaData;

        memcpy(&tableFullMetaData.tableMetaData.tableNameSize, data.data() + offSet, sizeof(int));
        offSet += sizeof(int);
        tableFullMetaData.tableMetaData.tableName.resize(tableFullMetaData.tableMetaData.tableNameSize);

        memcpy(&tableFullMetaData.tableMetaData.tableName[0], data.data() + offSet, tableFullMetaData.tableMetaData.tableNameSize);
        offSet += tableFullMetaData.tableMetaData.tableNameSize;

        memcpy(&tableFullMetaData.tableMetaData.firstPageId, data.data() + offSet, sizeof(uint16_t));
        offSet += sizeof(uint16_t);

        memcpy(&tableFullMetaData.tableMetaData.lastPageId, data.data() + offSet, sizeof(uint16_t));
        offSet += sizeof(uint16_t);

        memcpy(&tableFullMetaData.tableMetaData.maxRowSize, data.data() + offSet, sizeof(size_t));
        offSet += sizeof(size_t);

        memcpy(&tableFullMetaData.tableMetaData.numberOfColumns, data.data() + offSet, sizeof(uint16_t));
        offSet += sizeof(uint16_t);

        for(int j = 0; j < tableFullMetaData.tableMetaData.numberOfColumns; j++)
        {
            ColumnMetadata columnMetaData;
            memcpy(&columnMetaData.columnNameSize, data.data() + offSet, sizeof(int));
            offSet += sizeof(int);
            columnMetaData.columnName.resize(columnMetaData.columnNameSize);

            memcpy(&columnMetaData.columnName[0], data.data() + offSet, columnMetaData.columnNameSize);
            offSet += columnMetaData.columnNameSize;

            memcpy(&columnMetaData.columnTypeLiteralSize, data.data() + offSet, sizeof(int));
            offSet += sizeof(int);
            columnMetaData.columnTypeLiteral.resize(columnMetaData.columnTypeLiteralSize);

            memcpy(&columnMetaData.columnTypeLiteral[0], data.data() + offSet, columnMetaData.columnTypeLiteralSize);
            offSet += columnMetaData.columnTypeLiteralSize;

            memcpy(&columnMetaData.recordSize, data.data() + offSet, sizeof(uint32_t));
            offSet += sizeof(uint32_t);

            memcpy(&columnMetaData.columnType, data.data() + offSet, sizeof(ColumnType));
            offSet += sizeof(ColumnType);

            memcpy(&columnMetaData.columnIndex, data.data() + offSet, sizeof(uint16_t));
            offSet += sizeof(uint16_t);

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