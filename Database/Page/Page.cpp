#include "Page.h"

PageMetadata::PageMetadata()
{
    this->pageId = -1;
    this->pageSize = 0;
    this->bytesLeft = PAGE_SIZE;
    this->nextPageId = -1;
}

PageMetadata::~PageMetadata() = default;

Page::Page(const int& pageId)
{
    this->metadata.pageId = pageId;
    this->isDirty = false;
}

Page::~Page()
{
    for(const auto& row: this->rows)
        delete row;
}

void Page::InsertRow(Row* row)
{
    this->rows.push_back(row);
    this->metadata.bytesLeft -= row->GetRowSize();
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

void Page::GetPageDataFromFile(const vector<char>& data)
{
    memcpy(&this->metadata, data.data(), sizeof(PageMetadata));

    size_t dataOffset = sizeof(PageMetadata);
    int bytesToRead;
    
    for (int i = 0; i < this->metadata.pageSize; i++)
    {
        //figure out how to use db here
        // Row* row = new Row();
        // for(int j = 0; j < this->metadata.numberOfColumns; j++)
        // {
        //     memcpy(&bytesToRead, data.data() + dataOffset, sizeof(int));
        //
        //     // Row* row = new Row();
        //     unsigned char *bytes = new unsigned char[bytesToRead];
        //
        //     dataOffset += sizeof(int);
        //
        //     memcpy(bytes, data.data() + dataOffset, bytesToRead);
        //     dataOffset += bytesToRead;
        //
        //     Block* block = new Block(bytes, bytesToRead, nullptr);
        //
        //     row->InsertColumnData(block, j);
        // }
        // this->rows.push_back(row);
    }
}

void Page::WritePageToFile(fstream *filePtr)
{
    filePtr->write(reinterpret_cast<char*>(&this->metadata), sizeof(PageMetadata));

    for(const auto& row: this->rows)
    {
        for(const auto& block : row->GetData())
        {
            const auto& dataSize = block->GetBlockSize();
            filePtr->write(reinterpret_cast<const char *>(&dataSize), sizeof(size_t));

            const auto& data = block->GetBlockData();
            filePtr->write(reinterpret_cast<const char *>(data), dataSize);
        }
    }
}

void Page::SetNextPageId(const int &nextPageId) { this->metadata.nextPageId = nextPageId; }

void Page::SetFileName(const string &filename) { this->filename = filename; }

const string & Page::GetFileName() const { return this->filename; }

const int& Page::GetPageId() const { return this->metadata.pageId; }

const bool& Page::GetPageDirtyStatus() const { return this->isDirty; }

const size_t & Page::GetBytesLeft() const { return this->metadata.bytesLeft; }

const int & Page::GetNextPageId() const { return this->metadata.nextPageId; }

vector<Row> Page::GetRows() const
{
    vector<Row> copiedRows;
    for(const auto& row: this->rows)
        copiedRows.push_back(*row);

    return copiedRows;
}

MetaDataPage::MetaDataPage(const int &pageId): Page(pageId)
{
    this->isDirty = false;
}

MetaDataPage::~MetaDataPage() = default;

void MetaDataPage::WritePageToFile(fstream *filePtr)
{
    filePtr->write(reinterpret_cast<const char*>(&this->databaseMetaData.databaseNameSize), sizeof(int));
    filePtr->write(this->databaseMetaData.databaseName.c_str(), this->databaseMetaData.databaseNameSize);
    filePtr->write(reinterpret_cast<const char*>(&this->databaseMetaData.lastPageId), sizeof(int));
    filePtr->write(reinterpret_cast<const char*>(&this->databaseMetaData.numberOfTables), sizeof(int));

    for(const auto& tableMetaData: this->tablesMetaData)
    {
        filePtr->write(reinterpret_cast<const char *>(&tableMetaData.tableMetaData.tableNameSize), sizeof(int));
        filePtr->write(tableMetaData.tableMetaData.tableName.c_str(), tableMetaData.tableMetaData.tableNameSize);
        filePtr->write(reinterpret_cast<const char *>(&tableMetaData.tableMetaData.firstPageId), sizeof(int));
        filePtr->write(reinterpret_cast<const char *>(&tableMetaData.tableMetaData.lastPageId), sizeof(int));
        filePtr->write(reinterpret_cast<const char *>(&tableMetaData.tableMetaData.maxRowSize), sizeof(size_t));
        filePtr->write(reinterpret_cast<const char *>(&tableMetaData.tableMetaData.numberOfColumns), sizeof(int));

        for(const auto& columnMetaData: tableMetaData.columnsMetaData)
        {
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnNameSize), sizeof(int));
            filePtr->write(columnMetaData.columnName.c_str(), columnMetaData.columnNameSize);
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnTypeLiteralSize), sizeof(int));
            filePtr->write(columnMetaData.columnTypeLiteral.c_str(), columnMetaData.columnTypeLiteralSize);
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.recordSize), sizeof(size_t));
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnType), sizeof(ColumnType));
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.columnIndex), sizeof(size_t));
            filePtr->write(reinterpret_cast<const char *>(&columnMetaData.allowNulls), sizeof(bool));
        }
    }
}

void MetaDataPage::GetPageDataFromFile(const vector<char> &data)
{
    size_t dataOffset = 0;
    memcpy(&this->databaseMetaData.databaseNameSize, data.data(), sizeof(int));
    dataOffset += sizeof(int);

    this->databaseMetaData.databaseName.resize(this->databaseMetaData.databaseNameSize);
    memcpy(&this->databaseMetaData.databaseName[0], data.data() + dataOffset, this->databaseMetaData.databaseNameSize);
    dataOffset += this->databaseMetaData.databaseNameSize;

    memcpy(&this->databaseMetaData.lastPageId, data.data() + dataOffset, sizeof(int));
    dataOffset += sizeof(int);

    memcpy(&this->databaseMetaData.numberOfTables, data.data() + dataOffset, sizeof(int));
    dataOffset += sizeof(int);

    for(int i = 0;i < this->databaseMetaData.numberOfTables; i++)
    {
        TableFullMetaData tableFullMetaData;

        memcpy(&tableFullMetaData.tableMetaData.tableNameSize, data.data() + dataOffset, sizeof(int));
        dataOffset += sizeof(int);
        tableFullMetaData.tableMetaData.tableName.resize(tableFullMetaData.tableMetaData.tableNameSize);

        memcpy(&tableFullMetaData.tableMetaData.tableName[0], data.data() + dataOffset, tableFullMetaData.tableMetaData.tableNameSize);
        dataOffset += tableFullMetaData.tableMetaData.tableNameSize;

        memcpy(&tableFullMetaData.tableMetaData.firstPageId, data.data() + dataOffset, sizeof(int));
        dataOffset += sizeof(int);

        memcpy(&tableFullMetaData.tableMetaData.lastPageId, data.data() + dataOffset, sizeof(int));
        dataOffset += sizeof(int);

        memcpy(&tableFullMetaData.tableMetaData.maxRowSize, data.data() + dataOffset, sizeof(size_t));
        dataOffset += sizeof(size_t);

        memcpy(&tableFullMetaData.tableMetaData.numberOfColumns, data.data() + dataOffset, sizeof(int));
        dataOffset += sizeof(int);

        for(int j = 0; j < tableFullMetaData.tableMetaData.numberOfColumns; j++)
        {
            ColumnMetadata columnMetaData;
            memcpy(&columnMetaData.columnNameSize, data.data() + dataOffset, sizeof(int));
            dataOffset += sizeof(int);
            columnMetaData.columnName.resize(columnMetaData.columnNameSize);

            memcpy(&columnMetaData.columnName[0], data.data() + dataOffset, columnMetaData.columnNameSize);
            dataOffset += columnMetaData.columnNameSize;

            memcpy(&columnMetaData.columnTypeLiteralSize, data.data() + dataOffset, sizeof(int));
            dataOffset += sizeof(int);
            columnMetaData.columnTypeLiteral.resize(columnMetaData.columnTypeLiteralSize);

            memcpy(&columnMetaData.columnTypeLiteral[0], data.data() + dataOffset, columnMetaData.columnTypeLiteralSize);
            dataOffset += columnMetaData.columnTypeLiteralSize;

            memcpy(&columnMetaData.recordSize, data.data() + dataOffset, sizeof(size_t));
            dataOffset += sizeof(size_t);

            memcpy(&columnMetaData.columnType, data.data() + dataOffset, sizeof(ColumnType));
            dataOffset += sizeof(ColumnType);

            memcpy(&columnMetaData.columnIndex, data.data() + dataOffset, sizeof(size_t));
            dataOffset += sizeof(size_t);

            memcpy(&columnMetaData.allowNulls, data.data() + dataOffset, sizeof(bool));
            dataOffset += sizeof(bool);

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

        for(const auto& column: table->GetColumns())
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
