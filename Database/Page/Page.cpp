#include "Page.h"

PageMetadata::PageMetadata()
{
    this->pageId = 0;
    this->pageSize = 0;
    this->nextPageId = 0;
    this->bytesLeft = PAGE_SIZE - 4 * sizeof(uint16_t);

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

void Page::InsertRow(Row* row, const Table& table)
{
    this->rows.push_back(row);
    this->metadata.bytesLeft -= (row->GetRowSize() + table.GetNumberOfColumns() * sizeof(uint32_t));
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

void Page::GetPageDataFromFile(const vector<char>& data, const Table* table, uint32_t& offSet)
{
    memcpy(&this->metadata.pageId, data.data(), sizeof(uint16_t));
    offSet += sizeof(uint16_t);

    memcpy(&this->metadata.nextPageId, data.data() + offSet, sizeof(uint16_t));
    offSet += sizeof(uint16_t);

    memcpy(&this->metadata.pageSize, data.data() + offSet, sizeof(uint16_t));
    offSet += sizeof(uint16_t);

    memcpy(&this->metadata.bytesLeft, data.data() + offSet, sizeof(uint16_t));
    offSet += sizeof(uint16_t);

    const auto& columns = table->GetColumns();
    const int columnsSize = columns.size();

    for (int i = 0; i < this->metadata.pageSize; i++)
    {
         Row* row = new Row(*table);
         for(int j = 0; j < columnsSize; j++)
         {
             uint32_t bytesToRead = 0;
             memcpy(&bytesToRead, data.data() + offSet, sizeof(uint32_t));

             unsigned char* bytes = new unsigned char[bytesToRead];

             offSet += sizeof(uint32_t);

             memcpy(bytes, data.data() + offSet, bytesToRead);
             offSet += bytesToRead;

             Block* block = new Block(bytes, bytesToRead, columns[j]);

             row->InsertColumnData(block, j);

             delete[] bytes;
         }
         this->rows.push_back(row);
    }
}

void Page::WritePageToFile(fstream *filePtr)
{
    filePtr->write(reinterpret_cast<char*>(&this->metadata.pageId), sizeof(uint16_t));
    filePtr->write(reinterpret_cast<char*>(&this->metadata.nextPageId), sizeof(uint16_t));
    filePtr->write(reinterpret_cast<char*>(&this->metadata.pageSize), sizeof(uint16_t));
    filePtr->write(reinterpret_cast<char*>(&this->metadata.bytesLeft), sizeof(uint16_t));

    for(const auto& row: this->rows)
    {
        for(const auto& block : row->GetData())
        {
            const auto& dataSize = block->GetBlockSize();
            filePtr->write(reinterpret_cast<const char *>(&dataSize), sizeof(uint32_t));

            const auto& data = block->GetBlockData();
            filePtr->write(reinterpret_cast<const char *>(data), dataSize);
        }
    }
}

void Page::SetNextPageId(const int &nextPageId) { this->metadata.nextPageId = nextPageId; }

void Page::SetFileName(const string &filename) { this->filename = filename; }

const string& Page::GetFileName() const { return this->filename; }

const uint16_t& Page::GetPageId() const { return this->metadata.pageId; }

const bool& Page::GetPageDirtyStatus() const { return this->isDirty; }

const uint16_t& Page::GetBytesLeft() const { return this->metadata.bytesLeft; }

const uint16_t& Page::GetNextPageId() const { return this->metadata.nextPageId; }

vector<Row> Page::GetRows(const Table& table) const
{
    vector<Row> copiedRows;
    for(const auto& row: this->rows)
    {
        vector<Block*> copyBlocks;
        for(const auto& block : row->GetData())
        {
            Block* blockCopy = new Block(block);
            copyBlocks.push_back(blockCopy);
        }
        copiedRows.emplace_back(table, copyBlocks);
    }

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
