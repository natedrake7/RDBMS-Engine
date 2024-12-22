#include "Table.h"
#include "../Constants.h"
#include "../../AdditionalLibraries/AdditionalObjects/RowCondition/RowCondition.h"
#include "../../AdditionalLibraries/AdditionalObjects/Field/Field.h"
#include "../../AdditionalLibraries/BitMap/BitMap.h"
#include "../../AdditionalLibraries/AdditionalObjects/DateTime/DateTime.h"
#include "../../AdditionalLibraries/AdditionalObjects/Decimal/Decimal.h"
#include "../Pages/Page.h"
#include "../Column/Column.h"
#include "../Row/Row.h"
#include "../Database.h"
#include "../../AdditionalLibraries/SafeConverter/SafeConverter.h"
#include "../Pages/LargeObject/LargeDataPage.h"
#include "../Block/Block.h"

using namespace Pages;
using namespace DataTypes;
using namespace ByteMaps;

namespace DatabaseEngine::StorageTypes
{
    TableHeader::TableHeader()
    {
        this->indexAllocationMapPageId = 0;
        this->tableId = 0;
        this->maxRowSize = 0;
        this->numberOfColumns = 0;
        this->tableNameSize = 0;
        this->clusteredIndexPageId = 0;
        this->columnsNullBitMap = nullptr;
    }

    TableHeader::~TableHeader()
    {
        delete this->columnsNullBitMap;
        delete this->clusteredIndexesBitMap;
        delete this->nonClusteredIndexesBitMap;
    }

    TableHeader &TableHeader::operator=(const TableHeader &tableHeader)
    {
        if (this == &tableHeader)
            return *this;

        this->indexAllocationMapPageId = tableHeader.indexAllocationMapPageId;
        this->maxRowSize = tableHeader.maxRowSize;
        this->numberOfColumns = tableHeader.numberOfColumns;
        this->tableNameSize = tableHeader.tableNameSize;
        this->tableName = tableHeader.tableName;
        this->tableId = tableHeader.tableId;
        this->clusteredIndexPageId = tableHeader.clusteredIndexPageId;

        this->columnsNullBitMap = new BitMap(*tableHeader.columnsNullBitMap);
        this->clusteredIndexesBitMap = new BitMap(*tableHeader.clusteredIndexesBitMap);
        this->nonClusteredIndexesBitMap = new BitMap(*tableHeader.nonClusteredIndexesBitMap);

        return *this;
    }

    TableFullHeader::TableFullHeader() = default;

    TableFullHeader::TableFullHeader(const TableFullHeader &tableHeader)
    {
        this->tableHeader = tableHeader.tableHeader;
        this->columnsHeaders = tableHeader.columnsHeaders;
    }

    Table::Table(const string &tableName, const table_id_t &tableId, const vector<Column *> &columns, DatabaseEngine::Database *database, const vector<column_index_t> *clusteredKeyIndexes, const vector<column_index_t> *nonClusteredIndexes)
    {
        this->columns = columns;
        this->database = database;
        this->header.tableName = tableName;
        this->header.numberOfColumns = columns.size();
        this->header.columnsNullBitMap = new BitMap(this->header.numberOfColumns);
        this->header.clusteredIndexesBitMap = new BitMap(this->header.numberOfColumns);
        this->header.nonClusteredIndexesBitMap = new BitMap(this->header.numberOfColumns);
        this->header.tableId = tableId;

        this->SetTableIndexesToHeader(clusteredKeyIndexes, nonClusteredIndexes);

        uint16_t counter = 0;
        for (const auto &column : columns)
        {
            this->header.columnsNullBitMap->Set(counter, column->GetAllowNulls());

            this->header.maxRowSize += column->GetColumnSize();
            column->SetColumnIndex(counter);

            counter++;
        }
    }

    Table::Table(const TableHeader &tableHeader, DatabaseEngine::Database *database)
    {
        this->header = tableHeader;
        this->database = database;
    }

    Table::~Table()
    {
        for (const auto &column : columns)
            delete column;
    }

    void Table::SetTableIndexesToHeader(const vector<column_index_t> *clusteredKeyIndexes, const vector<column_index_t> *nonClusteredIndexes)
    {
        if (clusteredKeyIndexes != nullptr && !clusteredKeyIndexes->empty())
            for (const auto &clusteredIndex : *clusteredKeyIndexes)
                this->header.clusteredIndexesBitMap->Set(clusteredIndex, true);

        if (nonClusteredIndexes != nullptr && !nonClusteredIndexes->empty())
            for (const auto &nonClusteredIndex : *nonClusteredIndexes)
                this->header.nonClusteredIndexesBitMap->Set(nonClusteredIndex, true);
    }

    void Table::InsertRows(const vector<vector<Field>> &inputData)
    {
        uint32_t rowsInserted = 0;
        extent_id_t startingExtentIndex = 0;
        vector<extent_id_t> extents;
        for (const auto &rowData : inputData)
        {
            this->InsertRow(rowData, extents, startingExtentIndex);

            rowsInserted++;

            if (rowsInserted % 1000 == 0)
                cout << rowsInserted << endl;
        }

        cout << "Rows affected: " << rowsInserted << endl;
    }

    void Table::InsertRow(const vector<Field> &inputData, vector<extent_id_t> &allocatedExtents, extent_id_t &startingExtentIndex)
    {

        Row *row = new Row(*this);
        for (size_t i = 0; i < inputData.size(); ++i)
        {
            const column_index_t &associatedColumnIndex = inputData[i].GetColumnIndex();
            Block *block = new Block(columns[associatedColumnIndex]);
            const ColumnType columnType = columns[associatedColumnIndex]->GetColumnType();

            if (inputData[i].GetIsNull())
            {
                this->CheckAndInsertNullValues(block, row, associatedColumnIndex);
                continue;
            }

            if (columnType > ColumnType::ColumnTypeCount)
                throw invalid_argument("Table::InsertRow: Unsupported Column Type");

            this->setBlockDataByDataTypeArray[static_cast<int>(columnType)](block, inputData[i]);

            const auto &columnIndex = columns[associatedColumnIndex]->GetColumnIndex();
            row->InsertColumnData(block, columnIndex);
        }

        this->InsertLargeObjectToPage(row);
        this->database->InsertRowToPage(*this, allocatedExtents, startingExtentIndex, row);
    }

    void Table::InsertLargeObjectToPage(Row *row)
    {
        const vector<column_index_t> largeBlockIndexes = row->GetLargeBlocks();

        if (largeBlockIndexes.empty())
            return;

        RowHeader *rowHeader = row->GetHeader();

        const auto &rowData = row->GetData();

        for (const auto &largeBlockIndex : largeBlockIndexes)
        {
            rowHeader->largeObjectBitMap->Set(largeBlockIndex, true);

            page_offset_t offset = 0;

            block_size_t remainingBlockSize = rowData[largeBlockIndex]->GetBlockSize();

            RecursiveInsertToLargePage(row, offset, largeBlockIndex, remainingBlockSize, true, nullptr);
        }
    }

    void Table::RecursiveInsertToLargePage(Row *&row, page_offset_t &offset, const column_index_t &columnIndex, block_size_t &remainingBlockSize, const bool &isFirstRecursion, DataObject **previousDataObject)
    {
        LargeDataPage *largeDataPage = this->GetOrCreateLargeDataPage();

        const auto &pageSize = largeDataPage->GetBytesLeft();

        const auto &data = row->GetData()[columnIndex]->GetBlockData();

        large_page_index_t objectIndex;

        if (remainingBlockSize + OBJECT_METADATA_SIZE_T < pageSize)
        {

            largeDataPage->InsertObject(data + offset, remainingBlockSize, &objectIndex);

            this->database->SetPageMetaDataToPfs(largeDataPage);

            Table::InsertLargeDataObjectPointerToRow(row, isFirstRecursion, objectIndex, largeDataPage->GetPageId(), columnIndex);

            if (previousDataObject != nullptr)
            {
                (*previousDataObject)->nextPageId = largeDataPage->GetPageId();
                (*previousDataObject)->nextObjectIndex = objectIndex;
            }

            return;
        }

        // blockSize < pageSize
        const auto bytesToBeInserted = pageSize - OBJECT_METADATA_SIZE_T;

        remainingBlockSize -= bytesToBeInserted;

        DataObject *dataObject = largeDataPage->InsertObject(data + offset, bytesToBeInserted, &objectIndex);

        this->database->SetPageMetaDataToPfs(largeDataPage);

        if (previousDataObject != nullptr)
        {
            (*previousDataObject)->nextPageId = largeDataPage->GetPageId();
            (*previousDataObject)->nextObjectIndex = objectIndex;
        }

        offset += bytesToBeInserted;

        this->RecursiveInsertToLargePage(row, offset, columnIndex, remainingBlockSize, false, &dataObject);

        Table::InsertLargeDataObjectPointerToRow(row, isFirstRecursion, objectIndex, largeDataPage->GetPageId(), columnIndex);
    }

    LargeDataPage *Table::GetOrCreateLargeDataPage() const
    {
        LargeDataPage *largeDataPage = this->database->GetTableLastLargeDataPage(this->header.tableId, OBJECT_METADATA_SIZE_T + 1);

        return (largeDataPage == nullptr)
                   ? this->database->CreateLargeDataPage(this->header.tableId)
                   : largeDataPage;
    }

    void Table::LinkLargePageDataObjectChunks(DataObject *dataObject, const page_id_t &lastLargePageId, const large_page_index_t &objectIndex)
    {
        if (dataObject != nullptr)
        {
            dataObject->nextPageId = lastLargePageId;
            dataObject->nextObjectIndex = objectIndex;
        }
    }

    void Table::InsertLargeDataObjectPointerToRow(Row *row, const bool &isFirstRecursion, const large_page_index_t &objectIndex, const page_id_t &lastLargePageId, const column_index_t &largeBlockIndex) const
    {
        if (!isFirstRecursion)
            return;

        const DataObjectPointer objectPointer(objectIndex, lastLargePageId);

        Block *block = new Block(&objectPointer, sizeof(DataObjectPointer), this->columns[largeBlockIndex]);

        row->UpdateColumnData(block);
    }

    column_number_t Table::GetNumberOfColumns() const { return this->columns.size(); }

    const TableHeader &Table::GetTableHeader() const { return this->header; }

    const vector<Column *> &Table::GetColumns() const { return this->columns; }

    LargeDataPage *Table::GetLargeDataPage(const page_id_t &pageId) const
    {
        return this->database->GetLargeDataPage(pageId, this->header.tableId);
    }

    void Table::Select(vector<Row> &selectedRows, const vector<RowCondition *> *conditions, const size_t &count) const
    {
        const size_t rowsToSelect = (count == -1)
                                        ? numeric_limits<size_t>::max()
                                        : count;

        this->database->SelectTableRows(this->header.tableId, &selectedRows, rowsToSelect, conditions);
    }

    void Table::Update(const vector<Field> &updates, const vector<RowCondition *> *conditions) const
    {
        vector<Block *> updateBlocks;
        for (const auto &field : updates)
        {
            const auto &associatedColumnIndex = field.GetColumnIndex();
            const auto &columnType = this->columns[associatedColumnIndex]->GetColumnType();

            Block *block = new Block(this->columns[associatedColumnIndex]);

            this->setBlockDataByDataTypeArray[static_cast<int>(columnType)](block, field);
            updateBlocks.push_back(block);
        }

        this->database->UpdateTableRows(this->header.tableId, updateBlocks, conditions);

        for (const auto &block : updateBlocks)
            delete block;
    }

    void Table::UpdateIndexAllocationMapPageId(const page_id_t &indexAllocationMapPageId) { this->header.indexAllocationMapPageId = indexAllocationMapPageId; }

    bool Table::IsColumnNullable(const column_index_t &columnIndex) const { return this->header.columnsNullBitMap->Get(columnIndex); }

    void Table::AddColumn(Column *column) { this->columns.push_back(column); }

    string &Table::GetTableName() { return this->header.tableName; }

    row_size_t &Table::GetMaxRowSize() { return this->header.maxRowSize; }

    const table_id_t &Table::GetTableId() const { return this->header.tableId; }

    TableType Table::GetTableType() const { return this->header.clusteredIndexesBitMap->HasAtLeastOneEntry() ? TableType::CLUSTERED : TableType::HEAP; }

    row_size_t Table::GetMaximumRowSize() const
    {
        row_size_t maximumRowSize = 0;
        for (const auto &column : this->columns)
            maximumRowSize += (column->isColumnLOB())
                                  ? sizeof(DataObjectPointer)
                                  : column->GetColumnSize();

        return maximumRowSize;
    }

    void Table::GetIndexedColumnKeys(vector<column_index_t> *vector) const
    {
        for (bit_map_pos_t i = 0; i < this->header.clusteredIndexesBitMap->GetSize(); i++)
            if (this->header.clusteredIndexesBitMap->Get(i))
                vector->push_back(i);
    }

    void Table::SetIndexPageId(const page_id_t &indexPageId) { this->header.clusteredIndexPageId = indexPageId; }

    void Table::SetTinyIntData(Block *&block, const Field &inputData)
    {
        const int8_t convertedTinyInt = SafeConverter<int8_t>::SafeStoi(inputData.GetData());
        block->SetData(&convertedTinyInt, sizeof(int8_t));
    }

    void Table::SetSmallIntData(Block *&block, const Field &inputData)
    {
        const int16_t convertedSmallInt = SafeConverter<int16_t>::SafeStoi(inputData.GetData());
        block->SetData(&convertedSmallInt, sizeof(int16_t));
    }

    void Table::SetIntData(Block *&block, const Field &inputData)
    {
        const int32_t convertedInt = SafeConverter<int32_t>::SafeStoi(inputData.GetData());
        block->SetData(&convertedInt, sizeof(int32_t));
    }

    void Table::SetBigIntData(Block *&block, const Field &inputData)
    {
        const int64_t convertedBigInt = SafeConverter<int64_t>::SafeStoi(inputData.GetData());
        block->SetData(&convertedBigInt, sizeof(int64_t));
    }

    void Table::SetStringData(Block *&block, const Field &inputData)
    {
        const string &data = inputData.GetData();
        block->SetData(data.c_str(), data.size());
    }

    void Table::SetBoolData(Block *&block, const Field &inputData)
    {
        const string &data = inputData.GetData();

        bool value = false;

        if (data == "1")
            value = true;
        else if (data == "0")
            value = false;
        else
            throw invalid_argument("InsertRow: Invalid Boolean Value specified!");

        block->SetData(&value, sizeof(bool));
    }

    void Table::SetDateTimeData(Block *&block, const Field &inputData)
    {
        const string &data = inputData.GetData();

        const time_t unixMilliseconds = DateTime::ToUnixTimeStamp(data);

        block->SetData(&unixMilliseconds, sizeof(time_t));
    }

    void Table::SetDecimalData(Block *&block, const Field &inputData)
    {
        const string &data = inputData.GetData();

        const Decimal decimalValue(data);

        block->SetData(decimalValue.GetRawData(), decimalValue.GetRawDataSize());
    }

    void Table::CheckAndInsertNullValues(Block *&block, Row *&row, const column_index_t &associatedColumnIndex)
    {
        if (!columns[associatedColumnIndex]->GetAllowNulls())
            throw invalid_argument("Column " + columns[associatedColumnIndex]->GetColumnName() + " does not allow NULLs. Insert Fails.");

        block->SetData(nullptr, 0);

        row->SetNullBitMapValue(associatedColumnIndex, true);

        const auto &columnIndex = columns[associatedColumnIndex]->GetColumnIndex();
        row->InsertColumnData(block, columnIndex);
    }
}