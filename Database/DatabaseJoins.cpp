#include "Constants.h"
#include "Database.h"
#include "Table/Table.h"
#include "Row/Row.h"
#include "Block/Block.h"
#include <vector>

using namespace DatabaseEngine::StorageTypes;

namespace DatabaseEngine 
{
    /*Should be called when the where clause is not executed prior to the join*/
    void Database::JoinTables(
        vector<Row>& selectedRows, 
        Table* firstTable, 
        Table* secondTable, 
        const vector<column_index_t>& secondTableSelectedColumnIndices, 
        const vector<JoinField>& conditions
    )
    {
        //basic nested loop join
        vector<column_index_t> columnIndices;

        for(const auto& column: firstTable->GetColumns())
            columnIndices.push_back(column->GetColumnIndex());

        //join conditions should have 2 columnIndices for each field to indicate which columns to match
//        firstTable->Select(selectedRows, columnIndices);

        Database::JoinTables(selectedRows, secondTable, secondTableSelectedColumnIndices, conditions);
    }

    /*Should be called only when where clause of the select contains an index and can be executed faster this way*/
    void Database::JoinTables(
        vector<Row>& firstTableRows, 
        Table* secondTable, 
        const vector<column_index_t>& selectedColumnIndices, 
        const vector<JoinField>& conditions
    )
    {
        //basic nested loop join
        vector<Row> selectedRows;

        const auto& secondTableColumns = secondTable->GetColumns();

        //join conditions should have 2 columnIndices for each field to indicate which columns to match
        for(auto& row: firstTableRows)
        {
            //create condition here to join
            vector<Block> joinConditions;

            for(const auto& condition: conditions)
            {
                const auto& firstCondition = condition.GetFirstTableCondition();
                const auto& secondCondition = condition.GetSecondTableCondition();

                const auto& block = row.GetData()[firstCondition.GetColumnIndex()];

                joinConditions.emplace_back(block);
                joinConditions.back().SetColumn(secondTableColumns[secondCondition.GetColumnIndex()]);
            }

            selectedRows.clear();
            secondTable->SelectForJoin(selectedRows, selectedColumnIndices, &joinConditions);


            //if inner join , just discard the row if no match is found

            Database::MergeRows(row, selectedRows, selectedColumnIndices, secondTable);
        }
    }

    void Database::MergeRows(Row& row, const vector<Row>& selectedRows, const vector<column_index_t>& selectedColumnIndices, const Table* secondTable)
    {
        if(selectedRows.empty())
        {
            const auto& columns = secondTable->GetColumns();
            for(const auto& columnIndex: selectedColumnIndices)
                row.InsertNewColumn(new Block(nullptr, 0, columns[columnIndex]));

            return;    
        }

        for(const auto& selectedRow: selectedRows)
            for(const auto& block: selectedRow.GetData())
                row.InsertNewColumn(new Block(block));


    }

    void Database::GetIdentityColumns(){
      for(auto& table: this->tables)
        table->GetIdentityColumns();
    }

    void Database::UpdateMasterDatabase(){
      for(const auto& table: this->tables)
        table->UpdateMasterDatabase();
    }

    string Database::GetSystemFilename() const{ return this->systemFilename;}
}