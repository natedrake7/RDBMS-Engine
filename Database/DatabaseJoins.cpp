#include "Database.h"
#include "Table/Table.h"
#include "Row/Row.h"
#include "Block/Block.h"
#include <vector>

using namespace DatabaseEngine::StorageTypes;

namespace DatabaseEngine 
{
    void Database::JoinTables(Table* firstTable, Table* secondTable, const vector<Field>& conditions)
    {
        //basic nested loop join
        vector<Row> selectedRows;

        //join conditions should have 2 columnIndices for each field to indicate which columns to match
        firstTable->Select(selectedRows);

        for(const auto& row: selectedRows)
        {
            //create condition here to join
            secondTable->Select(selectedRows);
        }
    }

    void Database::JoinTables(vector<Row*>& firstTableRows, Table* secondTable, const vector<Field>& conditions)
    {
        //basic nested loop join
        vector<Row> selectedRows;

        vector<Row> finalRows;

        //join conditions should have 2 columnIndices for each field to indicate which columns to match
        for(const auto& row: firstTableRows)
        {
            //create condition here to join
            secondTable->Select(selectedRows, &conditions);

            //join the rows
            Row newRow(*row);

            for(const auto& selectedRow: selectedRows)
            {
                const auto& rowData = selectedRow.GetData();
                for(const auto& block: rowData)
                    newRow.InsertNewColumn(new Block(*block));
            }

            finalRows.push_back(newRow);
        }
    }

}