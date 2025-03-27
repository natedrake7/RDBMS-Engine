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
    void Database::JoinTables(Table* firstTable, Table* secondTable, const vector<Field>& conditions)
    {
        //basic nested loop join
        vector<Row> selectedRows;

        //join conditions should have 2 columnIndices for each field to indicate which columns to match
        // firstTable->Select(selectedRows);

        for(const auto& row: selectedRows)
        {
            //create condition here to join
            // secondTable->Select(selectedRows);
        }
    }

    /*Should be called only when where clause of the select contains an index and can be executed faster this way*/
    void Database::JoinTables(vector<Row>& firstTableRows, Table* secondTable, const vector<column_index_t>& selectedColumnIndices, const vector<JoinField>& conditions)
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

            secondTable->Select(selectedRows, selectedColumnIndices, &joinConditions);

            for(const auto& selectedRow: selectedRows)
                for(const auto& block: selectedRow.GetData())
                    row.InsertNewColumn(new Block(block));
        }
    }

}