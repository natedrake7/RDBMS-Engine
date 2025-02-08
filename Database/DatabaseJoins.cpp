#include "Database.h"
#include "Table/Table.h"
#include "Row/Row.h"
#include <vector>

using namespace DatabaseEngine::StorageTypes;

namespace DatabaseEngine 
{
    void Database::InnerJoin(Table* firstTable, Table* secondTable, const vector<Field>& conditions)
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

}