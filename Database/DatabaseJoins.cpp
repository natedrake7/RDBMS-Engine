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

    void Database::MergeRows(Row& row, const vector<Row>& selectedRows, const vector<column_index_t>& selectedColumnIndices, const Table* secondTable)
    {
        if(selectedRows.empty())
        {
            // const auto& columns = secondTable->GetColumns();
            // for(const auto& columnIndex: selectedColumnIndices)
            //     row.InsertNewColumn(new Block(nullptr, 0, columns[columnIndex]));

            return;    
        }

        // for(const auto& selectedRow: selectedRows)
        //     for(const auto& block: selectedRow.GetData())
        //         row.InsertNewColumn(new Block(block));


    }

    void Database::GetIdentityColumns()const{
      for(const auto& table: this->tables)
        table->GetIdentityColumns();
    }

    void Database::GetColumnsHeaders() const{
        for (const auto& table : this->tables)
            table->GetColumnsHeaders();
    }

    void Database::GetDefaultValues() const{
        for (const auto& table : this->tables)
            table->GetDefaultValuesHeaders();
    }

    void Database::GetIndexes() const{
        for (const auto& table : this->tables)
            table->GetIndexes();
    }

    void Database::GetTableHeaders() const{
    }

    void Database::UpdateMasterDatabase()const{
      for(const auto& table: this->tables)
        table->UpdateMasterDatabase();
    }

    string Database::GetSystemFilename() const{ return this->systemFilename;}
}