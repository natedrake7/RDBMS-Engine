#include "../../../include/DataStorage/Table.h"
#include "../../../include/DataStorage/Block.h"

namespace DatabaseEngine::StorageTypes{
    Database* Table::GetDatabase() const { return this->database; }

    void Table::InsertNullValues(Block *&block, Row* row, const column_index_t &columnIndex){
        block->SetData(nullptr, 0);
        row->SetNullBitMapValue(columnIndex, true);
        row->InsertColumnData(block, columnIndex);
    }
}