#include "../../../include/DataStorage/Table.h"
#include "../../../include/DataStorage/Block.h"

namespace DatabaseEngine::StorageTypes{
    bool Table::HasNonClusteredIndexes() const { return !this->header.nonClusteredIndexes.empty(); }

    Database * Table::GetDatabase() const { return this->database; }

    void Table::SetClusteredIndexPageId(const page_id_t &indexPageId) {
        this->header.clusteredIndexPageId = indexPageId;
    }

    const page_id_t & Table::GetClusteredIndexPageId() const { return this->header.clusteredIndexPageId; }

    void Table::SetNonClusteredIndexPageId(const page_id_t & indexPageId, const int& indexPosition) { this->header.nonClusteredIndexPageIds.at(indexPosition) = indexPageId; }

    const page_id_t & Table::GetNonClusteredIndexPageId(const int & indexPosition) const { return this->header.nonClusteredIndexPageIds.at(indexPosition); }

    void Table::InsertNullValues(Block *&block, Row* row, const column_index_t &columnIndex)
    {
        block->SetData(nullptr, 0);
        row->SetNullBitMapValue(columnIndex, true);
        row->InsertColumnData(block, columnIndex);
    }
}