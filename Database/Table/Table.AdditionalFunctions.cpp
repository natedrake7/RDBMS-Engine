#include "Table.h"
#include "../../AdditionalLibraries/DataTypes/DateTime/DateTime.h"
#include "../../AdditionalLibraries/DataTypes/Decimal/Decimal.h"
#include "../../AdditionalLibraries/DataTypes/Value/Value.h"
#include "../../AdditionalLibraries/Converter/Converter.h"
#include "../Block/Block.h"
#include "../Column/Column.h"
#include "../Constants.h"
#include "../Database.h"
#include "../Storage/StorageManager/StorageManager.h"
#include "../Row/Row.h"
#include "../B+Tree/BPlusTree.h"
#include <stdexcept>

using namespace Pages;
using namespace DataTypes;
using namespace ByteMaps;
using namespace Indexing;
using namespace Storage;

namespace DatabaseEngine::StorageTypes{
    bool Table::HasNonClusteredIndexes() const { return !this->header.nonClusteredIndexes.empty(); }

    Database * Table::GetDatabase() const { return this->database; }

    void Table::SetClusteredIndexPageId(const page_id_t &indexPageId) 
    {
        this->header.clusteredIndexPageId = indexPageId;
    }

    const page_id_t & Table::GetClusteredIndexPageId() const { return this->header.clusteredIndexPageId; }

    void Table::SetNonClusteredIndexPageId(const page_id_t & indexPageId, const int& indexPosition) { this->header.nonClusteredIndexPageIds.at(indexPosition) = indexPageId; }

    const page_id_t & Table::GetNonClusteredIndexPageId(const int & indexPosition) const { return this->header.nonClusteredIndexPageIds.at(indexPosition); }

    void Table::CheckAndInsertNullValues(Block *&block, Row *&row, const column_index_t &associatedColumnIndex)
    {
        block->SetData(nullptr, 0);
        row->SetNullBitMapValue(associatedColumnIndex, true);
        row->InsertColumnData(block, associatedColumnIndex);
    }
}