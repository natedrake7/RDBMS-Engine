#include "Table.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/DateTime/DateTime.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Decimal/Decimal.h"
#include "../../AdditionalLibraries/AdditionalDataTypes/Field/Field.h"
#include "../../AdditionalLibraries/SafeConverter/SafeConverter.h"
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
    void Table::GetIndexedColumnKeys(vector<column_index_t> *vector) const 
    {
        *vector = this->header.clusteredColumnIndexes; 
    }

    void Table::GetNonClusteredIndexedColumnKeys(vector<vector<column_index_t>>* vector) const
    {
        if(this->header.nonClusteredColumnIndexes.empty())
            return;

        vector->resize(this->header.nonClusteredColumnIndexes.size());

        for(const auto& nonClusteredIndex : this->header.nonClusteredColumnIndexes)
            vector->push_back(nonClusteredIndex);
    }

    bool Table::HasNonClusteredIndexes() const { return !this->header.nonClusteredColumnIndexes.empty(); }

    Database * Table::GetDatabase() const { return this->database; }

    void Table::SetClusteredIndexPageId(const page_id_t &indexPageId) 
    { 
        this->header.clusteredIndexPageId = indexPageId; 
    }

    const page_id_t & Table::GetClusteredIndexPageId() const { return this->header.clusteredIndexPageId; }

    void Table::SetNonClusteredIndexPageId(const page_id_t & indexPageId, const int& indexPosition) { this->header.nonClusteredIndexPageIds.at(indexPosition) = indexPageId; }

    const page_id_t & Table::GetNonClusteredIndexPageId(const int & indexPosition) const { return this->header.nonClusteredIndexPageIds.at(indexPosition); }

    const uint8_t & Table::GetNonClusteredIndexId(const int & indexPosition) const { return this->header.nonClusteredIndexesIds.at(indexPosition); }

    void Table::SetIndexAllocationMapPageId(const page_id_t & pageId) { this->header.indexAllocationMapPageId = pageId; }

    void Table::CheckAndInsertNullValues(Block *&block, Row *&row, const column_index_t &associatedColumnIndex)  
    {
        block->SetData(nullptr, 0);
        row->SetNullBitMapValue(associatedColumnIndex, true);
        row->InsertColumnData(block, associatedColumnIndex);
    }
}