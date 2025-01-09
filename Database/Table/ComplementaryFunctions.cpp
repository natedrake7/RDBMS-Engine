#include "Table.h"
#include "Table.h"
#include "../../AdditionalLibraries/AdditionalObjects/DateTime/DateTime.h"
#include "../../AdditionalLibraries/AdditionalObjects/Decimal/Decimal.h"
#include "../../AdditionalLibraries/AdditionalObjects/Field/Field.h"
#include "../../AdditionalLibraries/AdditionalObjects/RowCondition/RowCondition.h"
#include "../../AdditionalLibraries/BitMap/BitMap.h"
#include "../../AdditionalLibraries/SafeConverter/SafeConverter.h"
#include "../../AdditionalLibraries/B+Tree/BPlusTree.h"
#include "../Block/Block.h"
#include "../Column/Column.h"
#include "../Constants.h"
#include "../Database.h"
#include "../Pages/LargeObject/LargeDataPage.h"
#include "../Pages/IndexPage/IndexPage.h"
#include "../Pages/IndexMapAllocation/IndexAllocationMapPage.h"
#include "../Pages/Header/HeaderPage.h"
#include "../Pages/PageFreeSpace/PageFreeSpacePage.h"
#include "../Storage/StorageManager/StorageManager.h"
#include "../Pages/Page.h"
#include "../Row/Row.h"
#include <stdexcept>

using namespace Pages;
using namespace DataTypes;
using namespace ByteMaps;
using namespace Indexing;
using namespace Storage;

namespace DatabaseEngine::StorageTypes{
    void Table::GetIndexedColumnKeys(vector<column_index_t> *vector) const 
    {
        for (bit_map_pos_t i = 0; i < this->header.clusteredIndexesBitMap->GetSize(); i++)
            if (this->header.clusteredIndexesBitMap->Get(i))
                vector->push_back(i);
    }

    void Table::GetNonClusteredIndexedColumnKeys(vector<vector<column_index_t>>* vector) const
    {
        if(this->header.nonClusteredIndexesBitMap.empty())
            return;

        vector->resize(this->header.nonClusteredIndexesBitMap.size());

        for (int i = 0;i < this->header.nonClusteredIndexesBitMap.size(); i++)
        {
            for (bit_map_pos_t j = 0; j < this->header.nonClusteredIndexesBitMap[i]->GetSize(); j++)
                if(this->header.nonClusteredIndexesBitMap[i]->Get(j))
                    (*vector)[i].push_back(j);
        }
    }

    bool Table::HasNonClusteredIndexes() { return !this->header.nonClusteredIndexesBitMap.empty(); }

    void Table::SetIndexPageId(const page_id_t &indexPageId) { this->header.clusteredIndexPageId = indexPageId; }

    void Table::SetIndexAllocationMapPageId(const page_id_t & pageId) { this->header.indexAllocationMapPageId = pageId; }

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