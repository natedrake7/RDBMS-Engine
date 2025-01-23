#include "SortingFunctions.h"

void SortingFunctions::OrderDescending(vector<DatabaseEngine::StorageTypes::Row> &rows, const SortCondition &condition)
{
    const bool& isColumnIndexed = condition.GetIsColumnIndexed();
    const SortType sortType = condition.GetSortType();

    if(isColumnIndexed && sortType == SortType::ASCENDING)
        return;
    else if(isColumnIndexed && sortType == SortType::DESCENDING)
    {
        std::reverse(rows.begin(), rows.end());
        return;
    }

    for(const auto& row : rows)
    {
        
    }
}

bool SortingFunctions::CompareRows(const DatabaseEngine::StorageTypes::Row &firstRow, const DatabaseEngine::StorageTypes::Row &secondRow)
{
    
}

SortingFunctions& SortingFunctions::Get()
{
    static SortingFunctions instance;

    return instance;
}

void SortingFunctions::OrderBy(vector<DatabaseEngine::StorageTypes::Row> &rows, const vector<SortCondition> &sortConditions)
{
    if(rows.empty())
        return;

    for(const SortCondition &condition : sortConditions)
        SortingFunctions::OrderDescending(rows, condition);
}

void SortingFunctions::GroupBy(vector<DatabaseEngine::StorageTypes::Row> &rows, const vector<SortCondition> &sortConditions)
{
    
}
