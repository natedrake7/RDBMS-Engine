#include "SortingFunctions.h"

SortingFunctions& SortingFunctions::Get()
{
    static SortingFunctions instance;

    return instance;
}

void SortingFunctions::OrderBy(vector<DatabaseEngine::StorageTypes::Row> &rows, const vector<SortType> &sortConditions)
{
    
}
