#include "AdditionalFunctions.h"

AdditionalFunctions& AdditionalFunctions::Get()
{
    static AdditionalFunctions instance;

    return instance;
}

void AdditionalFunctions::OrderBy(vector<DatabaseEngine::StorageTypes::Row> &rows, const vector<SortType> &sortConditions)
{
    
}
