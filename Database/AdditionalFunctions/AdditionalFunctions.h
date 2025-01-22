#pragma once
#include <vector>
#include "../../AdditionalLibraries/AdditionalDataTypes/OrderCondition/SortCondition.h"

namespace DatabaseEngine::StorageTypes {
    class Row;
}

using namespace std;

class AdditionalFunctions{

    

    public:
        static AdditionalFunctions& Get();
        static void OrderBy(vector<DatabaseEngine::StorageTypes::Row>& rows, const vector<SortType>& sortConditions);
};