#pragma once
#include <vector>
#include "../../AdditionalLibraries/AdditionalDataTypes/SortCondition/SortCondition.h"

namespace DatabaseEngine::StorageTypes {
    class Row;
}

using namespace std;

class SortingFunctions{
    static void OrderDescending(vector<DatabaseEngine::StorageTypes::Row>& rows, const SortCondition& condition);
    static bool CompareRows(const DatabaseEngine::StorageTypes::Row& firstRow, const DatabaseEngine::StorageTypes::Row& secondRow);

    public:
        static SortingFunctions& Get();
        static void OrderBy(vector<DatabaseEngine::StorageTypes::Row>& rows, const vector<SortCondition>& sortConditions);
        static void GroupBy(vector<DatabaseEngine::StorageTypes::Row>& rows, const vector<SortCondition>& sortConditions);
};