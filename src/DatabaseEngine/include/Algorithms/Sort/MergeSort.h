#pragma once
#include <vector>
#include "../../../include/Constants.h"
#include "../../../../Systemic/include/DataTypes/SortCondition.h"

namespace QueryPipeline::Statements {
  struct OrderColumn;
}

namespace Expressions {
  class Expression;
}

class QueryResult;

namespace DatabaseEngine::StorageTypes {
    class Row;
}

using namespace Constants;
using namespace std;

class MergeSort {
        static void Merge(vector<QueryResult>& rows, const int& left, const int& mid, const int& right, const vector<QueryPipeline::Statements::OrderColumn*>& sortConditions);
    public:
        static void Sort(vector<QueryResult>& rows, const int& left, const int& right, const vector<QueryPipeline::Statements::OrderColumn*>& sortConditions);
};
