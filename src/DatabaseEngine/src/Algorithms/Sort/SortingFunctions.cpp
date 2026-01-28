#include "../../../../Systemic/include/GroupCondition.h"
#include "../../../include/DataStorage/Row.h"
#include "../../../include/Algorithms/Sort/SortingFunctions.h"
#include "../../../include/Algorithms/AggregateFunctions.h"
#include "../../../include/Algorithms/Sort/MergeSort.h"
#include "../../../include/Algorithms/Sort/QuickSort.h"

#include <ranges>
#include <cstring>

using namespace DatabaseEngine::StorageTypes;

bool SortingFunctions::CompareRowsAscending(const Pages::RowReference& firstRow, const Pages::RowReference& secondRow, const column_index_t& columnIndex){
    return (firstRow.PartialMaterialize(columnIndex) < secondRow.PartialMaterialize(columnIndex)).AsBool();
}

bool SortingFunctions::CompareRowsDescending(const Pages::RowReference& firstRow, const Pages::RowReference& secondRow, const column_index_t &columnIndex)
{
    return !SortingFunctions::CompareRowsAscending(firstRow, secondRow, columnIndex);
}

AggregateResults::AggregateResults()
{
    this->average = 0;
    this->min = 0;
    this->max = 0;
    this->sum = 0;
    this->count = 0;
}

MergeElement& MergeElement::operator=(const MergeElement& other)= default;

MergeElement& MergeElement::operator=(MergeElement&& other) noexcept {
    this->value = std::move(other.value);
    this->batchId = other.batchId;
    this->rowId = other.rowId;

    return *this;
}

MergeElement::MergeElement(const QueryResult& value, const Int batchId){
    this->value = value;
    this->batchId = batchId;
}

MergeElement::MergeElement(QueryResult& value, const Int batchId, const DataTypes::RowIdentifier& rowId)
    : value(std::move(value)), rowId(rowId), batchId(batchId) {}

MergeElement::MergeElement(const MergeElement& other){
    this->value = other.value;
    this->batchId = other.batchId;
    this->rowId = other.rowId;
}

MergeElement::MergeElement(MergeElement&& other) noexcept {
    this->value = std::move(other.value);
    this->batchId = other.batchId;
    this->rowId = other.rowId;
}

bool SortingFunctions::CompareRows(
    const QueryResult& firstRow,
    const QueryResult& secondRow,
    const vector<QueryPipeline::Statements::OrderColumn*> &sortConditions
){
    Expressions::EvaluationContext context(Expressions::EvaluationContext::EvaluationContextType::MaterializedRow, {});
    for (const auto& condition : sortConditions)
    {
        context.materializedRow = firstRow;
        const auto& firstValue = condition->expression->Evaluate(context);

        context.materializedRow = secondRow;
        const auto& secondValue = condition->expression->Evaluate(context);

        //if column is indexed(and it is the first condition, it is already sorted by it so set the result accordingly result is positive)
        // const int result = SortingFunctions::CompareBlockByDataType(firstRowData, secondRowData);
        int result = 0;

        if ((firstValue < secondValue).AsBool())
            result = 1;
        if ((firstValue > secondValue).AsBool())
            result = -1;

        if(result == 0)
            continue;

        return (condition->type == OrderType::DESCENDING)
                        ? (result < 0)
                        : (result > 0);
    }

    return false;
}

void SortingFunctions::OrderBy(vector<QueryResult> &rows, const vector<QueryPipeline::Statements::OrderColumn*> &conditions){
    if(rows.empty())
        return;

    //handle multiple conditions priority
    //after first condition break into multiple arrays where the first condition is satisfied
    //order by the 2nd condition. do it for the rest etc.
    // const auto& condition = conditions.front();
    // const bool& isColumnIndexed = condition.GetIsColumnIndexed();
    // const OrderType sortType = condition->type;
    //
    // if(isColumnIndexed && sortType == OrderType::ASCENDING)
    //     return;
    // if(isColumnIndexed && sortType == OrderType::DESCENDING)
    // {
    //     ranges::reverse(rows);
    //     return;
    // }

    //if dataset is small use quicksort (is it needed?)
    // if(rows.size() <= 1000)
    // {
    //     QuickSort::Sort(rows, 0, static_cast<int>(rows.size() - 1), sortConditions);
    //     return;
    // }
    //else mergesort
    
    MergeSort::Sort(rows, 0, static_cast<int>(rows.size() - 1), conditions);
}

unordered_map<string, AggregateResults> SortingFunctions::GroupBy(
    const vector<Pages::RowReference> &rows,
    const vector<GroupCondition> &sortConditions
){
    unordered_map<string, AggregateResults> groupedResults;
    unordered_map<string, vector<Pages::RowReference>> groupedRows;

    //add any aggregate function execution asWell by condition
    //also store the keys of the groupBy used in order to prin them.
    //should be done in a single loop
    for(const auto& row : rows)
        groupedRows[SortingFunctions::CreateGroupByKey(row, sortConditions)].push_back(row);

    for(const auto& [key , rowGroup ] : groupedRows)
    {
        AggregateResults aggregateResults;
        for(const auto& condition : sortConditions)
        {
            switch (condition.GetAggregateFunction())
            {
                case NONE:
                case COUNT:
                default:
                    aggregateResults.count = AggregateFunctions::Count(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
                    break;
                case SUM:
                    aggregateResults.sum = AggregateFunctions::Sum(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
                    break;
                case MIN:
                    aggregateResults.min = AggregateFunctions::Min(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
                    break;
                case MAX:
                    aggregateResults.max = AggregateFunctions::Max(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
                    break;
                case AVERAGE:
                    aggregateResults.average = AggregateFunctions::Average(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
                    break;
            }
        }

        groupedResults[key] = aggregateResults;
    }

    return groupedResults;
}

MergeComparator::MergeComparator(const std::vector<QueryPipeline::Statements::OrderColumn*>* sortConditions)
    : sortConditions(sortConditions){}

bool MergeComparator::operator()(const MergeElement& first, const MergeElement& second) const{
    return SortingFunctions::CompareRows(
        first.value,
        second.value,
        *this->sortConditions
    );
}

string SortingFunctions::CreateGroupByKey(const Pages::RowReference& row, const vector<GroupCondition> &sortConditions)
{
    string hashKey;
    for(const auto& condition : sortConditions){
        const auto value = row.PartialMaterialize(condition.GetColumnIndex());
        hashKey.append(reinterpret_cast<const char*>(value.Data()), value.Size());
    }

    return hashKey;
}

long double SortingFunctions::ApplyAggregateFunctionToGroup(const vector<Pages::RowReference> &rowGroup, const GroupCondition &condition)
{
    switch (condition.GetAggregateFunction())
    {
        case NONE:
        case COUNT:
        default:
            return static_cast<long double>(AggregateFunctions::Count(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue()));
        case SUM:
            return AggregateFunctions::Sum(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
        case MIN:
            return AggregateFunctions::Min(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
        case MAX:
            return AggregateFunctions::Max(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
    }
}
