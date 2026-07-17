#include "../../../../Systemic/include/GroupCondition.h"
#include "../../../include/DataStorage/Row.h"
#include "../../../include/Algorithms/Sort/SortingFunctions.h"
#include "../../../include/Algorithms/AggregateFunctions.h"
#include "../../../include/Algorithms/Sort/MergeSort.h"
#include "../../../include/Algorithms/Sort/QuickSort.h"

#include <ranges>
#include "Evaluators/Expression.h"

#include "../../QueryPipeline/include/Statements.h"
#include "Contexts/ExecutionContext.h"
#include "DataStructures/PolymorphicArray.h"

bool SortingFunctions::CompareRowsAscending(
    const CoreEngine::ExecutionContext& context,
    const CoreEngine::StorageTypes::RID& firstRow,
    const CoreEngine::StorageTypes::RID& secondRow,
    const column_index_t& columnIndex
){
    // return firstRow.PartialMaterialize(context.GetAllocator(), columnIndex) < secondRow.PartialMaterialize(context.GetAllocator(), columnIndex);
}

bool SortingFunctions::CompareRowsDescending(
     const CoreEngine::ExecutionContext& context,
    const CoreEngine::StorageTypes::RID& firstRow,
    const CoreEngine::StorageTypes::RID& secondRow,
    const column_index_t &columnIndex
){
    return !SortingFunctions::CompareRowsAscending(context, firstRow, secondRow, columnIndex);
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
     const CoreEngine::ExecutionContext& context,
    const QueryResult& firstRow,
    const QueryResult& secondRow,
    const DataStructures::PolymorphicArray<QueryPipeline::Statements::OrderColumn*> &sortConditions
){
    Expressions::EvaluationContext evaluationContext(
        Expressions::EvaluationContext::EvaluationContextType::MaterializedRow,
        &context
    );

    for (const auto& condition : sortConditions)
    {
        // evaluationContext.materializedRow = firstRow;
        // const auto firstValue = Expressions::EvaluateExpression(condition->expression, evaluationContext);
        //
        // evaluationContext.materializedRow = secondRow;
        // const auto& secondValue = Expressions::EvaluateExpression(condition->expression, evaluationContext);
        //
        // //if column is indexed(and it is the first condition, it is already sorted by it so set the result accordingly result is positive)
        // // const int result = SortingFunctions::CompareBlockByDataType(firstRowData, secondRowData);
        // int result = 0;
        //
        // if (firstValue < secondValue)
        //     result = 1;
        // if (firstValue > secondValue)
        //     result = -1;
        //
        // if(result == 0)
        //     continue;
        //
        // return (condition->type == Constants::OrderType::DESCENDING)
        //                 ? (result < 0)
        //                 : (result > 0);
    }

    return false;
}

void SortingFunctions::OrderBy(
     const CoreEngine::ExecutionContext& context,
    DataStructures::PolymorphicArray<QueryResult> &rows,
    const DataStructures::PolymorphicArray<QueryPipeline::Statements::OrderColumn*> &conditions
){
    if(rows.Empty())
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

    const auto right = static_cast<int>(rows.Size() - 1);

    MergeSortParameters parameters = {
        .properties = &context,
        .rows = &rows,
        .sortConditions = &conditions,
        .left = 0,
        .right = right,
        .mid = 0
    };
    
    MergeSort::Sort(parameters);
}

std::unordered_map<std::string, AggregateResults> SortingFunctions::GroupBy(
    const std::vector<CoreEngine::StorageTypes::RID> &rows,
    const std::vector<GroupCondition> &sortConditions
){
    std::unordered_map<std::string, AggregateResults> groupedResults;
    std::unordered_map<std::string, std::vector<CoreEngine::StorageTypes::RID>> groupedRows;

    //add any aggregate function execution asWell by condition
    //also store the keys of the groupBy used in order to prin them.
    //should be done in a single loop
    // for(const auto& row : rows)
    //     groupedRows[SortingFunctions::CreateGroupByKey(row, sortConditions)].push_back(row);

    for(const auto& [key , rowGroup ] : groupedRows)
    {
        AggregateResults aggregateResults;
        for(const auto& condition : sortConditions)
        {
            switch (condition.GetAggregateFunction())
            {
                case Constants::NONE:
                case Constants::COUNT:
                default:
                    aggregateResults.count = AggregateFunctions::Count(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
                    break;
                case Constants::SUM:
                    aggregateResults.sum = AggregateFunctions::Sum(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
                    break;
                case Constants::MIN:
                    aggregateResults.min = AggregateFunctions::Min(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
                    break;
                case Constants::MAX:
                    aggregateResults.max = AggregateFunctions::Max(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
                    break;
                case Constants::AVERAGE:
                    aggregateResults.average = AggregateFunctions::Average(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
                    break;
            }
        }

        groupedResults[key] = aggregateResults;
    }

    return groupedResults;
}

MergeComparator::MergeComparator(
    const DataStructures::PolymorphicArray<QueryPipeline::Statements::OrderColumn*>* sortConditions,
    const CoreEngine::ExecutionContext* context
): sortConditions(sortConditions), context(context){}

bool MergeComparator::operator()(const MergeElement& first, const MergeElement& second) const{
    return SortingFunctions::CompareRows(
        *this->context,
        first.value,
        second.value,
        *this->sortConditions
    );
}

void MergeComparator::SetExecutionContext(const CoreEngine::ExecutionContext* otherContext){
    this->context = otherContext;
}

bool MergeComparator::HasProperties() const{
    return this->context != nullptr;
}

std::string SortingFunctions::CreateGroupByKey(const CoreEngine::StorageTypes::RID& row, const std::vector<GroupCondition> &sortConditions)
{
    std::string hashKey;
    for(const auto& condition : sortConditions){
        // const auto value = row.PartialMaterialize(condition.GetColumnIndex());
        // hashKey.append(reinterpret_cast<const char*>(value.Data()), value.Size());
    }

    return hashKey;
}

long double SortingFunctions::ApplyAggregateFunctionToGroup(const std::vector<CoreEngine::StorageTypes::RID> &rowGroup, const GroupCondition &condition)
{
    switch (condition.GetAggregateFunction())
    {
        case Constants::NONE:
        case Constants::COUNT:
        default:
            return static_cast<long double>(AggregateFunctions::Count(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue()));
        case Constants::SUM:
            return AggregateFunctions::Sum(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
        case Constants::MIN:
            return AggregateFunctions::Min(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
        case Constants::MAX:
            return AggregateFunctions::Max(rowGroup, condition.GetColumnIndex(), condition.GetConstantValue());
    }
}
