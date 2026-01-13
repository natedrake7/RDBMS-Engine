#include "../../../../Systemic/include/GroupCondition.h"
#include "../../../include/DataStorage/Block.h"
#include "../../../include/DataStorage/Row.h"
#include "../../../include/Algorithms/Sort/SortingFunctions.h"
#include "../../../include/Algorithms/AggregateFunctions.h"
#include "../../../include/Algorithms/Sort/MergeSort.h"
#include "../../../include/Algorithms/Sort/QuickSort.h"

#include <ranges>
#include <cstring>

using namespace DatabaseEngine::StorageTypes;

bool SortingFunctions::CompareRowsAscending(const Row *firstRow, const Row *secondRow, const column_index_t& columnIndex)
{
    const Block* firstRowData = firstRow->GetData()[columnIndex];
    const Block* secondRowData = secondRow->GetData()[columnIndex];
    
    return SortingFunctions::CompareBlockByDataType(firstRowData, secondRowData);
}

bool SortingFunctions::CompareRowsDescending(const Row* firstRow, const Row* secondRow, const column_index_t &columnIndex)
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

MergeElement::MergeElement(const QueryResult& value, const Int& batchId){
    this->value = value;
    this->batchId = batchId;
}

MergeElement::MergeElement(QueryResult& value, const int& batchId, Headers::RowIdentifier& rowId)
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

int SortingFunctions::CompareBlockByDataType(const Block *&firstBlock, const Block *&secondBlock)
{
    switch (firstBlock->GetColumnType())
    {
        case DataType::TinyInt:
        {
            const auto& firstBlockData = *reinterpret_cast<const int8_t*>(firstBlock->GetRawData());
            const auto& secondBlockData = *reinterpret_cast<const int8_t*>(secondBlock->GetRawData());

            if (firstBlockData < secondBlockData) return 1;
            if (firstBlockData > secondBlockData) return -1;
            return 0;
        }
        case DataType::SmallInt:
        {
            const auto& firstBlockData = *reinterpret_cast<const int16_t*>(firstBlock->GetRawData());
            const auto& secondBlockData = *reinterpret_cast<const int16_t*>(secondBlock->GetRawData());

            if (firstBlockData < secondBlockData) return 1;
            if (firstBlockData > secondBlockData) return -1;
            return 0;
        }
        case DataType::Int:
        {
            const auto& firstBlockData = *reinterpret_cast<const int32_t*>(firstBlock->GetRawData());
            const auto& secondBlockData = *reinterpret_cast<const int32_t*>(secondBlock->GetRawData());

            if (firstBlockData < secondBlockData) return 1;
            if (firstBlockData > secondBlockData) return -1;
            return 0;
        }
        case DataType::BigInt:
        {
            const auto& firstBlockData = *reinterpret_cast<const int64_t*>(firstBlock->GetRawData());
            const auto& secondBlockData = *reinterpret_cast<const int64_t*>(secondBlock->GetRawData());

            if (firstBlockData < secondBlockData) return 1;
            if (firstBlockData > secondBlockData) return -1;
            return 0;
        }
        case DataType::Decimal:
        {
            //implement support for decimal class operations
            return true;
        }
        case DataType::DateTime:
        {
            const auto& firstBlockData = *reinterpret_cast<const time_t*>(firstBlock->GetRawData());
            const auto& secondBlockData = *reinterpret_cast<const time_t*>(secondBlock->GetRawData());

            if (firstBlockData < secondBlockData) return 1;
            if (firstBlockData > secondBlockData) return -1;
            return 0;
        }
        case DataType::Bool:
        {
            const auto& firstBlockData = firstBlock->GetBool();
            const auto& secondBlockData = secondBlock->GetBool();

            if (firstBlockData < secondBlockData) return 1;
            if (firstBlockData > secondBlockData) return -1;
            return 0;
        }
        case DataType::String:
        {
            const auto& firstBlockDataSize = firstBlock->GetSize();
            const auto& secondBlockDataSize = secondBlock->GetSize();
            
            if (firstBlockDataSize < secondBlockDataSize) return 1;
            if (firstBlockDataSize > secondBlockDataSize) return -1;

            const int result = memcmp(firstBlock->GetRawData(), secondBlock->GetRawData(), firstBlockDataSize);

            if (result > 0) return 1;
            if (result < 0) return -1;
            return 0;
        }
        case DataType::Guid:
        {
            //both guids are 16 bytes in memory
            const auto& dataSize = firstBlock->GetSize();

            const int result = memcmp(firstBlock->GetRawData(), secondBlock->GetRawData(), dataSize);

            if (result > 0) return 1;
            if (result < 0) return -1;
            return 0;
        }
        default:
            throw invalid_argument("AggregateFunctions::CompareMaxWithRow(): Unsupported column type");
    }
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

        if ((firstValue < secondValue).GetBool())
            result = 1;
        if ((firstValue > secondValue).GetBool())
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

unordered_map<string, AggregateResults> SortingFunctions::GroupBy(const vector<Row*> &rows, const vector<GroupCondition> &sortConditions)
{
    unordered_map<string, AggregateResults> groupedResults;
    unordered_map<string, vector<Row*>> groupedRows;

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

string SortingFunctions::CreateGroupByKey(const Row* row, const vector<GroupCondition> &sortConditions)
{
    string hashKey;
    const auto& rowData = row->GetData();

    for(const auto& condition : sortConditions)
    {
        const auto& block = rowData[condition.GetColumnIndex()];
        
        hashKey.append(reinterpret_cast<const char*>(block->GetRawData()), block->GetSize());
    }

    return hashKey;
}

long double SortingFunctions::ApplyAggregateFunctionToGroup(const vector<Row *> &rowGroup, const GroupCondition &condition)
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
