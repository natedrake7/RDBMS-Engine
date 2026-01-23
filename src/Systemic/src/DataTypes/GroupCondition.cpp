#include "../../include/GroupCondition.h"

GroupCondition::GroupCondition(const column_index_t &columnIndex, const DataType &columnType, const Constants::AggregateFunction& aggregateFunction, const bool &isColumnIndexed, const long double* constantValue )
{
    this->columnIndex = columnIndex;
    this->columnType = columnType;
    this->aggregateFunction = aggregateFunction;
    this->isColumnIndexed = isColumnIndexed;
    this->constantValue = constantValue == nullptr
                        ? nullptr
                        : new long double(*constantValue);
}

GroupCondition::~GroupCondition()
{
    delete this->constantValue;
}

const DataType & GroupCondition::GetColumnType() const { return this->columnType; }

const column_index_t & GroupCondition::GetColumnIndex() const { return this->columnIndex; }

const bool & GroupCondition::GetIsColumnIndexed() const { return this->isColumnIndexed; }

const Constants::AggregateFunction & GroupCondition::GetAggregateFunction() const { return this->aggregateFunction; }

long double* GroupCondition::GetConstantValue() const { return this->constantValue; }
