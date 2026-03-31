#pragma once
#include "../../CoreEngine/include/DatabaseConstants.h"

class GroupCondition {
    column_index_t columnIndex;
    DataType columnType;
    bool isColumnIndexed;
    Constants::AggregateFunction aggregateFunction;
    long double* constantValue;
    

    public:
        GroupCondition(
            const column_index_t& columnIndex,
            const DataType& columnType,
            const Constants::AggregateFunction& aggregateFunction = Constants::NONE,
            const bool& isColumnIndexed = false,
            const long double* constantValue = nullptr
        );
        ~GroupCondition();
        [[nodiscard]] const DataType& GetColumnType() const;
        [[nodiscard]] const column_index_t& GetColumnIndex() const;
        [[nodiscard]] const bool& GetIsColumnIndexed() const;
        [[nodiscard]] const Constants::AggregateFunction& GetAggregateFunction() const;
        [[nodiscard]] long double* GetConstantValue() const;
};