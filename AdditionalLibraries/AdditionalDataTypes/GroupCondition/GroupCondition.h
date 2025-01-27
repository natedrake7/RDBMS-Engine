#pragma once
#include "../../../Database/Constants.h"
#include "../../../Database/Column/Column.h"

using namespace Constants;

using namespace DatabaseEngine::StorageTypes;

class GroupCondition {
    column_index_t columnIndex;
    ColumnType columnType;
    bool isColumnIndexed;
    AggregateFunction aggregateFunction;
    long double* constantValue;
    

    public:
        GroupCondition(const column_index_t& columnIndex, const ColumnType& columnType, const AggregateFunction& aggregateFunction = NONE, const bool& isColumnIndexed = false, const long double* constantValue = nullptr);
        ~GroupCondition();
        [[nodiscard]] const ColumnType& GetColumnType() const;
        [[nodiscard]] const column_index_t& GetColumnIndex() const;
        [[nodiscard]] const bool& GetIsColumnIndexed() const;
        [[nodiscard]] const AggregateFunction& GetAggregateFunction() const;
        [[nodiscard]] long double* GetConstantValue() const;
};