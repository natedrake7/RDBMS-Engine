#pragma once
#include <string>
#include "../Constants.h"
#include "../../../Systemic/include/Headers.h"
#include "../Managers/IdentityManager.h"

namespace DatabaseEngine::StorageTypes
{
    class Table;
    class Block;
    class Row;

    struct ColumnHeader
    {
        int32_t id;
        DataType columnType;
        column_index_t columnIndex;
        row_size_t recordSize;
        int8_t precision;
        int8_t scale;

        Headers::DefaultValuesHeader defaultValue;
    };

    class Column
    {
        ColumnHeader header;
        IdentityManager identityManager;

        MultiThreading::ReadWriteMutex statisticsLatch;
        Headers::ColumnStatistics statistics;

        std::vector<Headers::ColumnHistograms> histograms;

        std::string name;
        const Table *table;
        bool allowNulls;
        bool isOverflowed;

    public:
        Column(
            const std::string& columnName,
            const DataType& type,
            const row_size_t& recordSize,
            const column_index_t& index,
            const bool& allowNulls
        );

        Column(
            const Headers::sysColumn& header,
            const column_index_t& tablePos ,
            const Table* table
        );

        explicit Column(
            const Headers::ColumnHeader& masterDbHeader,
            const Table *table
        );

        ~Column();

        [[nodiscard]] const string& GetColumnName() const;

        void SetColumnName(const std::string& otherName);

        [[nodiscard]] const DataType &GetColumnType() const;

        [[nodiscard]] const row_size_t &GetColumnSize() const;

        [[nodiscard]] bool IsColumnNullable() const;

        [[nodiscard]] const bool &GetAllowNulls() const;

        void SetColumnIndex(const column_index_t &columnIndex);

        [[nodiscard]] const column_index_t &GetColumnIndex() const;

        [[nodiscard]] const ColumnHeader &GetColumnHeader() const;

        [[nodiscard]] bool isColumnLOB() const;

        [[nodiscard]] bool isColumnOverflowed() const;

        [[nodiscard]] const int32_t& GetColumnId() const;

        void SetColumnId(const int32_t &columnId);

        void SetIdentityManagerIds(const int32_t& tableId);

        [[nodiscard]] const Headers::IdentityColumnsHeader&  GetIdentity()const;

        void SetIdentity(const Headers::IdentityColumnsHeader &identity);

        void SetDefaultValue(const Headers::DefaultValuesHeader &defaultValue);

        [[nodiscard]] const Headers::DefaultValuesHeader &GetDefaultValue() const;

        void SetIsOverflowed(const bool &isOverflow);

        void SetColumnStatistics(const Headers::ColumnStatistics& stats);

        void SetHistograms(std::vector<Headers::ColumnHistograms>& otherHistograms);

        void UpdateColumnStatistics(const StorageTypes::Row* row);

        [[nodiscard]] bool GenerateIdentityValue(int64_t& value);

        void UpdateMetadata()const;

        [[nodiscard]] bool HasIdentity() const;
    };
}