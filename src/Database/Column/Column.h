#pragma once
#include <string>
#include "../Constants.h"
#include "../../AdditionalLibraries/DataTypes/Headers/Headers.h"
#include "IdentityManager/IdentityManager.h"

namespace DatabaseEngine::StorageTypes
{
    class Table;
    class Block;
    class Row;

    typedef struct ColumnHeader
    {
        int32_t id;
        Constants::DataType columnType;
        Constants::column_index_t columnIndex;
        Constants::row_size_t recordSize;
        int8_t precision;
        int8_t scale;

        Headers::DefaultValuesHeader defaultValue;
    } ColumnHeader;

    class Column
    {
        ColumnHeader header;
        IdentityManager identityManager;
        Headers::ColumnStatistics statistics;

        std::string name;
        const Table *table;
        bool allowNulls;
        bool isOverflowed;

    public:
        Column(
            const std::string& columnName,
            const Constants::DataType& type,
            const Constants::row_size_t& recordSize,
            const Constants::column_index_t& index,
            const bool& allowNulls
        );

        Column(
            const Headers::sysColumn& header,
            const Constants::column_index_t& tablePos ,
            const Table* table
        );

        explicit Column(
            const Headers::ColumnHeader& masterDbHeader,
            const Table *table
        );

        ~Column();

        [[nodiscard]] const string& GetColumnName() const;

        void SetColumnName(const std::string& name);

        [[nodiscard]] const Constants::DataType &GetColumnType() const;

        [[nodiscard]] const Constants::row_size_t &GetColumnSize() const;

        [[nodiscard]] bool IsColumnNullable() const;

        [[nodiscard]] const bool &GetAllowNulls() const;

        void SetColumnIndex(const Constants::column_index_t &columnIndex);

        [[nodiscard]] const Constants::column_index_t &GetColumnIndex() const;

        [[nodiscard]] const ColumnHeader &GetColumnHeader() const;

        [[nodiscard]] bool isColumnLOB() const;

        [[nodiscard]] bool isColumnOverflowed() const;

        [[nodiscard]] const int32_t& GetColumnId() const;

        void SetColumnId(const int32_t &columnId);

        [[nodiscard]] const Headers::IdentityColumnsHeader&  GetIdentity()const;

        void SetIdentity(const Headers::IdentityColumnsHeader &identity);

        void SetDefaultValue(const Headers::DefaultValuesHeader &defaultValue);

        [[nodiscard]] const Headers::DefaultValuesHeader &GetDefaultValue() const;

        void SetIsOverflowed(const bool &isOverflowed);

        void SetColumnStatistics(const Headers::ColumnStatistics& statistics);

        void UpdateColumnStatistics(const StorageTypes::Row* row);

        [[nodiscard]] bool GenerateIdentityValue(int64_t& value);

        void UpdateMetadata()const;

        [[nodiscard]] bool HasIdentity() const;
    };
}