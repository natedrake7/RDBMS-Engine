#pragma once
#include <string>
#include "../DatabaseConstants.h"
#include "../../../Systemic/include/Headers.h"
#include "../Managers/IdentityManager.h"

namespace DatabaseEngine::StorageTypes
{
    class Table;
    class Block;
    class Row;

    struct ColumnHeader{
        Int id;
        DataType columnType;
        column_index_t columnIndex;
        row_size_t recordSize;
        TinyInt precision;
        TinyInt scale;

        Headers::DefaultValuesHeader defaultValue;
    };

    class Column{
        ColumnHeader header;
        IdentityManager identityManager;

        std::string name;
        const Table *table;
        bool allowNulls;
        bool isOverflowed;

    public:
        Column(
            const std::string& columnName,
            DataType type,
            row_size_t recordSize,
            column_index_t index,
            bool allowNulls
        );

        Column(
            const Headers::sysColumn& header,
            column_index_t ordinalPosition,
            const Table* table
        );

        explicit Column(
            const Headers::ColumnHeader& masterDbHeader,
            const Table *table
        );

        ~Column();

        [[nodiscard]] const std::string& GetColumnName() const;

        void SetColumnName(const std::string& otherName);

        [[nodiscard]] DataType GetColumnType() const;

        [[nodiscard]] row_size_t GetColumnSize() const;

        [[nodiscard]] bool IsColumnNullable() const;

        [[nodiscard]] bool GetAllowNulls() const;

        void SetColumnIndex(column_index_t columnIndex);

        [[nodiscard]] column_index_t GetColumnIndex() const;

        [[nodiscard]] const ColumnHeader& GetColumnHeader() const;

        [[nodiscard]] bool isColumnLOB() const;

        [[nodiscard]] bool isColumnOverflowed() const;

        [[nodiscard]] Int GetColumnId() const;

        void SetColumnId(Int columnId);

        void SetIdentityManagerIds(Int tableId);

        [[nodiscard]] const Headers::IdentityColumnsHeader&  GetIdentity()const;

        void SetIdentity(const Headers::IdentityColumnsHeader &identity);

        void SetDefaultValue(const Headers::DefaultValuesHeader &defaultValue);

        [[nodiscard]] const Headers::DefaultValuesHeader &GetDefaultValue() const;

        void SetIsOverflowed(bool isOverflow);

        [[nodiscard]] BigInt GenerateIdentityValue();

        void UpdateMetadata()const;

        [[nodiscard]] bool HasIdentity() const;
    };
}