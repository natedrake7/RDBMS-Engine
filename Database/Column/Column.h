#pragma once
#include <string>
#include "../Constants.h"

using namespace std;
using namespace Constants;

namespace DatabaseEngine::StorageTypes
{
    class Table;
    class Block;

    typedef struct ColumnHeader
    {
        header_literal_t columnNameSize;
        string columnName;

        header_literal_t columnTypeLiteralSize;
        string columnTypeLiteral;

        ColumnType columnType;
        column_index_t columnIndex;
        row_size_t recordSize;
    } ColumnHeader;

    class Column
    {
        ColumnHeader header;
        const Table *table;
        bool allowNulls;

    protected:
        [[nodiscard]] ColumnType SetColumnType() const;

    public:
        Column(const string &columnName, const string &columnTypeLiteral, const row_size_t &recordSize, const bool &allowNulls);

        explicit Column(const ColumnHeader &header, const Table *table);

        ~Column();

        string &GetColumnName();

        [[nodiscard]] const ColumnType &GetColumnType() const;

        [[nodiscard]] const row_size_t &GetColumnSize() const;

        [[nodiscard]] bool IsColumnNullable() const;

        [[nodiscard]] const bool &GetAllowNulls() const;

        void SetColumnIndex(const column_index_t &columnIndex);

        [[nodiscard]] const column_index_t &GetColumnIndex() const;

        [[nodiscard]] const ColumnHeader &GetColumnHeader() const;

        [[nodiscard]] bool isColumnLOB() const;
    };
}