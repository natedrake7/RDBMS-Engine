﻿#pragma once
#include <string>
#include <stdexcept>
#include <vector>
#include "../Constants.h"

using namespace std;
using namespace Constants;

namespace DatabaseEngine::StorageTypes
{
    class Table;
    class Block;

    enum class ColumnType : uint8_t
    {
        TinyInt = 0,
        SmallInt = 1,
        Int = 2,
        BigInt = 3,
        Decimal = 4,
        String = 5,
        UnicodeString = 6,
        Bool = 7,
        DateTime = 8,
        ColumnTypeCount = 9
    };

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