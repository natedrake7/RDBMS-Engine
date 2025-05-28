#pragma once
#include <string>
#include "../Constants.h"

namespace Headers {
    struct ColumnHeader;
    struct sysColumn;
}

using namespace std;
using namespace Constants;

namespace DatabaseEngine::StorageTypes
{
    class Table;
    class Block;

    typedef struct ColumnHeader
    {
        ColumnType columnType;
        column_index_t columnIndex;
        row_size_t recordSize;
    } ColumnHeader;

    class Column
    {
        ColumnHeader header;
        std::string name;
        const Table *table;
        bool allowNulls;
        bool isOverflowed;

    protected:
        [[nodiscard]] ColumnType SetColumnType() const;

    public:
        Column(const std::string& columnName, const ColumnType& type, const row_size_t&  recordSize, const column_index_t& index, const bool& allowNulls);

        Column(const Headers::sysColumn& header, const column_index_t& tablePos , const Table* table);

        explicit Column(const Headers::ColumnHeader& masterDbHeader, const Table *table);

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

        [[nodiscard]] bool isColumnOverflowed() const;

        void SetIsOverflowed(const bool &isOverflowed);
    };
}