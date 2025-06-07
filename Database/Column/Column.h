#pragma once
#include <string>
#include "../Constants.h"
#include "../../QueryPipeline/Statements/Statements.h"

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
        int32_t id;
        ColumnType columnType;
        column_index_t columnIndex;
        row_size_t recordSize;

        Headers::IdentityColumnsHeader identity;
        int32_t identityStartingValue;
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

        [[nodiscard]] const int32_t& GetColumnId() const;

        void SetColumnId(const int32_t &columnId);

        [[nodiscard]] Headers::IdentityColumnsHeader&  GetIdentity();

        [[nodiscard]] const int32_t& GetIdentityStartingValue() const;

        void SetIdentityStartingValue(const int32_t& identityStartingValue);

        void SetIdentity(const Headers::IdentityColumnsHeader &identity);

        void SetIsOverflowed(const bool &isOverflowed);
    };
}