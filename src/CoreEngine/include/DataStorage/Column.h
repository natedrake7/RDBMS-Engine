#pragma once
#include "../DatabaseConstants.h"
#include "../../../Systemic/include/Headers.h"
#include "../Managers/IdentityManager.h"
#include "../Memory/Allocator.h"
#include "../Memory/PersistentAllocator.h"

namespace CoreEngine::StorageTypes{
    class Table;
    class Block;
    class Row;

    struct ColumnHeader{
        Headers::DefaultValuesHeader defaultValue;

        Int id;
        column_index_t columnIndex;
        row_size_t recordSize;
        TinyInt precision;
        TinyInt scale;
        DataType columnType;
    };

    class Column{
        ColumnHeader header;
        IdentityManager identityManager;

        DataTypes::String name;

        Memory::PersistentAllocator _allocator;

        const Table *table;
        bool allowNulls;
        bool isOverflowed;

    public:
        Column(
            const DataTypes::StringView& columnName,
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

        void Destroy()const;

        ~Column();

        [[nodiscard]] const DataTypes::String& GetColumnName() const;

        void SetColumnName(const DataTypes::StringView& otherName);

        [[nodiscard]] DataType Type() const;

        [[nodiscard]] row_size_t Size() const;

        [[nodiscard]] bool IsNullable() const;

        void SetOrdinalPosition(column_index_t columnIndex);

        [[nodiscard]] column_index_t OrdinalPosition() const;

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

        [[nodiscard]] BigInt GenerateIdentityValue(const ::Memory::IAllocator* allocator);

        void UpdateMetadata(const ::Memory::IAllocator* allocator)const;

        [[nodiscard]] bool HasIdentity() const;
    };
}
