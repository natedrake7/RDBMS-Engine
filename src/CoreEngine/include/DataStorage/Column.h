#pragma once
#include "../../../Systemic/include/Headers.h"
#include "../Managers/IdentityManager.h"
#include "../Memory/Allocator.h"
#include "../Memory/PersistentAllocator.h"

namespace CoreEngine::StorageTypes{
    class Table;
    class Block;

    struct ColumnHeader{
        Headers::DefaultValuesHeader defaultValue;

        Int _id;
        column_index_t columnIndex;
        row_size_t recordSize;
        TinyInt precision;
        TinyInt scale;
        DataType columnType;
    };

    class Column{
        ColumnHeader _header;
        IdentityManager _identityManager;

        DataTypes::String _name;

        Memory::PersistentAllocator _allocator;

        const Table* _table;
        bool _allowNulls;
        bool _isLob;
        bool _isOverflowed;

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

        [[nodiscard]] DataTypes::StringView GetColumnNameView() const;

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

        void SetIsLob(bool isLob);

        template<DataTypes::IsInteger T>
        [[nodiscard]] T GenerateIdentityValue(const ::Memory::IAllocator* allocator){
            return this->_identityManager.Generate<T>(allocator);
        }

       [[nodiscard]] Value GenerateIdentityValue(const ::Memory::IAllocator* allocator);

        void UpdateMetadata(const ::Memory::IAllocator* allocator)const;

        [[nodiscard]] bool HasIdentity() const;
    };
}
