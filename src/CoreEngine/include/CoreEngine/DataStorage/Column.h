#pragma once
#include <CoreEngine/SystemDatabases/CatalogHeaders.h>
#include <CoreEngine/Managers/IdentityManager.h>
#include <CoreEngine/Memory/Allocator.h>
#include <CoreEngine/Memory/PersistentAllocator.h>

namespace CoreEngine::StorageTypes{
    class Table;
    class Block;

    struct Columns{
        ColumnSchema* _columns;
        column_index_t _columnCount;
    };

    struct ColumnHeaders{
        Catalog::ColumnHeader columnHeader;
        Catalog::DefaultValuesHeader defaultValue;
    };
    class Column{
        ColumnHeaders _headers;
        IdentityManager _identityManager;

        DataTypes::String _name;

        bool _allowNulls;

    public:
        explicit Column(
            const Catalog::ColumnHeader& catalogHeader,
            const Table *table
        );

        [[nodiscard]] const DataTypes::String& GetColumnName() const;

        [[nodiscard]] DataTypes::StringView GetColumnNameView() const;

        void SetColumnName(const DataTypes::StringView& otherName);

        [[nodiscard]] DataType Type() const;

        [[nodiscard]] row_size_t Size() const;

        [[nodiscard]] bool IsNullable() const;

        void SetOrdinalPosition(column_index_t ordinalPosition);

        [[nodiscard]] column_index_t OrdinalPosition() const;

        [[nodiscard]] const ColumnHeader& GetColumnHeader() const;

        [[nodiscard]] Int GetColumnId() const;

        void SetColumnId(Int columnId);

        void SetIdentityManagerIds(Int tableId);

        [[nodiscard]] const Catalog::IdentityColumnsHeader&  GetIdentity()const;

        void SetIdentity(const Catalog::IdentityColumnsHeader &identity);

        void SetDefaultValue(const Catalog::DefaultValuesHeader &defaultValue);

        [[nodiscard]] const Catalog::DefaultValuesHeader &GetDefaultValue() const;

        template<DataTypes::IsInteger T>
        [[nodiscard]] T GenerateIdentityValue(const ::Memory::IAllocator* allocator){
            return this->_identityManager.Generate<T>(allocator);
        }

        template<DataTypes::IsInteger T>
        [[nodiscard]] T ReserveIdentityRange(const ::Memory::IAllocator* allocator, const T rowCount){
            return this->_identityManager.ReserveRange<T>(allocator, rowCount);
        }

        [[nodiscard]] Int GetIncrement() const;

       [[nodiscard]] Value GenerateIdentityValue(const ::Memory::IAllocator* allocator);

        void UpdateMetadata(const ::Memory::IAllocator* allocator)const;

        [[nodiscard]] bool HasIdentity() const;
    };
}
