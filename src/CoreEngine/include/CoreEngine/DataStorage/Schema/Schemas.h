#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataTypes/StringView.h>

namespace CoreEngine::Catalog{
    struct DefaultValuesHeader;
}

namespace CoreEngine::Schemas{
    enum ConstraintType: UnsignedTinyInt {
        PrimaryKey = 0,
        ForeignKey = 1,
        Unique = 2,
        IndexKey = 3,
        Check = 4,
        NotNull = 5
    };

    struct IdentitySchema{
        Int seedValue;
        Int increment;

        IdentitySchema() = default;
    };

    struct ColumnSchema{
        DataTypes::StringView _name;

        Int _id;
        row_size_t _recordSize;
        column_index_t _ordinalPosition;
        DataType _type;
        TinyInt _precision;
        TinyInt _scale;
        bool _isNullable;
        bool _isSystem;

        const Catalog::DefaultValuesHeader* _defaultValue;
        const IdentitySchema* _identity;
    };

    struct IndexSchema{
        DataTypes::StringView _name;
        Int _id;
        UnsignedTinyInt _treeOrdinalPosition;
        bool _isClustered;
        bool _isUnique;

        const column_index_t* keyColumns;
        UnsignedTinyInt _keyCount;
    };

    struct ConstraintSchema{
        DataTypes::StringView _name;
        Int _id;
        ConstraintType _constraintType;
        UnsignedTinyInt _indexOrdinalPosition;

        column_index_t* _keyColumns;
        UnsignedTinyInt _keyCount;
    };

    struct TableSchema{
        DataTypes::StringView _name;
        DataTypes::StringView _schemaName;

        const ColumnSchema* _columns;

        Int _id;
        schema_version_t _version;
        column_number_t _columnCount;

        // const IndexSchema* _indexes;
        // UnsignedTinyInt _indexCount;
        //
        // const ConstraintSchema* _constraints;
        // column_number_t _constraintCount;
    };

    static_assert(std::is_trivially_destructible_v<ColumnSchema>);
    static_assert(std::is_trivially_destructible_v<TableSchema>);
}
