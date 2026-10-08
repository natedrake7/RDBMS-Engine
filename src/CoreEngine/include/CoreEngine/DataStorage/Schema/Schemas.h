#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataTypes/StringView.h>
#include <CoreEngine/Memory/PersistentAllocator.h>
#include <Systemic/DataTypes/PackedWord.h>

#include <CoreEngine/SystemDatabases/CatalogHeaders.h>

#include <limits>
#include <type_traits>
#include <Systemic/Reflection/Enum.h>

namespace Expressions{
    class Expression;
}

namespace CoreEngine::Catalog{
    struct TableHeader;
    struct DefaultValuesHeader;
}

namespace CoreEngine::Schemas{
    enum class Volatility: UnsignedTinyInt{
        Immutable = 0,
        Stable = 1,
        Volatile = 2,
    };

    inline static constexpr auto VOLATILITY_COUNT = Reflection::EnumCount<Volatility>;

    enum class DefinitionKind: UnsignedTinyInt{
        Default = 0,
        Check = 1,
        Computed = 2
    };

    inline static constexpr auto DEFINITION_COUNT = Reflection::EnumCount<DefinitionKind>;

    struct ExpressionSchema{
        const Expressions::Expression* _parsedTree;

        DataTypes::StringView _definition;

        const column_index_t* _referencedColumns;
        column_number_t _referencedColumnCount;

        DataType _resultType;
        Volatility _volatility;
        DefinitionKind _definitionKind;
    };

    enum class ComputedKind: UnsignedTinyInt{
        None = 0,
        Virtual = 1,
        Stored = 2
    };

    inline static constexpr auto COMPUTED_COUNT = Reflection::EnumCount<ComputedKind>;

    enum class ConstraintType: UnsignedTinyInt {
        PrimaryKey = 0,
        ForeignKey = 1,
        Unique = 2,
        IndexKey = 3,
        Check = 4,
        NotNull = 5
    };

    inline static constexpr auto CONSTRAINT_TYPE_COUNT = Reflection::EnumCount<ConstraintType>;

    struct IdentitySchema{
        BigInt _seedValue;
        BigInt _increment;
        BigInt _lastValue;

        UnsignedTinyInt _ordinalPosition;
    };
    static_assert(std::is_trivially_destructible_v<IdentitySchema>);

    struct ColumnSchema{
        DataTypes::StringView _name;

        const ExpressionSchema* _default;
        const ExpressionSchema* _computed;
        const IdentitySchema* _identity;

        Int _id;
        row_size_t _recordSize;
        column_index_t _ordinalPosition;
        DataType _type;
        TinyInt _precision, _scale;
        bool _isNullable, _isSystem;

        ComputedKind _computedKind;
    };
    static_assert(std::is_trivially_destructible_v<ColumnSchema>);

    struct LowerNameEntry{
        DataTypes::StringView _name;
        column_index_t _ordinalPosition;
    };
    static_assert(std::is_trivially_destructible_v<LowerNameEntry>);

    struct ColumnMask{
        static constexpr UnsignedInt BITS_COUNT = std::numeric_limits<column_index_t>::max() + 1;
        static constexpr UnsignedInt WORDS_COUNT = EngineBitmap::WordsFor(BITS_COUNT);

        UnsignedBigInt _words[WORDS_COUNT]{};

        constexpr void Set(const column_index_t ordinalPosition){
            EngineBitmap::OrBitmapBit(this->_words, ordinalPosition, true);
        }

        constexpr void Clear(const column_index_t ordinalPosition){
            EngineBitmap::SetBitmapBit(this->_words, ordinalPosition, false);
        }

        [[nodiscard]] constexpr bool Test(const column_index_t ordinalPosition) const{
            return EngineBitmap::GetBitmapBit(this->_words, ordinalPosition);
        }

        [[nodiscard]] constexpr bool Intersects(const ColumnMask& other) const{
            UnsignedBigInt any = 0;
            for (UnsignedInt i = 0; i < WORDS_COUNT; i++)
                any |= this->_words[i] & other._words[i];
            return any != 0;
        }

        [[nodiscard]] constexpr bool Empty() const{
            UnsignedBigInt any = 0;
            for (const auto word : this->_words)
                any |= word;
            return any == 0;
        }
    };
    static_assert(ColumnMask::BITS_COUNT == std::numeric_limits<column_index_t>::max() + 1);
    static_assert(ColumnMask::WORDS_COUNT == 4);
    static_assert(std::is_trivially_destructible_v<ColumnMask>);

    struct IndexKeyColumn{
        column_index_t _ordinalPosition;
        bool _isDescending;
    };
    static_assert(std::is_trivially_destructible_v<IndexKeyColumn>);

    struct IndexSchema{
        DataTypes::StringView _name;
        const IndexKeyColumn* _keyColumns;

        const DataType* _keyTypes;
        const column_index_t* _includedColumns;

        const ExpressionSchema* _filter;
        ColumnMask _coveredColumnsMask;

        Int _id;
        UnsignedTinyInt _keyCount;
        UnsignedTinyInt _includedCount;
        UnsignedTinyInt _treeOrdinalPosition;

        bool _isClustered;
        bool _isUnique;
        bool _isDisabled;
    };
    static_assert(std::is_trivially_destructible_v<IndexSchema>);

    struct ConstraintSchema{
        static constexpr UnsignedTinyInt NO_INDEX = std::numeric_limits<UnsignedTinyInt>::max();

        DataTypes::StringView _name;
        Int _id;
        ConstraintType _constraintType;
        UnsignedTinyInt _indexOrdinalPosition;

        const column_index_t* _keyColumns;
        UnsignedTinyInt _keyCount;

        const Expressions::Expression* _checkPredicate;

        ConstraintSchema()
            :   _id(0), _constraintType(),
                _indexOrdinalPosition(NO_INDEX), _keyColumns(nullptr),
                _keyCount(0), _checkPredicate(nullptr){}
    };
    static_assert(std::is_trivially_destructible_v<ConstraintSchema>);

    class TableSchema{
        Memory::PersistentAllocator _allocator;

    public:
        static constexpr UnsignedTinyInt NONE = std::numeric_limits<UnsignedTinyInt>::max();

        const ColumnSchema* _columns;
        const LowerNameEntry* _columnsByLowerName;

        const IndexSchema* _indexes;
        const ConstraintSchema* _constraints;
        const column_index_t* _storedComputedOrder;

        const UnsignedTinyInt* _checkConstraints;
        DataTypes::StringView _name;

        Int _id;
        Int _schemaId;
        schema_version_t _version;
        column_number_t _columnCount;

        UnsignedTinyInt _indexesCount;
        UnsignedTinyInt _constraintsCount;
        UnsignedTinyInt _storedComputedOrderCount;
        UnsignedTinyInt _checkConstraintsCount;
        UnsignedTinyInt _identityCount;

        UnsignedTinyInt _clusteredIndexOrdinalPosition;
        UnsignedTinyInt _primaryKeyOrdinalPosition;

        TableSchema()
            : _columns(nullptr), _columnsByLowerName(nullptr),
              _indexes(nullptr), _constraints(nullptr),
              _storedComputedOrder(nullptr), _checkConstraints(nullptr), _id(0), _schemaId(INVALID_SCHEMA_ID),
              _version(0), _columnCount(0),
              _indexesCount(0), _constraintsCount(0),
              _storedComputedOrderCount(0), _checkConstraintsCount(0), _identityCount(0),
              _clusteredIndexOrdinalPosition(NONE), _primaryKeyOrdinalPosition(NONE)
        {}

        explicit TableSchema(const UnsignedInt size)
            : _allocator(size), _columns(nullptr), _columnsByLowerName(nullptr),
              _indexes(nullptr), _constraints(nullptr),
              _storedComputedOrder(nullptr), _checkConstraints(nullptr), _id(0), _schemaId(INVALID_SCHEMA_ID),
              _version(0), _columnCount(0),
              _indexesCount(0), _constraintsCount(0),
              _storedComputedOrderCount(0), _checkConstraintsCount(0), _identityCount(0),
              _clusteredIndexOrdinalPosition(NONE), _primaryKeyOrdinalPosition(NONE){}

        TableSchema(const TableSchema&) = delete;
        TableSchema& operator=(const TableSchema&) = delete;

        TableSchema(TableSchema&&) = default;
        TableSchema& operator=(TableSchema&&) = default;

        ~TableSchema(){
            this->_allocator.Release();
        }

        [[nodiscard]] const ColumnSchema* FindColumnByName(const DataTypes::StringView& name) const;
        [[nodiscard]] bool IsClustered() const{ return this->_clusteredIndexOrdinalPosition != NONE; }
        [[nodiscard]] const IndexSchema* ClusteredIndex() const{
            return this->IsClustered()
                ? &this->_indexes[this->_clusteredIndexOrdinalPosition]
                : nullptr;
        }

        [[nodiscard]] UnsignedTinyInt NonClusteredIndexesStart() const{ return this->IsClustered(); }
        [[nodiscard]] bool HasPrimaryKey() const{ return this->_primaryKeyOrdinalPosition != NONE; }
        [[nodiscard]] const ConstraintSchema* PrimaryKey() const{
            return this->HasPrimaryKey()
                ? &this->_constraints[this->_primaryKeyOrdinalPosition]
                : nullptr;
        }

        [[nodiscard]] const IndexSchema* IndexOf(const ConstraintSchema* constraint)const{
            return constraint->_indexOrdinalPosition == ConstraintSchema::NO_INDEX
                ? nullptr
                : &this->_indexes[constraint->_indexOrdinalPosition];
        }

        static TableSchema* Build(
            const Catalog::TableDefinition& tableDefinition,
            schema_version_t version
        );
        static void Destroy(const TableSchema* tableSchema){
            delete tableSchema;
        }
    };
}
