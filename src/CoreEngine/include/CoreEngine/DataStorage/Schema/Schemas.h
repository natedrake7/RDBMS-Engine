#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataTypes/StringView.h>
#include <CoreEngine/Memory/PersistentAllocator.h>
#include <Systemic/DataTypes/PackedWord.h>

#include <limits>
#include <type_traits>
#include <Systemic/Reflection/Enum.h>

#include "Systemic/Constants.h"

namespace Expressions{
    class Expression;
}

namespace CoreEngine::Catalog{
    struct TableDefinition;
    struct TableHeader;
    struct DefaultValuesHeader;
}

namespace CoreEngine::Schemas{
    enum class Volatility: UInt8_t{
        Immutable = 0,
        Stable = 1,
        Volatile = 2,
    };

    inline static constexpr auto VOLATILITY_COUNT = Reflection::EnumCount<Volatility>;

    enum class DefinitionKind: UInt8_t{
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

    enum class ComputedKind: UInt8_t{
        None = 0,
        Virtual = 1,
        Stored = 2
    };

    inline static constexpr auto COMPUTED_COUNT = Reflection::EnumCount<ComputedKind>;

    enum class ConstraintType: UInt8_t {
        PrimaryKey = 0,
        ForeignKey = 1,
        Unique = 2,
        IndexKey = 3,
        Check = 4,
        NotNull = 5
    };

    inline static constexpr auto CONSTRAINT_TYPE_COUNT = Reflection::EnumCount<ConstraintType>;

    struct IdentitySchema{
        Int64_t _seedValue;
        Int64_t _increment;
        Int64_t _lastValue;
        Int32_t _cacheBlock;

        UInt8_t _slot;
    };
    static_assert(std::is_trivially_destructible_v<IdentitySchema>);

    struct ColumnSchema{
        DataTypes::StringView _name;

        const ExpressionSchema* _default;
        const ExpressionSchema* _computed;
        const IdentitySchema* _identity;

        Int32_t _id;
        row_size_t _recordSize;
        column_index_t _ordinalPosition;
        DataType _type;
        Int8_t _precision, _scale;
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
        static constexpr UInt32_t BITS_COUNT = std::numeric_limits<column_index_t>::max() + 1;
        static constexpr UInt32_t WORDS_COUNT = EngineBitmap::WordsFor(BITS_COUNT);

        UInt64_t _words[WORDS_COUNT]{};

        constexpr void Set(const column_index_t ordinalPosition){
            EngineBitmap::OrBitmapBit(this->_words, ordinalPosition, true);
        }

        constexpr void Clear(const column_index_t ordinalPosition){
            EngineBitmap::SetBitmapBit(this->_words, ordinalPosition, false);
        }

        [[nodiscard]] constexpr bool HasValue(const column_index_t ordinalPosition) const{
            return EngineBitmap::GetBitmapBit(this->_words, ordinalPosition);
        }

        [[nodiscard]] constexpr bool Intersects(const ColumnMask& other) const{
            UInt64_t any = 0;
            for (UInt32_t i = 0; i < WORDS_COUNT; i++)
                any |= this->_words[i] & other._words[i];
            return any != 0;
        }

        [[nodiscard]] constexpr bool Empty() const{
            UInt64_t any = 0;
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

        Int32_t _id;
        UInt8_t _keyCount;
        UInt8_t _includedCount;
        UInt8_t _treeOrdinalPosition;

        bool _isClustered;
        bool _isUnique;
        bool _isDisabled;
    };
    static_assert(std::is_trivially_destructible_v<IndexSchema>);

    struct ConstraintSchema{
        static constexpr UInt8_t NO_INDEX = std::numeric_limits<UInt8_t>::max();

        DataTypes::StringView _name;
        Int32_t _id;
        ConstraintType _type;
        UInt8_t _indexOrdinalPosition;

        const column_index_t* _keyColumns;
        UInt8_t _keyCount;

        const Expressions::Expression* _checkPredicate;

        ConstraintSchema()
            :   _id(0), _type(),
                _indexOrdinalPosition(NO_INDEX), _keyColumns(nullptr),
                _keyCount(0), _checkPredicate(nullptr){}
    };
    static_assert(std::is_trivially_destructible_v<ConstraintSchema>);

    class TableSchema{
        Memory::PersistentAllocator _allocator;

    public:
        static constexpr UInt8_t NONE = std::numeric_limits<UInt8_t>::max();

        const ColumnSchema* _columns;
        const LowerNameEntry* _columnsByLowerName;

        const IdentitySchema* _identities;

        const IndexSchema* _indexes;
        const ConstraintSchema* _constraints;
        const column_index_t* _storedComputedOrder;

        const UInt8_t* _checkConstraints;
        DataTypes::StringView _lowerName;

        Int32_t _id;
        Int32_t _schemaId;
        UInt16_t _ordinalPosition;
        schema_version_t _version;
        column_number_t _columnCount;

        UInt8_t _indexesCount;
        UInt8_t _constraintsCount;
        UInt8_t _storedComputedOrderCount;
        UInt8_t _checkConstraintsCount;
        UInt8_t _identitiesCount;

        UInt8_t _clusteredIndexOrdinalPosition;
        UInt8_t _primaryKeyOrdinalPosition;

        TableSchema()
            : _columns(nullptr), _columnsByLowerName(nullptr), _identities(nullptr),
              _indexes(nullptr), _constraints(nullptr),
              _storedComputedOrder(nullptr), _checkConstraints(nullptr), _id(0), _schemaId(INVALID_SCHEMA_ID),
              _ordinalPosition(0),
              _version(0), _columnCount(0),
              _indexesCount(0), _constraintsCount(0),
              _storedComputedOrderCount(0), _checkConstraintsCount(0), _identitiesCount(0),
              _clusteredIndexOrdinalPosition(NONE), _primaryKeyOrdinalPosition(NONE)
        {}

        explicit TableSchema(const UInt32_t size)
            : _allocator(size), _columns(nullptr), _columnsByLowerName(nullptr), _identities(nullptr),
              _indexes(nullptr), _constraints(nullptr),
              _storedComputedOrder(nullptr), _checkConstraints(nullptr), _id(0), _schemaId(INVALID_SCHEMA_ID),
              _ordinalPosition(0),
              _version(0), _columnCount(0),
              _indexesCount(0), _constraintsCount(0),
              _storedComputedOrderCount(0), _checkConstraintsCount(0), _identitiesCount(0),
              _clusteredIndexOrdinalPosition(NONE), _primaryKeyOrdinalPosition(NONE)
        {}

        TableSchema(const TableSchema&) = delete;
        TableSchema& operator=(const TableSchema&) = delete;

        TableSchema(TableSchema&&) = delete;
        TableSchema& operator=(TableSchema&&) = delete;

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

        [[nodiscard]] UInt8_t NonClusteredIndexesStart() const{ return this->IsClustered(); }
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
