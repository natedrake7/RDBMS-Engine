#include <CoreEngine/DataStorage/Schema/Schemas.h>

#include <CoreEngine/SystemDatabases/CatalogHeaders.h>
#include <Systemic/DataStructures/PolymorphicArray.h>
#include <Systemic/Functions/StringFunctions.h>

namespace CoreEngine::Schemas{
    namespace{
        template<typename T>
        constexpr std::size_t RegionBytes(const std::size_t count){
            return count == 0
                ? 0
                : count * sizeof(T) + alignof(T) - 1;
        }

        template<typename T>
        T* AllocateArray(const ::Memory::IAllocator* allocator, const std::size_t size){
            if (size == 0)
                return nullptr;

            return allocator->AllocateRaw<T>(size * sizeof(T));
        }

        template<bool LowerCase>
        DataTypes::StringView CopyString(const ::Memory::IAllocator* allocator, const DataTypes::String& text){
            const auto size = text.Size();

            if (size == 0)
                return DataTypes::StringView();

            auto* destination =  allocator->AllocateRaw<char>(size);
            const auto* source = text.Data();

            for (data_size_t i = 0; i < size; i++){
                if constexpr (LowerCase)
                    destination[i] = DataTypes::StringView::Lower(source[i]);
                else
                    destination[i] = source[i];
            }

            return DataTypes::StringView(destination, size);
        }

        class ColumnOrdinals{
            struct Entry{
                Int _id;
                column_index_t _ordinalPosition;
            };

            static constexpr column_number_t ENTRIES_SIZE = std::numeric_limits<column_index_t>::max() + 1;

            DataStructures::StaticArray<Entry, ENTRIES_SIZE> _entries;

        public:
            static constexpr column_number_t NOT_FOUND = ENTRIES_SIZE;

            explicit ColumnOrdinals(const DataStructures::PolymorphicArray<Catalog::ColumnHeader>& headers){
                for (const auto& header : headers){
                    this->_entries.Push(Entry{
                        ._id = header.id,
                        ._ordinalPosition = static_cast<column_index_t>(header.ordinalPosition)
                    });
                }

                const auto sortFunction = [](const Entry& lhs, const Entry& rhs){
                    return lhs._id < rhs._id;
                };

                if (!std::ranges::is_sorted(this->_entries.begin(), this->_entries.end(), sortFunction))
                    std::ranges::sort(this->_entries.begin(), this->_entries.end(), sortFunction);
            }

            [[nodiscard]] column_index_t GetOrdinal(const Int columnId){
                const auto it = std::lower_bound(this->_entries.begin(), this->_entries.end(), columnId, [](const Entry& lhs, const Int id){
                    return lhs._id < id;
                });

                return it == this->_entries.end()
                    ? NOT_FOUND
                    : it->_ordinalPosition;
            }

        };
    }

    const ColumnSchema* TableSchema::FindColumnByName(const DataTypes::StringView& name) const{
        for (column_number_t i = 0;i < this->_columnCount; i++){
            const auto& [columnName, ordinalPosition] = this->_columnsByLowerName[i];
            if (DataTypes::StringView::Compare<StringComparisonType::EqualsIgnoreCase, DataTypes::StringView, DataTypes::StringView>(
                name,
                columnName
            )){
                return &this->_columns[ordinalPosition];
            }
        }

        return nullptr;
    }
    TableSchema* TableSchema::Build(
        const Catalog::TableDefinition& tableDefinition,
        const schema_version_t version
    ){
        const auto* __restrict__ tableHeader = tableDefinition._table;

        ColumnOrdinals columnOrdinals(tableDefinition._columns);

        const UnsignedInt indexesByteCount = [&]{
            //later add coveredColumns and filter size as well if they exist
            return
                RegionBytes<IndexSchema>(tableDefinition._indexes.Size())
                + RegionBytes<IndexKeyColumn>(tableDefinition._indexesColumnsCount)
                + RegionBytes<DataType>(tableDefinition._indexesColumnsCount);
        }();

        const UnsignedInt constraintsByteCount = [&]{
            //add later check predicate size(sizeof expression)
            return RegionBytes<ConstraintSchema>(tableDefinition._constraints.Size())
            + RegionBytes<column_index_t>(tableDefinition._constraintsColumnsCount);
        }();

        //count first objects data sizes
        UnsignedInt arenaSize =
            RegionBytes<ColumnSchema>(tableDefinition._columns.Size())
            + RegionBytes<IdentitySchema>(tableDefinition._identities.Size())
            + RegionBytes<ExpressionSchema>(tableDefinition._defaults.Size())
            + constraintsByteCount
            + indexesByteCount;

        //count strings bytes
        arenaSize += tableHeader->name.Size();
        for (const auto& column : tableDefinition._columns)
            arenaSize += 2 * column.name.Size();
        for (const auto& defaultValue : tableDefinition._defaults)
            arenaSize += defaultValue.value.Size();
        for (const auto& constraint: tableDefinition._constraints)
            arenaSize += constraint.name.Size();
        for (const auto& index : tableDefinition._indexes)
            arenaSize += index.name.Size();

        auto schema = std::make_unique<TableSchema>(arenaSize);

        const auto* __restrict__ allocator = &schema->_allocator;

        auto* identities = AllocateArray<IdentitySchema>(allocator, tableDefinition._identities.Size());
        for (UnsignedTinyInt i = 0; i < static_cast<UnsignedTinyInt>(tableDefinition._identities.Size()); i++){
            const auto& header = tableDefinition._identities[i];
            std::construct_at(&identities[i], IdentitySchema{
               ._seedValue = header.seedValue,
               ._increment = header.increment,
               ._lastValue = header.lastValue,
                ._cacheBlock = header.cacheBlock,
               ._slot = i
            });
        }

        auto* columns = AllocateArray<ColumnSchema>(allocator, tableDefinition._columns.Size());
        auto* lowerNames = AllocateArray<LowerNameEntry>(allocator, tableDefinition._columns.Size());
        auto* defaults = AllocateArray<ExpressionSchema>(allocator, tableDefinition._defaults.Size());
        auto* indexes = AllocateArray<IndexSchema>(allocator, tableDefinition._indexes.Size());
        auto* indexesColumns = AllocateArray<IndexKeyColumn>(allocator, tableDefinition._indexesColumnsCount);
        auto* indexesTypes = AllocateArray<DataType>(allocator, tableDefinition._indexesColumnsCount);

        auto* constraints = AllocateArray<ConstraintSchema>(allocator, tableDefinition._constraints.Size());
        auto* constraintsColumns = AllocateArray<column_index_t>(allocator, tableDefinition._constraintsColumnsCount);

        auto nextDefault = 0;

        for (column_number_t i = 0;i < static_cast<column_number_t>(tableDefinition._columns.Size()); i++){
            const auto& header = tableDefinition._columns[i];
            assert(header.ordinalPosition == i && "TableSchema::Build: columns must be sorted by ordinal position");

            auto* column = std::construct_at(&columns[i]);

            column->_name = CopyString<false>(allocator, header.name);
            column->_id = header.id;
            column->_recordSize = header.recordSize;
            column->_ordinalPosition = header.ordinalPosition;
            column->_type = static_cast<DataType>(header.dataType);
            column->_precision = header.precision;
            column->_scale = header.scale;
            column->_isNullable = header.isNullable;
            column->_isSystem = header.isSystem;
            column->_computedKind = ComputedKind::None;

            for (auto j = 0;j < tableDefinition._identities.Size(); j++){
                const auto& identity = tableDefinition._identities[j];
                if (identity.columnId == header.id){
                    column->_identity = &identities[j];
                    break;
                }
            }

            for (auto j = 0; j < tableDefinition._defaults.Size(); j++){
                const auto& defaultValue = tableDefinition._defaults[j];
                if (defaultValue.columnId == header.id){
                    auto* defaultExpression = std::construct_at(&defaults[nextDefault++]);

                    defaultExpression->_definition = CopyString<false>(allocator, defaultValue.value);
                    defaultExpression->_resultType = column->_type;
                    defaultExpression->_volatility = Volatility::Volatile;
                    defaultExpression->_definitionKind = DefinitionKind::Default;

                    column->_default = defaultExpression;
                    break;
                }
            }

            std::construct_at(&lowerNames[i], LowerNameEntry{
                ._name = CopyString<true>(allocator, header.name),
                ._ordinalPosition = column->_ordinalPosition,
            });
        }

        //sort lowercased columns
        std::ranges::sort(lowerNames, lowerNames + tableDefinition._columns.Size(), [](const LowerNameEntry& lhs, const LowerNameEntry& rhs){
            return Comparators::Compare(lhs._name, rhs._name) == Comparators::Comparator::Less;
        });

        std::size_t nextIndexColumn = 0;
        std::size_t nextDataTypeIndex = 0;
        Int clusteredIndexOrdinal = NONE;
        for (Int i = 0;i < tableDefinition._indexes.Size(); i++){
            const auto& header = tableDefinition._indexes[i];

            auto* index = std::construct_at(&indexes[i]);
            index->_name = CopyString<false>(allocator, header.name);
            index->_id = header.id;
            index->_keyCount = header.columns.Size();
            index->_includedCount = 0;
            index->_treeOrdinalPosition = NONE;
            index->_isClustered = header.isClustered;
            index->_isUnique = header.isClustered;
            index->_isDisabled = header.isDisabled;

            if (index->_isClustered)
                clusteredIndexOrdinal = i;

            for (Int j = 0; j < header.columns.Size(); j++){
                const auto& column = header.columns[j];
                const auto ordinalPosition = columnOrdinals.GetOrdinal(column.columnId);

                assert(
                    column.ordinalPosition == j
                    && "TableSchema::Build: Index Columns must be ordered"
                );
                assert(
                    ordinalPosition != ColumnOrdinals::NOT_FOUND
                    && "TableSchema::Build: Column Id given in index columns build is out of bounds"
                );

                index->_coveredColumnsMask.Set(ordinalPosition);
                const auto* indexColumn = std::construct_at(&indexesColumns[nextIndexColumn++], IndexKeyColumn{
                    ._ordinalPosition = ordinalPosition,
                    ._isDescending = false
                });

                auto* dataType = &indexesTypes[nextDataTypeIndex++];
                *dataType = columns[ordinalPosition]._type;

                if (j == 0){
                    index->_keyColumns = indexColumn;
                    index->_keyTypes = dataType;
                }
            }
        }

        const auto findIndexById = [&](const Int indexId){
            if (indexId == INVALID_INDEX_ID)
                return INVALID_INDEX_ID;

            for (Int i = 0;i < tableDefinition._indexes.Size(); i++){
                if (indexes[i]._id == indexId)
                    return i;
            }

            std::unreachable();
        };

        std::size_t nextConstraintColumn = 0;
        Int primaryKeyOrdinal = NONE;
        for (Int i = 0;i < tableDefinition._constraints.Size(); i++){
            const auto& header = tableDefinition._constraints[i];
            auto* constraint = std::construct_at(&constraints[i]);
            constraint->_id = header.constraintId;
            constraint->_name = CopyString<false>(allocator, header.name);
            constraint->_type = header.type;
            constraint->_indexOrdinalPosition = findIndexById(header.indexId);
            constraint->_checkPredicate = nullptr;
            constraint->_keyCount = static_cast<UnsignedTinyInt>(header.columns.Size());

            if (constraint->_type == ConstraintType::PrimaryKey)
                primaryKeyOrdinal = i;

            for (Int j = 0;j < header.columns.Size(); j++){
                const auto& column = header.columns[j];
                auto* constraintColumn = &constraintsColumns[nextConstraintColumn++];
                 *constraintColumn = columnOrdinals.GetOrdinal(column.columnId);

                if (j == 0)
                    constraint->_keyColumns = constraintColumn;
            }
        }

        schema->_id = tableHeader->id;
        schema->_schemaId = tableHeader->schemaId;
        schema->_ordinalPosition = static_cast<UnsignedSmallInt>(tableHeader->ordinalPosition);
        schema->_version = version;
        schema->_name = CopyString<false>(allocator, tableHeader->name);

        schema->_columns = columns;
        schema->_columnsByLowerName = lowerNames;
        schema->_constraints = constraints;
        schema->_indexes = indexes;
        schema->_identities = identities;
        schema->_storedComputedOrder = nullptr;

        schema->_primaryKeyOrdinalPosition = static_cast<UnsignedTinyInt>(primaryKeyOrdinal);
        schema->_clusteredIndexOrdinalPosition = static_cast<UnsignedTinyInt>(clusteredIndexOrdinal);
        schema->_checkConstraintsCount = tableDefinition._constraints.Size();
        schema->_columnCount = tableDefinition._columns.Size();
        schema->_indexesCount = tableDefinition._indexes.Size();
        schema->_identitiesCount = tableDefinition._identities.Size();
        schema->_storedComputedOrderCount = 0;

        return schema.release();
    }
}
