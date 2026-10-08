#include <CoreEngine/DataStorage/Schema/Schemas.h>
#include <CoreEngine/SystemDatabases/SystemCatalog.h>

#include "QueryPipeline/Parsing/Token.h"
#include "Systemic/Functions/StringFunctions.h"

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

            return static_cast<T*>(allocator->AllocateRaw(size, alignof(T)));
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

        struct ColumnOrdinals{
            struct Entry{
                Int _id;
                column_index_t _ordinalPosition;
            };

            DataStructures::StaticArray<Entry, std::numeric_limits<column_index_t>::max() + 1> _entries;
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
        schema_version_t version
    ){
        const auto* __restrict__ tableHeader = tableDefinition._table;

        const UnsignedInt indexesByteCount = [&]{
            //later add coveredColumns and filter size as well if they exist
            return
                RegionBytes<IndexSchema>(tableDefinition._indexes.Size())
                + RegionBytes<IndexKeyColumn>(tableDefinition._indexesColumnsCount);
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
               ._ordinalPosition = i
            });
        }

        auto* columns = AllocateArray<ColumnSchema>(allocator, tableDefinition._columns.Size());
        auto* lowerNames = AllocateArray<LowerNameEntry>(allocator, tableDefinition._columns.Size());
        auto* defaults = AllocateArray<ExpressionSchema>(allocator, tableDefinition._defaults.Size());
        auto* indexes = AllocateArray<IndexSchema>(allocator, tableDefinition._indexes.Size());
        auto* indexesColumns = AllocateArray<IndexKeyColumn>(allocator, tableDefinition._indexesColumnsCount);

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
        for (column_number_t i = 0;i < static_cast<column_number_t>(tableDefinition._indexes.Size()); i++){
            const auto& header = tableDefinition._indexes[i];

            for (const auto& column: header.columns){
                auto* indexColumn = std::construct_at(&indexesColumns[nextIndexColumn++], IndexKeyColumn{
                    ._ordinalPosition = column.,
                    ._isDescending = false
                });
            }

            std::construct_at(&indexes[i], IndexSchema{
               ._name = CopyString<false>(allocator, header.name),
               ._keyColumns = ,
               ._keyTypes = ,
               ._includedColumns = nullptr,
               ._filter = nullptr,
               ._coveredColumnsMask = ,
               ._id = header.id,
               ._keyCount = header.columns.Size(),
               ._includedCount = 0,
               ._treeOrdinalPosition = NONE,
               ._isClustered = header.isClustered,
               ._isUnique = header.isClustered,
               ._isDisabled = header.isDisabled
            });
        }


        schema->_columns = columns;
        schema->_columnsByLowerName = lowerNames;
        schema->_constraints = constraints;
        schema->_identityCount = tableDefinition._identities.Size();
        schema->_name = CopyString<false>(allocator, tableHeader->name);
        schema->_id = tableHeader->id;
        schema->_schemaId = tableHeader->schemaId;
        schema->_version = version;

        return schema.release();
    }
}
