#include <CoreEngine/SystemDatabases/CatalogSchema.h>
#include <CoreEngine/SystemDatabases/CatalogHeaders.h>
#include <Systemic/Memory/IAllocator.h>

namespace CoreEngine{
    Catalog::TableDefinition SystemTableDefinition(
        const ::Memory::IAllocator* allocator,
        const SystemTable& table
    ){
        static constexpr Int PRIMARY_KEY_INDEX_ID = 1;
        static constexpr Int PRIMARY_KEY_CONSTRAINT_ID = 1;

        const auto stringCopy = [&](const DataTypes::StringView& view){
            return DataTypes::String(view, allocator);
        };

        auto* header = allocator->Allocate<Catalog::TableHeader>();
        header->databaseId = Constants::SYSTEM_CATALOG_ID;
        header->id = table.id;
        header->schemaId = Constants::SYSTEM_SCHEMA_ID;
        header->name = stringCopy(table.name);
        header->ordinalPosition = table.ordinal;
        header->isSystem = true;

        const auto columnCount = table.columns.size();

        Catalog::TableDefinition definition;
        definition._table = header;
        definition._columns = DataStructures::PolymorphicArray<Catalog::ColumnHeader>(allocator, static_cast<Int>(columnCount));
        definition._defaults.SetAllocator(allocator);
        definition._identities.SetAllocator(allocator);
        definition._indexes = DataStructures::PolymorphicArray<Catalog::IndexHeader>(allocator, 1);
        definition._constraints = DataStructures::PolymorphicArray<Catalog::ConstraintsHeader>(allocator, 1);

        for (column_index_t ordinal = 0; ordinal < static_cast<column_index_t>(table.columns.size()); ++ordinal){
            const auto& source = table.columns[ordinal];
            const auto columnId = SystemColumnId(table.id, ordinal);
            const auto fixedSize = DataTypes::FixedColumnSize(source.type);

            Catalog::ColumnHeader column;
            column.tableId = table.id;
            column.id = columnId;
            column.name = stringCopy(source.name);
            column.dataType = static_cast<UnsignedTinyInt>(source.type);
            column.recordSize = fixedSize != 0 ? fixedSize : source.size;
            column.isNullable = source.nullable;
            column.ordinalPosition = ordinal;
            column.isSystem = true;
            definition._columns.Push(std::move(column));

            if (!source.defaultValue.Empty()){
                Catalog::DefaultValuesHeader value;
                value.columnId = columnId;
                value.value = stringCopy(source.defaultValue);
                definition._defaults.Push(std::move(value));
            }

            if (source.identity)
                definition._identities.Push(Catalog::IdentityColumnsHeader(
                    table.id, columnId,
                    Constants::DEFAULT_IDENTITY_SEED, Constants::DEFAULT_IDENTITY_INCREMENT,
                    source.identityStart, true, Constants::DEFAULT_IDENTITY_CACHE_BLOCK
                ));
        }

        Catalog::IndexHeader index;
        index.tableId = table.id;
        index.id = PRIMARY_KEY_INDEX_ID;
        index.name = DataTypes::String::Concat(allocator, "PK_", header->name);
        index.isClustered = true;
        index.isDisabled = false;
        index.columns = DataStructures::PolymorphicArray<Catalog::IndexColumnsHeader>(allocator, static_cast<Int>(table.primaryKey.size()));

        Catalog::ConstraintsHeader constraint;
        constraint.tableId = table.id;
        constraint.constraintId = PRIMARY_KEY_CONSTRAINT_ID;
        constraint.name = index.name;
        constraint.type = Schemas::ConstraintType::PrimaryKey;
        constraint.isDisabled = false;
        constraint.indexId = PRIMARY_KEY_INDEX_ID;
        constraint.columns = DataStructures::PolymorphicArray<Catalog::ConstraintsColumnsHeader>(allocator, static_cast<Int>(table.primaryKey.size()));

        for (SmallInt position = 0; position < static_cast<SmallInt>(table.primaryKey.size()); position++){
            const auto columnId = SystemColumnId(table.id, table.primaryKey[position]);

            Catalog::IndexColumnsHeader indexColumn;
            indexColumn.indexId = index.id;
            indexColumn.columnId = columnId;
            indexColumn.ordinalPosition = position;
            indexColumn.isIncluded = false;
            index.columns.Push(std::move(indexColumn));

            Catalog::ConstraintsColumnsHeader constraintColumn;
            constraintColumn.constraintId = constraint.constraintId;
            constraintColumn.columnId = columnId;
            constraintColumn.ordinalPosition = position;
            constraint.columns.Push(std::move(constraintColumn));
        }

        definition._indexesColumnsCount = index.columns.Size();
        definition._constraintsColumnsCount = constraint.columns.Size();
        definition._indexes.Push(std::move(index));
        definition._constraints.Push(std::move(constraint));

        return definition;
    }
}
