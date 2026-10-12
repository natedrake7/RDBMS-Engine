#include <CoreEngine/Database.h>
#include <CoreEngine/SystemDatabases/SystemCatalog.h>
#include <CoreEngine/BufferPool/StorageManager.h>
#include <CoreEngine/Directory/DirectoryEntries.h>

#include "CoreEngine/DataStorage/Table.h"

namespace CoreEngine{
    namespace{
        // Bump-copies a lowercased name into a buffer sized up front; used for the names the
        // database owns (its own and its schemas'). Table keys point into each TableSchema instead.
        template <typename TText>
        DataTypes::StringView CopyString(char*& cursor, const TText& str){
            const auto size = str.Size();
            const auto* data = str.Data();

            auto* start = cursor;
            for (data_size_t i = 0;i < size; i++){
                *cursor = DataTypes::StringView::Lower(data[i]);
                cursor++;
            }

            return {start, size};
        }
    }

    StorageTypes::Table* Database::CreateTable(
        const Schemas::TableSchema* schema
    ){
        auto* table = this->CreateTablePtr(schema, StorageTypes::TableHeader());
        this->header.numberOfTables++;
        this->header.lastTableId = schema->_id;

        this->_tables.Insert(Directory::TableEntry(table, Directory::TableKey(schema->_schemaId, schema->_lowerName)));
        return table;
    }

    StorageTypes::Table* Database::CreateTablePtr(
        const Schemas::TableSchema* schema,
        const StorageTypes::TableHeader& physicalHeader
    ){
        return new StorageTypes::Table(schema, physicalHeader, this);
    }

    void Database::Bootstrap(
        const ::Memory::IAllocator* allocator,
        const Catalog::DatabaseHeader& header
    ){
        static auto& catalog = SystemCatalog::Get();

        const auto headerPage = Storage::StorageManager::Get().GetPage<Pages::HeaderPageView>(
            this->systemFileKey,
            Constants::HEADER_PAGE_ID
        );

        const auto tableHeaders = catalog.SelectTables(allocator, this->id);

        const auto schemas = catalog.SelectSchemas(allocator, this->id);

        // Only the database and schema names: table keys point into each TableSchema's own arena.
        // The entries live in the directories' arrays, which allocate from _allocator themselves.
        UnsignedInt stringSize = header.name.Size();
        for (const auto& schema : schemas)
            stringSize += schema.name.Size();

        auto* cursor = this->_allocator.AllocateRaw<char>(stringSize);
        this->_lowerName = CopyString(cursor, header.name);

        this->_schemas.Reserve(schemas.Size());
        for (const auto& schema : schemas)
            this->_schemas.Append(Directory::SchemaEntry(CopyString(cursor, schema.name), schema.id));

        if (!this->_schemas.Seal())
            throw std::runtime_error("Database::Bootstrap: duplicate schema name in catalog");

        this->_tables.Reserve(tableHeaders.Size());

        for (const auto& tableHeader : tableHeaders){
            const auto tableDefinition = catalog.SelectTableDefinition(allocator, tableHeader);
            const auto* schema = Schemas::TableSchema::Build(tableDefinition, 1);

            auto* table = this->CreateTablePtr(schema, *headerPage.GetTableHeaderPtr(tableHeader.ordinalPosition));
            this->_tables.Append(Directory::TableEntry(table, Directory::TableKey(schema->_schemaId, schema->_lowerName)));
        }

        if (!this->_tables.Seal())
            throw std::runtime_error("Database::Bootstrap: duplicate table name in catalog under the same schema");
    }

    void Database::BootstrapSystem(
        const ::Memory::IAllocator* allocator,
        const DataTypes::StringView& name,
        const bool isNew
    ){
        const auto headerPage = Storage::StorageManager::Get().GetPage<Pages::HeaderPageView>(
            this->systemFileKey,
            Constants::HEADER_PAGE_ID
        );

        auto* cursor = this->_allocator.AllocateRaw<char>(name.Size() + SYSTEM_SCHEMA_NAME.Size());
        this->_lowerName = CopyString(cursor, name);

        this->_schemas.Append(Directory::SchemaEntry(CopyString(cursor, SYSTEM_SCHEMA_NAME), SYSTEM_SCHEMA_ID));
        if (!this->_schemas.Seal())
            throw std::runtime_error("Database::BootstrapSystem: duplicate system schema");

        this->_tables.Reserve(static_cast<Int>(Catalog::SYSTEM_TABLES.size()));

        for (const auto& systemTable : Catalog::SYSTEM_TABLES){
            // The definition is scratch: Build copies everything it keeps into the schema's own arena
            auto allocationStep = allocator->RecordAllocationStart();
            const auto tableDefinition = SystemTableDefinition(allocator, systemTable);
            const auto* schema = Schemas::TableSchema::Build(tableDefinition, 1);
            allocator->ReleaseFromAllocationStep(allocationStep);

            const auto physicalHeader = isNew
                ? StorageTypes::TableHeader()
                : *headerPage.GetTableHeaderPtr(systemTable.ordinal);

            auto* table = this->CreateTablePtr(schema, physicalHeader);
            this->_tables.Append(Directory::TableEntry(table, Directory::TableKey(schema->_schemaId, schema->_lowerName)));

            if (isNew){
                this->header.numberOfTables++;
                this->header.lastTableId = schema->_id;
            }
        }

        if (!this->_tables.Seal())
            throw std::runtime_error("Database::BootstrapSystem: duplicate system table name");
    }
}
