#include "../../include/SystemDatabases/SystemCatalog.h"

#include <fstream>

#include "../../include/SystemDatabases/CatalogSchema.h"
#include "../../include/Database.h"

#include <iostream>
#include <nlohmann/json.hpp>

#include "Converter.h"
#include "DataStorage/Table.h"
#include "DataTypes/DataTypes.StaticData.h"
#include "Extensions/StringExtensions.h"

namespace Headers {
    void from_json(const nlohmann::json& j, sysColumn& sysColumn) {
        j.at("name").get_to(sysColumn.name);
        j.at("type").get_to(sysColumn.type);

        if (j.contains("size"))
            j.at("size").get_to(sysColumn.size);
        if (j.contains("default"))
            j.at("default").get_to(sysColumn._default);
        if (j.contains("nullable"))
            j.at("nullable").get_to(sysColumn.nullable);
        if (j.contains("hasIdentity"))
            j.at("hasIdentity").get_to(sysColumn.hasIdentity);
    }

    void from_json(const nlohmann::json& j, sysTable& sysTable) {
        j.at("name").get_to(sysTable.name);
        j.at("id").get_to(sysTable.id);
        j.at("columns").get_to(sysTable.columns);
        j.at("primaryKey").get_to(sysTable.primaryKey);
    }
}

namespace CoreEngine {
    SystemCatalog::SystemCatalog() {
        this->masterDb = nullptr;
    }

    std::tuple<DataTypes::String, DataTypes::String> SystemCatalog::ReadConfiguration(
        const ::Memory::IAllocator* allocator,
        const DataTypes::StringView& configPath
    ) {
        std::ifstream file(configPath.Data());

        if (!file.is_open())
            throw std::runtime_error("System Tables file: " + std::string(configPath.Data(), configPath.Size()) + " could not be opened");

        nlohmann::json jsonFile;

        try {
            file >> jsonFile;
        }
        catch (std::exception &e){
            throw std::runtime_error(e.what());
        }

        DataTypes::String sysDbName(allocator);
        DataTypes::String sysDbPath(allocator);

        jsonFile.at("db_name").get_to(sysDbName);
        jsonFile.at("db_path").get_to(sysDbPath);
        jsonFile.at("tables").get_to(this->sysTables);

        return std::make_tuple(std::move(sysDbName), std::move(sysDbPath));
    }

    bool SystemCatalog::CatalogExists(const DataTypes::StringView& path){
        return Storage::FileManager::FileExists(path);
    }

    void SystemCatalog::UseCatalogDatabase(const ::Memory::IAllocator* allocator, const DataTypes::String& dbName) {
        this->masterDb = new Database(
            allocator,
            Constants::SYSTEM_CATALOG_ID,
            dbName,
            this->sysTables
        );
        this->masterDb->GetColumnsHeaders(allocator);
        this->masterDb->GetIndexes(allocator);
        this->masterDb->GetIdentityColumns(allocator);
    }

    void SystemCatalog::CreateCatalogDatabase(const ::Memory::IAllocator* allocator, const DataTypes::String& dbName) {
        CreateDatabase(Constants::SYSTEM_CATALOG_ID, dbName);
        this->masterDb = new Database(
            allocator,
            Constants::SYSTEM_CATALOG_ID,
            dbName,
            true
        );

        for (int i = 0;i < this->sysTables.size(); i++) {
            const auto& tableHeader = this->sysTables[i];

            column_index_t primaryKeyIndexes[10];
            Int counter = 0;

            auto* table = this->masterDb->CreateTable(tableHeader.id, i);
            for (Int columnIndex = 0; columnIndex < tableHeader.columns.size(); columnIndex++) {
                const auto& columnHeader = tableHeader.columns[columnIndex];

                block_size_t columnSize = 0;
                const auto strView = DataTypes::StringView(columnHeader.type);
                if (!COLUMN_SIZES_BY_TYPENAME.TryGetValue(strView, columnSize))
                    throw std::runtime_error("Column type " + std::string(strView.Data(), strView.Size()) + " does not exist");
                if (columnSize == 0)
                    columnSize = columnHeader.size;

                const auto columnType = COLUMN_TYPENAMES_TO_ENUMS.Get(&strView);
                for (const auto& key: tableHeader.primaryKey) {
                    if (columnHeader.name != key)
                        continue;

                    primaryKeyIndexes[counter++] = columnIndex;
                }

                const auto nameView = DataTypes::StringView(columnHeader.name);
                auto* column = table->AddColumn(nameView, columnType, columnSize, columnIndex, columnHeader.nullable);

                if (columnHeader.hasIdentity){
                    auto defaultIdentityValue = Constants::DEFAULT_IDENTITY_VALUE;
                    if (i == CatalogTables::SysDatabases
                        && columnIndex == static_cast<Int>(SysDatabases::DatabaseId)
                    ) defaultIdentityValue = Constants::SYSTEM_CATALOG_ID;

                    column->SetIdentity(
                        Headers::IdentityColumnsHeader(
                            tableHeader.id,
                            columnIndex,
                            Constants::DEFAULT_IDENTITY_SEED,
                            Constants::DEFAULT_IDENTITY_INCREMENT,
                            defaultIdentityValue,
                            true,
                            Constants::DEFAULT_IDENTITY_CACHE_BLOCK
                        )
                    );
                }
            }

            if (counter == 0)
                throw std::runtime_error("All tables in masterDb must have a primary key");
            table->SetPrimaryKeyIndexedColumns(primaryKeyIndexes, counter);
        }
    }

    void SystemCatalog::StoreSystemTablesToCatalog(
        const ExecutionContext& baseContext,
        const DataTypes::StringView& dbNameView,
        const DataTypes::StringView& dbPathView
    )const {
        const auto dbInsertResult = this->InsertDbToMasterDb(
            baseContext,
            dbNameView,
            dbPathView,
            true
        );

        const auto databaseId = dbInsertResult.primaryKey.AsInt();

        const auto schemaInsertResult = this->InsertSchemaToMasterDb(
            baseContext,
            databaseId,
            Constants::DEFAULT_SCHEMA_NAME
        );

        const auto schemaId = schemaInsertResult.primaryKey.AsInt(1);

        Dictionary<std::string, column_index_t> columnNameToIndex;

        int counter=  0;
        for (int i = 0;i < this->sysTables.size(); i++) {
            const auto& table = this->sysTables[i];

            const auto tableResult =
            this->InsertTableToMasterDb(
                baseContext,
                databaseId,
                schemaId,
                DataTypes::StringView(table.name),
                static_cast<SmallInt>(i),
                true
            );


            int columnPos = 0;
            const auto tableId = tableResult.primaryKey.AsInt(1);

            Dictionary<std::string, Int> columnIdsDict;

            for (auto& column: table.columns) {
                counter++;

                const auto normalizedColumnType = DataTypes::String::Normalize(column.type, baseContext.GetAllocator());
                const auto strView = normalizedColumnType.ToView();

                auto columnSize = COLUMN_SIZES_BY_TYPENAME.Get(&strView);

                if (columnSize == 0)
                    columnSize = column.size;

                const auto& type = COLUMN_TYPENAMES_TO_ENUMS.Get(&strView);

                const auto columnResult =
                this->InsertColumnToMasterDb(
                    baseContext,
                    tableId,
                    DataTypes::StringView(column.name),
                    type,
                    columnSize,
                    INVALID_DECIMAL_PRECISION,
                    INVALID_DECIMAL_SCALE,
                    column.nullable,
                    columnPos,
                    true
                );

                if (columnResult.code != Errors::RuntimeError::Ok)
                    std::cerr << columnResult.message << std::endl;

                const auto columnId = columnResult.primaryKey.AsInt(1);

                if (column.hasIdentity){
                    const auto identityValue = (i == 0)
                        ? Constants::SYSTEM_CATALOG_ID
                        : Constants::DEFAULT_IDENTITY_VALUE;

                    const auto _ = this->InsertIdentityColumnToMasterDb(
                        baseContext,
                        tableId,
                        columnId,
                        Constants::DEFAULT_IDENTITY_SEED,
                        Constants::DEFAULT_IDENTITY_INCREMENT,
                        identityValue,
                        true,
                        Constants::DEFAULT_IDENTITY_CACHE_BLOCK
                    );
                }

                columnNameToIndex.Add(column.name, columnPos);
                columnIdsDict.Add(column.name, columnId);

                columnPos++;
            }

            DataTypes::String _columns(static_cast<const char*>("PK"), baseContext.GetAllocator());

            for (const auto& primaryKeyStr : table.primaryKey) {
                const auto key = columnNameToIndex.Get(primaryKeyStr);
                char buffer[3];
                snprintf(buffer, sizeof(buffer), "%d", key);
                _columns += "_" + primaryKeyStr;
            }

            //TODO keep the last value keys
            const auto indexResult =
                this->InsertIndexToMasterDb(
                    baseContext,
                    tableId,
                    _columns.ToView(),
                    true
                );

            auto indexId = indexResult.primaryKey.AsInt(1);

            const auto constraintResult =
                this->InsertConstraintToMasterDb(
                    baseContext,
                    tableId,
                    _columns.ToView(),
                    Headers::ConstraintType::PrimaryKey,
                    false,
                    &indexId
                );

            for(int j = 0;j < table.primaryKey.size(); j++){
                auto _ = this->InsertIndexColumnToMasterDb(
                    baseContext,
                    indexId,
                    columnIdsDict.Get(table.primaryKey[j]),
                    static_cast<SmallInt>(j),
                    true
                );

                _ = this->InsertConstraintColumnToMasterDb(
                    baseContext,
                    constraintResult.primaryKey.AsInt(1),
                    columnIdsDict.Get(table.primaryKey[j]),
                    static_cast<SmallInt>(j)
                );
            }
        }

        this->masterDb->GetColumnsHeaders(baseContext.GetAllocator());
        this->masterDb->UpdateIdentityManagersIds(baseContext.GetAllocator());
    }

    Headers::DatabaseHeader SystemCatalog::ToDatabaseHeader(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::RID* rowPtr,
        const StorageTypes::Table* table
    ) {
        const auto materializedRow = table->MaterializeFromPage(allocator, rowPtr);
        const auto& data = materializedRow.Data();

        return Headers::DatabaseHeader{
            .id = data[static_cast<column_index_t>(SysDatabases::DatabaseId)].AsInt(),
            .name = data[static_cast<column_index_t>(SysDatabases::Name)].AsString(),
            .filepath = data[static_cast<column_index_t>(SysDatabases::FilePath)].AsString(),
            .isSystem = data[static_cast<column_index_t>(SysDatabases::IsSystem)].AsBool(),
            .additionalInfo = Headers::AuditInformation()
        };
    }

    Headers::DatabaseHeader SystemCatalog::ToDatabaseHeader(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::RID* rowPtr,
        const StorageTypes::Table* table,
        DataStructures::PolymorphicArray<Headers::TableHeader>& dbTables,
        DataStructures::PolymorphicArray<Headers::SchemaHeader>& schemas
    ) {
        const auto materializedRow = table->MaterializeFromPage(allocator, rowPtr);
        const auto& data = materializedRow.Data();

        return Headers::DatabaseHeader{
            .id = data[static_cast<column_index_t>(SysDatabases::DatabaseId)].AsInt(),
            .name = data[static_cast<column_index_t>(SysDatabases::Name)].AsString(),
            .filepath = data[static_cast<column_index_t>(SysDatabases::FilePath)].AsString(),
            .isSystem = data[static_cast<column_index_t>(SysDatabases::IsSystem)].AsBool(),
            .additionalInfo = Headers::AuditInformation(
                data[static_cast<column_index_t>(SysDatabases::CreatedAt)].AsDateTime(),
                data[static_cast<column_index_t>(SysDatabases::LastModifiedAt)].AsDateTime(),
                data[static_cast<column_index_t>(SysDatabases::LastModifiedBy)].AsString(),
                data[static_cast<column_index_t>(SysDatabases::Version)].AsInt(),
                data[static_cast<column_index_t>(SysDatabases::IsDeleted)].AsBool(),
                data[static_cast<column_index_t>(SysDatabases::DeletedAt)].IsNull()
                    ? DataTypes::DateTime()
                    : data[static_cast<column_index_t>(SysDatabases::DeletedAt)].AsDateTime()
            ),
            .tables = std::move(dbTables),
            .schemas = std::move(schemas)
        };
    }

    Headers::SchemaHeader SystemCatalog::ToSchemaHeader(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::RID* rowPtr,
        const StorageTypes::Table* table
    ) {
        const auto materializedRow = table->MaterializeFromPage(allocator, rowPtr);
        const auto& data = materializedRow.Data();

        auto header = Headers::SchemaHeader(
            data[static_cast<column_index_t>(SysSchemas::SchemaId)].AsInt(),
            data[static_cast<column_index_t>(SysSchemas::DatabaseId)].AsInt(),
            data[static_cast<column_index_t>(SysSchemas::Name)].AsString()
        );

        header.additionalInfo.createdAt = data[static_cast<column_index_t>(SysSchemas::CreatedAt)].AsDateTime();
        header.additionalInfo.lastModified = data[static_cast<column_index_t>(SysSchemas::LastModifiedAt)].AsDateTime();
        header.additionalInfo.lastModifiedBy = data[static_cast<column_index_t>(SysSchemas::LastModifiedBy)].AsString();

        return header;
    }

    Headers::TableHeader SystemCatalog::ToTableHeader(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::RID* rowPtr,
        const StorageTypes::Table* table
    )
    {
        const auto materializedRow = table->MaterializeFromPage(allocator, rowPtr);
        const auto& data = materializedRow.Data();

        auto header = Headers::TableHeader();

        header.databaseId = data[static_cast<column_index_t>(SysTables::DatabaseId)].AsInt();
        header.id = data[static_cast<column_index_t>(SysTables::TableId)].AsInt();
        header.schemaId = data[static_cast<column_index_t>(SysTables::SchemaId)].AsInt();
        header.name = data[static_cast<column_index_t>(SysTables::Name)].AsString();
        header.ordinalPosition = data[static_cast<column_index_t>(SysTables::OrdinalPosition)].AsSmallInt();
        header.isSystem = data[static_cast<column_index_t>(SysTables::IsSystemTable)].AsBool();
        header.additionalInfo.createdAt = data[static_cast<column_index_t>(SysTables::CreatedAt)].AsDateTime();
        header.additionalInfo.lastModified = data[static_cast<column_index_t>(SysTables::LastModifiedAt)].AsDateTime();
        header.additionalInfo.lastModifiedBy = data[static_cast<column_index_t>(SysTables::LastModifiedBy)].AsString();

        return header;
    }

    Headers::ColumnHeader SystemCatalog::ToColumnHeader(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::RID* rowPtr,
        const StorageTypes::Table* table
    ){
        const auto materializedRow = table->MaterializeFromPage(allocator, rowPtr);
        const auto& data = materializedRow.Data();

        return Headers::ColumnHeader{
            .tableId = data[static_cast<column_index_t>(SysColumns::TableId)].AsInt(),
            .id = data[static_cast<column_index_t>(SysColumns::ColumnId)].AsInt(),
            .name = data[static_cast<column_index_t>(SysColumns::Name)].AsString(),
            .dataType = static_cast<UnsignedTinyInt>(data[static_cast<column_index_t>(SysColumns::DataType)].AsTinyInt()),
            .recordSize = data[static_cast<column_index_t>(SysColumns::RecordSize)].AsInt(),
            .precision = data[static_cast<column_index_t>(SysColumns::Precision)].IsNull()
                ? INVALID_DECIMAL_PRECISION
                : data[static_cast<column_index_t>(SysColumns::Precision)].AsTinyInt(),
            .scale = data[static_cast<column_index_t>(SysColumns::Scale)].IsNull()
                ? INVALID_DECIMAL_SCALE
                : data[static_cast<column_index_t>(SysColumns::Scale)].AsTinyInt(),
            .isNullable = data[static_cast<column_index_t>(SysColumns::IsNullable)].AsBool(),
            .ordinalPosition = data[static_cast<column_index_t>(SysColumns::OrdinalPosition)].AsSmallInt(),
            .isSystem = data[static_cast<column_index_t>(SysColumns::IsSystemColumn)].AsBool(),
            .additionalInfo = Headers::AuditInformation(
            data[static_cast<column_index_t>(SysColumns::CreatedAt)].AsDateTime(),
            data[static_cast<column_index_t>(SysColumns::LastModifiedAt)].AsDateTime(),
            data[static_cast<column_index_t>(SysColumns::LastModifiedBy)].AsString(),
            data[static_cast<column_index_t>(SysColumns::Version)].AsInt(),
            data[static_cast<column_index_t>(SysColumns::IsDeleted)].AsBool(),
            data[static_cast<column_index_t>(SysColumns::DeletedAt)].IsNull()
                ? DataTypes::DateTime()
                : data[static_cast<column_index_t>(SysColumns::DeletedAt)].AsDateTime()
            )
        };
    }

    Headers::IndexHeader SystemCatalog::ToIndexHeader(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::RID* rowPtr,
        const StorageTypes::Table* table
    ){
        const auto materializedRow = table->MaterializeFromPage(allocator, rowPtr);
        const auto& data = materializedRow.Data();
        return Headers::IndexHeader{
            .tableId = data[static_cast<column_index_t>(SysIndexes::TableId)].AsInt(),
            .id = data[static_cast<column_index_t>(SysIndexes::IndexId)].AsInt(),
            .name = data[static_cast<column_index_t>(SysIndexes::Name)].AsString(),
            .isClustered = data[static_cast<column_index_t>(SysIndexes::IsClustered)].AsBool(),
            .isDisabled = data[static_cast<column_index_t>(SysIndexes::IsDisabled)].AsBool(),
            .additionalInfo = Headers::AuditInformation(
                data[static_cast<column_index_t>(SysIndexes::CreatedAt)].AsDateTime(),
                data[static_cast<column_index_t>(SysIndexes::LastModifiedAt)].AsDateTime(),
                data[static_cast<column_index_t>(SysIndexes::LastModifiedBy)].AsString(),
                data[static_cast<column_index_t>(SysIndexes::Version)].AsInt(),
                data[static_cast<column_index_t>(SysIndexes::IsDeleted)].AsBool(),
                data[static_cast<column_index_t>(SysIndexes::DeletedAt)].IsNull()
                    ? DataTypes::DateTime()
                    : data[static_cast<column_index_t>(SysIndexes::DeletedAt)].AsDateTime()
            ),
        };
    }

    Headers::IndexColumnsHeader SystemCatalog::ToIndexColumnsHeader(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::RID* rowPtr,
        const StorageTypes::Table* table
    ){
        const auto materializedRow = table->MaterializeFromPage(allocator, rowPtr);
        const auto& data = materializedRow.Data();

        return Headers::IndexColumnsHeader{
            .indexId = data[static_cast<column_index_t>(SysIndexColumns::IndexId)].AsInt(),
            .columnId = data[static_cast<column_index_t>(SysIndexColumns::ColumnId)].AsInt(),
            .ordinalPosition = data[static_cast<column_index_t>(SysIndexColumns::OrdinalPosition)].AsSmallInt(),
            .isIncluded = data[static_cast<column_index_t>(SysIndexColumns::IsIncluded)].AsBool(),
            .additionalInfo = Headers::AuditInformation(
            data[static_cast<column_index_t>(SysIndexColumns::Version)].AsInt(),
            data[static_cast<column_index_t>(SysIndexColumns::IsDeleted)].AsBool(),
            DataTypes::String::Null(),
            data[static_cast<column_index_t>(SysIndexColumns::DeletedAt)].IsNull()
                    ? DataTypes::DateTime()
                    : data[static_cast<column_index_t>(SysIndexColumns::DeletedAt)].AsDateTime()
            )
        };
    }

    Headers::IdentityColumnsHeader SystemCatalog::ToIdentityColumnsHeader(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::RID* rowPtr,
        const StorageTypes::Table* table
    ){
        const auto materializedRow = table->MaterializeFromPage(allocator, rowPtr);
        const auto& data = materializedRow.Data();

        return Headers::IdentityColumnsHeader(
            data[static_cast<column_index_t>(SysIdentityColumns::TableId)].AsInt(),
            data[static_cast<column_index_t>(SysIdentityColumns::ColumnId)].AsInt(),
            data[static_cast<column_index_t>(SysIdentityColumns::SeedValue)].AsInt(),
            data[static_cast<column_index_t>(SysIdentityColumns::IncrementValue)].AsInt(),
            data[static_cast<column_index_t>(SysIdentityColumns::LastValue)].AsBigInt(),
            data[static_cast<column_index_t>(SysIdentityColumns::IsCached)].AsBool(),
            data[static_cast<column_index_t>(SysIdentityColumns::CacheBlock)].AsInt(),
            Headers::AuditInformation(
            data[static_cast<column_index_t>(SysIdentityColumns::Version)].AsInt(),
            data[static_cast<column_index_t>(SysIdentityColumns::IsDeleted)].AsBool(),
            DataTypes::String::Null(),
            data[static_cast<column_index_t>(SysIdentityColumns::DeletedAt)].IsNull()
                    ? DataTypes::DateTime()
                    : data[static_cast<column_index_t>(SysIdentityColumns::DeletedAt)].AsDateTime()
            )
        );
    }

    Headers::ConstraintsHeader SystemCatalog::ToConstraintsHeader(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::RID* rowPtr,
        const StorageTypes::Table* table,
        DataStructures::PolymorphicArray<Headers::ConstraintsColumnsHeader>& constraintColumns,
        Headers::IndexHeader& indexHeader
    ){
    const auto materializedRow = table->MaterializeFromPage(allocator, rowPtr);
    const auto& data = materializedRow.Data();

    return Headers::ConstraintsHeader{
        .tableId = data[static_cast<column_index_t>(SysConstraints::TableId)].AsInt(),
        .constraintId = data[static_cast<column_index_t>(SysConstraints::ConstraintId)].AsInt(),
        .name = data[static_cast<column_index_t>(SysConstraints::Name)].AsString(),
        .type = static_cast<Headers::ConstraintType>(data[static_cast<column_index_t>(SysConstraints::Type)].AsTinyInt()),
        .isDisabled = data[static_cast<column_index_t>(SysConstraints::IsDisabled)].AsBool(),
        .indexId = indexHeader.id,
        .index = std::move(indexHeader),
        .columns = std::move(constraintColumns),
        .additionalInfo = Headers::AuditInformation(
        data[static_cast<column_index_t>(SysConstraints::CreatedAt)].AsDateTime(),
        data[static_cast<column_index_t>(SysConstraints::LastModifiedAt)].AsDateTime(),
        data[static_cast<column_index_t>(SysConstraints::LastModifiedBy)].AsString(),
        data[static_cast<column_index_t>(SysConstraints::Version)].AsInt(),
        data[static_cast<column_index_t>(SysConstraints::IsDeleted)].AsBool(),
        data[static_cast<column_index_t>(SysConstraints::DeletedAt)].IsNull()
                ? DataTypes::DateTime()
                : data[static_cast<column_index_t>(SysConstraints::DeletedAt)].AsDateTime()
        ),
    };
    }

    Headers::ConstraintsColumnsHeader SystemCatalog::ToConstraintsColumnsHeader(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::RID* rowPtr,
        const StorageTypes::Table* table
    ){
    const auto materializedRow = table->MaterializeFromPage(allocator, rowPtr);
    const auto& data = materializedRow.Data();

    return Headers::ConstraintsColumnsHeader{
        .constraintId = data[static_cast<column_index_t>(SysConstraintColumns::ConstraintId)].AsInt(),
        .columnId = data[static_cast<column_index_t>(SysConstraintColumns::ColumnId)].AsInt(),
        .ordinalPosition = data[static_cast<column_index_t>(SysConstraintColumns::OrdinalPosition)].AsInt(),
        .additionalInfo = Headers::AuditInformation(
        data[static_cast<column_index_t>(SysConstraintColumns::Version)].AsInt(),
        data[static_cast<column_index_t>(SysConstraintColumns::IsDeleted)].AsBool(),
        DataTypes::String::Null(),
        data[static_cast<column_index_t>(SysConstraintColumns::DeletedAt)].IsNull()
                ? DataTypes::DateTime()
                : data[static_cast<column_index_t>(SysConstraintColumns::DeletedAt)].AsDateTime()
        ),
    };
    }

    Headers::DefaultValuesHeader SystemCatalog::ToDefaultValuesHeader(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::RID* rowPtr,
        const StorageTypes::Table* table
    )
    {
    const auto materializedRow = table->MaterializeFromPage(allocator, rowPtr);
    const auto& data = materializedRow.Data();

    return Headers::DefaultValuesHeader{
        .columnId = data[static_cast<column_index_t>(SysDefaultValues::ColumnId)].AsInt(),
        .value = data[static_cast<column_index_t>(SysDefaultValues::Value)].AsString(),
        .additionalInfo = Headers::AuditInformation(
        data[static_cast<column_index_t>(SysDefaultValues::Version)].AsInt(),
        data[static_cast<column_index_t>(SysDefaultValues::IsDeleted)].AsBool(),
        DataTypes::String::Null(),
        data[static_cast<column_index_t>(SysDefaultValues::DeletedAt)].IsNull()
                ? DataTypes::DateTime()
                : data[static_cast<column_index_t>(SysDefaultValues::DeletedAt)].AsDateTime()
        ),
    };
    }

    Headers::TableStatistics SystemCatalog::ToTableStatistics(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::RID* rowPtr,
        const StorageTypes::Table* table
    )
    {
    const auto materializedRow = table->MaterializeFromPage(allocator, rowPtr);
    const auto& data = materializedRow.Data();

    return {
        data[static_cast<column_index_t>(SysTableStats::TableId)].AsInt(),
        data[static_cast<column_index_t>(SysTableStats::RowCount)].AsBigInt(),
        data[static_cast<column_index_t>(SysTableStats::AvgRowSize)].AsInt(),
        data[static_cast<column_index_t>(SysTableStats::PageCount)].AsInt(),
        data[static_cast<column_index_t>(SysTableStats::LastUpdatedAt)].AsDateTime()
    };
    }

    Headers::ColumnStatistics SystemCatalog::ToColumnStatistics(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::RID* rowPtr,
        const StorageTypes::Table* table,
        const DataType columnType
    ) {
        const auto materializedRow = table->MaterializeFromPage(allocator, rowPtr);
        const auto& data = materializedRow.Data();

        return Headers::ColumnStatistics{
            .columnId = data[static_cast<column_index_t>(SysColumnStats::ColumnId)].AsInt(),
            .distinctCount = data[static_cast<column_index_t>(SysColumnStats::DistinctCount)].AsBigInt(),
            .min = Value(
                data[static_cast<column_index_t>(SysColumnStats::MinimumValue)].Data(),
                data[static_cast<column_index_t>(SysColumnStats::MinimumValue)].Size(),
                columnType,
                allocator
            ),
            .max = Value(
                data[static_cast<column_index_t>(SysColumnStats::MaximumValue)].Data(),
                data[static_cast<column_index_t>(SysColumnStats::MaximumValue)].Size(),
                columnType,
                allocator
            ),
            .nullCount = data[static_cast<column_index_t>(SysColumnStats::NullCount)].AsBigInt()
        };
    }

    Headers::ColumnHistograms SystemCatalog::ToColumnHistograms(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::RID* rowPtr,
        const StorageTypes::Table* table,
        const DataType columnType
    ) {
        const auto materializedRow = table->MaterializeFromPage(allocator, rowPtr);
        const auto& data = materializedRow.Data();

        return Headers::ColumnHistograms{
                data[static_cast<column_index_t>(SysColumnHistograms::ColumnId)].AsInt(),
                data[static_cast<column_index_t>(SysColumnHistograms::HistogramId)].AsInt(),
                Value(
                    data[static_cast<column_index_t>(SysColumnHistograms::RangeStart)].Data(),
                    data[static_cast<column_index_t>(SysColumnHistograms::RangeStart)].Size(),
                    columnType,
                    allocator
                ),
                Value(
                    data[static_cast<column_index_t>(SysColumnHistograms::RangeEnd)].Data(),
                    data[static_cast<column_index_t>(SysColumnHistograms::RangeEnd)].Size(),
                    columnType,
                    allocator
                ),
                data[static_cast<column_index_t>(SysColumnHistograms::RowCount)].AsInt(),
                data[static_cast<column_index_t>(SysColumnHistograms::DistinctCount)].AsInt()
        };
    }

    Headers::IndexStatistics SystemCatalog::ToIndexStatistics(
        const ::Memory::IAllocator* allocator,
        const StorageTypes::RID* rowPtr,
        const StorageTypes::Table* table
    ){
        const auto materializedRow = table->MaterializeFromPage(allocator, rowPtr);
        const auto& data = materializedRow.Data();

        return {
        data[static_cast<column_index_t>(SysIndexStats::TableId)].AsInt(),
        data[static_cast<column_index_t>(SysIndexStats::IndexId)].AsInt(),
        data[static_cast<column_index_t>(SysIndexStats::LeafPages)].AsInt(),
        data[static_cast<column_index_t>(SysIndexStats::Depth)].AsTinyInt(),
        data[static_cast<column_index_t>(SysIndexStats::AverageFragmentation)].AsDecimal(),
        data[static_cast<column_index_t>(SysIndexStats::LastUpdated)].AsDateTime(),
        };
    }

    SystemCatalog & SystemCatalog::Get() {
        static SystemCatalog instance;
        return instance;
    }

    Database* SystemCatalog::GetDatabase() const{ return this->masterDb; }

    bool SystemCatalog::Initialize(
            const ExecutionContext& baseContext,
            const DataTypes::StringView& configPath
    ) {
        const auto [sysDbName, sysDbPath] = this->ReadConfiguration(baseContext.GetAllocator(), configPath);

        if (SystemCatalog::CatalogExists(sysDbName.ToView())){
            this->UseCatalogDatabase(baseContext.GetAllocator(), sysDbName);
            return false;
        }

        this->CreateCatalogDatabase(baseContext.GetAllocator(), sysDbName);
        this->StoreSystemTablesToCatalog(baseContext, sysDbName.ToView(), sysDbPath.ToView());

        return true;
    }

    void SystemCatalog::Shutdown(){
        const Memory::Allocator allocator;
        this->masterDb->UpdateMasterDatabase(&allocator);

        delete this->masterDb;
        this->masterDb = nullptr;
    }

    DataStructures::PolymorphicArray<Headers::DatabaseHeader> SystemCatalog::RetrieveCatalog() const {

    // DataStructures::PolymorphicArray<Pages::RowReference> selectedDatabases;
    //
    // IndexState state;
    // sysDatabases->ClusteredIndexScan(this->baseExecutionContext, &selectedDatabases, state, nullptr);
    //
    // DataStructures::PolymorphicArray<Headers::DatabaseHeader> databasesHeaders;
    //
    // if (selectedDatabases.Empty())
    //   return {};

    // for (const auto& row : selectedDatabases) {
    //
    //   const auto& data = row.GetData();
    //
    //   const auto databaseId = data[static_cast<column_index_t>(SysDatabases::DatabaseId)]->AsInt();
    //
    //   auto schemas = this->SelectSchemas(databaseId);
    //
    //   auto tables = this->SelectTables(databaseId);
    //
    //   for (auto& table : tables) {
    //     table.columns = this->SelectColumns(table.id);
    //     table.statistics = this->SelectTableStatisticsById(table.id);
    //
    //     const auto identityColumns = this->SelectIdentityColumnsByTableIdToDictionary(table.id);
    //
    //     for (auto& column : table.columns) {
    //       Headers::IdentityColumnsHeader identityHeader;
    //       identityColumns.TryGetValue(column.id, identityHeader);
    //
    //       column.identity = std::move(identityHeader);
    //       column.defaultValue = this->SelectDefaultValueByColumnId(column.id);
    //       column.statistics = this->SelectColumnStatisticsById(column.id, static_cast<DataType>(column.dataType));
    //     }
    //
    //     table.constraints = this->SelectConstraints(table.id);
    //     // table.identity = this->SelectIdentityColumnsByTableId(table.id);
    //   }
    //
    //   databasesHeaders.Push(SystemCatalog::ToDatabaseHeader(row, tables, schemas));
    // }

    // return databasesHeaders;
        return {};
    }

    DataStructures::PolymorphicArray<Security::Role*> SystemCatalog::InsertSystemRoles(const ExecutionContext& baseContext)const{
    auto* tempAllocator = baseContext.GetAllocator();

    DataStructures::PolymorphicArray<Security::Role*> roles(tempAllocator);

    auto result = this->InsertRoleToMasterDb(
        baseContext,
        Constants::ADMIN_NAME,
        Constants::ADMIN_PERMISSIONS
    );

    auto* adminRole = tempAllocator->Allocate<Security::Role>(
        result.primaryKey.AsInt(),
        DataTypes::String::FromView(Constants::ADMIN_NAME, tempAllocator),
        Constants::ADMIN_PERMISSIONS,
        true
    );

    roles.Push(adminRole);

    result = this->InsertRoleToMasterDb(
        baseContext,
        Constants::DB_OWNER_NAME,
        Constants::DB_OWNER_PERMISSIONS
    );

    auto* dbOwnerRole = tempAllocator->Allocate<Security::Role>(
        result.primaryKey.AsInt(),
        DataTypes::String::FromView(Constants::DB_OWNER_NAME, tempAllocator),
        Constants::DB_OWNER_PERMISSIONS,
        true
    );

    roles.Push(dbOwnerRole);

    result = this->InsertRoleToMasterDb(
        baseContext,
        Constants::DB_WRITER_NAME,
        Constants::DB_WRITER_PERMISSIONS
    );

    auto* dbWriterRole = tempAllocator->Allocate<Security::Role>(
        result.primaryKey.AsInt(),
        DataTypes::String::FromView(Constants::DB_WRITER_NAME, tempAllocator),
        Constants::DB_WRITER_PERMISSIONS,
        true
    );

    roles.Push(dbWriterRole);

    result = this->InsertRoleToMasterDb(
        baseContext,
        Constants::DB_READER_NAME,
        Constants::DB_READER_PERMISSIONS
    );

    auto* dbReaderRole = tempAllocator->Allocate<Security::Role>(
        result.primaryKey.AsInt(),
        DataTypes::String::FromView(Constants::DB_READER_NAME, tempAllocator),
        Constants::DB_READER_PERMISSIONS,
        true
    );

    roles.Push(dbReaderRole);

    result = this->InsertRoleToMasterDb(
        baseContext,
        Constants::GUEST_NAME,
        Constants::GUEST_PERMISSIONS
    );

    auto* guestRole = tempAllocator->Allocate<Security::Role>(
        result.primaryKey.AsInt(),
        DataTypes::String::FromView(Constants::GUEST_NAME, tempAllocator),
        Constants::GUEST_PERMISSIONS,
        true
    );

    roles.Push(guestRole);

    return roles;
    }

    Security::User SystemCatalog::InsertSystemUsers(
        const ExecutionContext& baseContext,
        const DataTypes::String& hashedPassword,
        const Int defaultRoleId
    )const{
        const auto result =
            this->InsertUserToMasterDb(
                baseContext,
                Constants::ADMIN_NAME,
                hashedPassword.ToView(),
                defaultRoleId,
                true
            );

        const auto username = DataTypes::String(Constants::ADMIN_NAME.Data(), Constants::ADMIN_NAME.Size(), baseContext.GetAllocator());
        return Security::User(
            result.primaryKey.AsInt(),
            username,
            hashedPassword,
            defaultRoleId,
            nullptr,
            true
        );
    }

    Errors::RuntimeStatus SystemCatalog::InsertDbToMasterDb(
        const ExecutionContext& executionContext,
        const DataTypes::StringView& dbName,
        const DataTypes::StringView& dbPath,
        const bool isSystem,
        const DataTypes::StringView& user,
        const Int version,
        const bool isDeleted
    ) const{
        auto* table = this->masterDb->OpenTable(CatalogTables::SysDatabases);

        const auto currentDate = DataTypes::DateTime::Now();

        const auto fields = DataStructures::PolymorphicArray<Value>::From(executionContext.GetAllocator(),
        Value(dbName, executionContext.GetAllocator(), static_cast<column_index_t>(SysDatabases::Name)),
        Value(dbPath, executionContext.GetAllocator(), static_cast<column_index_t>(SysDatabases::FilePath)),
        Value(isSystem, executionContext.GetAllocator(), static_cast<column_index_t>(SysDatabases::IsSystem)),
        Value(currentDate, executionContext.GetAllocator(), static_cast<column_index_t>(SysDatabases::CreatedAt)),
        Value(currentDate, executionContext.GetAllocator(), static_cast<column_index_t>(SysDatabases::LastModifiedAt)),
        Value(user, executionContext.GetAllocator(), static_cast<column_index_t>(SysDatabases::LastModifiedBy)),
        Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysDatabases::Version)),
        Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysDatabases::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysDatabases::DeletedAt))
        );

    auto result = table->InsertRow(executionContext, fields);

    std::cout << "Inserted database: "<< dbName << " to master db" << std::endl;

    return result;
    }

    Errors::RuntimeStatus  SystemCatalog::InsertSchemaToMasterDb(
        const ExecutionContext& executionContext,
        const Int databaseId,
        const DataTypes::StringView& schemaName,
        const DataTypes::StringView& user,
        const Int version,
        const bool isDeleted
    ) const{
        auto* table = this->masterDb->OpenTable(CatalogTables::SysSchemas);
        const auto currentDate = DataTypes::DateTime::Now();

        const auto fields = DataStructures::PolymorphicArray<Value>::From(executionContext.GetAllocator(),
        Value(databaseId, executionContext.GetAllocator(), static_cast<column_index_t>(SysSchemas::DatabaseId)),
        Value(schemaName, executionContext.GetAllocator(), static_cast<column_index_t>(SysSchemas::Name)),
        Value(currentDate, executionContext.GetAllocator(), static_cast<column_index_t>(SysSchemas::CreatedAt)),
        Value(currentDate, executionContext.GetAllocator(), static_cast<column_index_t>(SysSchemas::LastModifiedAt)),
        Value(user, executionContext.GetAllocator(), static_cast<column_index_t>(SysSchemas::LastModifiedBy)),
        Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysSchemas::Version)),
        Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysSchemas::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysSchemas::DeletedAt))
        );

    auto result = table->InsertRow(executionContext, fields);

        std::cout << "Inserted schema: "<< schemaName << " to master db" << std::endl;

    return result;
    }

    Errors::RuntimeStatus SystemCatalog::InsertTableToMasterDb(
        const ExecutionContext& executionContext,
        const Int databaseId,
        const Int schemaId,
        const DataTypes::StringView& tableName,
        const SmallInt ordinalPosition,
        const bool isSystem,
        const DataTypes::StringView& user,
        const Int version,
        const bool isDeleted
    ) const{

        StorageTypes::Table* table = this->masterDb->OpenTable(CatalogTables::SysTables);
        const auto currentDate = DataTypes::DateTime::Now();

        const auto fields = DataStructures::PolymorphicArray<Value>::From(executionContext.GetAllocator(),
        Value(databaseId, executionContext.GetAllocator(), static_cast<column_index_t>(SysTables::DatabaseId)),
        Value(schemaId, executionContext.GetAllocator(), static_cast<column_index_t>(SysTables::SchemaId)),
        Value(tableName, executionContext.GetAllocator(), static_cast<column_index_t>(SysTables::Name)),
        Value(ordinalPosition, executionContext.GetAllocator(), static_cast<column_index_t>(SysTables::OrdinalPosition)),
        Value(isSystem, executionContext.GetAllocator(), static_cast<column_index_t>(SysTables::IsSystemTable)),
        Value(currentDate, executionContext.GetAllocator(), static_cast<column_index_t>(SysTables::CreatedAt)),
        Value(currentDate, executionContext.GetAllocator(), static_cast<column_index_t>(SysTables::LastModifiedAt)),
        Value(user, executionContext.GetAllocator(), static_cast<column_index_t>(SysTables::LastModifiedBy)),
        Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysTables::Version)),
        Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysTables::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysTables::DeletedAt))
        );

        auto result = table->InsertRow(executionContext, fields);

        std::cout << "Inserted table: "<< tableName << " to master db" << std::endl;

        return result;
    }

    Errors::RuntimeStatus SystemCatalog::InsertColumnToMasterDb(
        const ExecutionContext& executionContext,
        const Int tableId,
        const DataTypes::StringView& columnName,
        const DataType columnType,
        const Int columnSize,
        const TinyInt precision,
        const TinyInt scale,
        const bool isNullable,
        const Int ordinalPosition,
        const bool isSystem,
        const DataTypes::StringView& user,
        const Int version,
        const bool isDeleted
    ) const{
        auto* table = this->masterDb->OpenTable(CatalogTables::SysColumns);

        const auto currentDate = DataTypes::DateTime::Now();

        auto precisionField = precision != INVALID_DECIMAL_PRECISION
            ? Value(precision, executionContext.GetAllocator(), static_cast<column_index_t>(SysColumns::Precision))
            : Value::Null(static_cast<column_index_t>(SysColumns::Precision));

        auto scaleField = scale != INVALID_DECIMAL_SCALE
            ? Value(scale, executionContext.GetAllocator(), static_cast<column_index_t>(SysColumns::Scale))
            : Value::Null(static_cast<column_index_t>(SysColumns::Scale));

        const auto fields = DataStructures::PolymorphicArray<Value>::From(executionContext.GetAllocator(),
        Value(tableId, executionContext.GetAllocator(), static_cast<column_index_t>(SysColumns::TableId)),
        Value(columnName, executionContext.GetAllocator(), static_cast<column_index_t>(SysColumns::Name)),
        Value(static_cast<TinyInt>(columnType), executionContext.GetAllocator(), static_cast<column_index_t>(SysColumns::DataType)),
        Value(columnSize, executionContext.GetAllocator(), static_cast<column_index_t>(SysColumns::RecordSize)),
        std::move(precisionField),
        std::move(scaleField),
        Value(isNullable, executionContext.GetAllocator(), static_cast<column_index_t>(SysColumns::IsNullable)),
        Value(ordinalPosition, executionContext.GetAllocator(), static_cast<column_index_t>(SysColumns::OrdinalPosition)),
        Value(isSystem, executionContext.GetAllocator(), static_cast<column_index_t>(SysColumns::IsSystemColumn)),
        Value(currentDate, executionContext.GetAllocator(), static_cast<column_index_t>(SysColumns::CreatedAt)),
        Value(currentDate, executionContext.GetAllocator(), static_cast<column_index_t>(SysColumns::LastModifiedAt)),
        Value(user, executionContext.GetAllocator(), static_cast<column_index_t>(SysColumns::LastModifiedBy)),
        Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysColumns::Version)),
        Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysColumns::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysColumns::DeletedAt))
        );

        auto result = table->InsertRow(executionContext, fields);

        std::cout << "Inserted column: "<< columnName << " to master db" << std::endl;

        return result;
    }

    Errors::RuntimeStatus SystemCatalog::InsertIndexToMasterDb(
    const ExecutionContext& executionContext,
    const Int tableId,
    const DataTypes::StringView& indexName,
    const bool isClustered,
    const bool isDisabled,
    const DataTypes::StringView& user,
    const Int version,
    const bool isDeleted
    ) const{
        StorageTypes::Table* table = this->masterDb->OpenTable(CatalogTables::SysIndexes);
        const auto currentDate = DataTypes::DateTime::Now();

        const auto fields = DataStructures::PolymorphicArray<Value>::From(executionContext.GetAllocator(),
        Value(tableId, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexes::TableId)),
        Value(indexName, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexes::Name)),
        Value(isClustered, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexes::IsClustered)),
        Value(isDisabled, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexes::IsDisabled)),
        Value(currentDate, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexes::CreatedAt)),
        Value(currentDate, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexes::LastModifiedAt)),
        Value(user, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexes::LastModifiedBy)),
        Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexes::Version)),
        Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexes::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysIndexes::DeletedAt))
        );

        auto result = table->InsertRow(executionContext, fields);

        std::cout << "Inserted index: "<< indexName << " to master db" << std::endl;

        return result;
    }

    Errors::RuntimeStatus SystemCatalog::InsertIndexColumnToMasterDb(
    const ExecutionContext& executionContext,
    const Int indexId,
    const Int columnId,
    const SmallInt & ordinalPosition,
    const bool  isIncluded,
    const Int version,
    const bool isDeleted) const{
    StorageTypes::Table* table = this->masterDb->OpenTable(CatalogTables::SysIndexColumns);
    const auto currentDate = DataTypes::DateTime::Now();

    const auto fields = DataStructures::PolymorphicArray<Value>::From(executionContext.GetAllocator(),
        Value(indexId, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexColumns::IndexId)),
        Value(columnId, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexColumns::ColumnId)),
        Value(ordinalPosition, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexColumns::OrdinalPosition)),
        Value(isIncluded, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexColumns::IsIncluded)),
        Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexColumns::Version)),
        Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexColumns::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysIndexColumns::DeletedAt))
    );

    auto result = table->InsertRow(executionContext, fields);

        std::cout << "Inserted index column to master db" << std::endl;

    return result;
    }

    Errors::RuntimeStatus SystemCatalog::InsertConstraintToMasterDb(
        const ExecutionContext& executionContext,
        const Int tableId,
        const DataTypes::StringView&  constraintName,
        const Headers::ConstraintType& constraintType,
        const bool  isDisabled,
        const Int *constraintIndexId,
        const DataTypes::StringView&  user,
        const Int version,
        const bool isDeleted
    ) const{

    auto* table = this->masterDb->OpenTable(CatalogTables::SysConstraints);

    const auto currentDate = DataTypes::DateTime::Now();

    auto constraintField = constraintIndexId != nullptr
        ? Value(*constraintIndexId, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraints::IndexId))
        : Value::Null(static_cast<column_index_t>(SysConstraints::IndexId));

    const auto fields = DataStructures::PolymorphicArray<Value>::From(executionContext.GetAllocator(),
        Value(tableId, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraints::TableId)),
        Value(constraintName, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraints::Name)),
        Value(static_cast<TinyInt>(constraintType), executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraints::Type)),
        Value(isDisabled, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraints::IsDisabled)),
        std::move(constraintField),
        Value(currentDate, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraints::CreatedAt)),
        Value(currentDate, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraints::LastModifiedAt)),
        Value(user, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraints::LastModifiedBy)),
        Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraints::Version)),
        Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraints::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysConstraints::DeletedAt))
    );

    auto result = table->InsertRow(executionContext, fields);

    std::cout << "Inserted constraint: "<< constraintName <<" to master db" << std::endl;

    return result;
    }

    Errors::RuntimeStatus SystemCatalog::InsertConstraintColumnToMasterDb(
    const ExecutionContext& executionContext,
    const Int constraintId,
    const Int columnId,
    const Int ordinalPosition,
    const Int version,
    const bool isDeleted
    ) const{

    auto* table = this->masterDb->OpenTable(CatalogTables::SysConstraintColumns);
    const auto currentDate = DataTypes::DateTime::Now();

    const auto fields = DataStructures::PolymorphicArray<Value>::From(executionContext.GetAllocator(),
        Value(constraintId, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraintColumns::ConstraintId)),
        Value(columnId, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraintColumns::ColumnId)),
        Value(ordinalPosition, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraintColumns::OrdinalPosition)),
        Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraintColumns::Version)),
        Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraintColumns::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysConstraintColumns::DeletedAt))
    );

    auto result = table->InsertRow(executionContext, fields);

        std::cout << "Inserted constraint column to master db" << std::endl;

    return result;
    }

    Errors::RuntimeStatus SystemCatalog::InsertIdentityColumnToMasterDb(
        const ExecutionContext& executionContext,
        const Int tableId,
        const Int columnId,
        const Int seedValue,
        const Int increment,
        const Int lastValue,
        const bool  isCached,
        const Int cacheBlock,
        const Int version,
        const bool isDeleted
    ) const{

        auto* table = this->masterDb->OpenTable(CatalogTables::SysIdentityColumns);
        const auto currentDate = DataTypes::DateTime::Now();

        const auto fields = DataStructures::PolymorphicArray<Value>::From(executionContext.GetAllocator(),
        Value(tableId, executionContext.GetAllocator(), static_cast<column_index_t>(SysIdentityColumns::TableId)),
        Value(columnId, executionContext.GetAllocator(), static_cast<column_index_t>(SysIdentityColumns::ColumnId)),
        Value(seedValue, executionContext.GetAllocator(), static_cast<column_index_t>(SysIdentityColumns::SeedValue)),
        Value(increment, executionContext.GetAllocator(), static_cast<column_index_t>(SysIdentityColumns::IncrementValue)),
        Value(lastValue, executionContext.GetAllocator(), static_cast<column_index_t>(SysIdentityColumns::LastValue)),
        Value(isCached, executionContext.GetAllocator(), static_cast<column_index_t>(SysIdentityColumns::IsCached)),
        Value(cacheBlock, executionContext.GetAllocator(), static_cast<column_index_t>(SysIdentityColumns::CacheBlock)),
        Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysIdentityColumns::Version)),
        Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysIdentityColumns::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysIdentityColumns::DeletedAt))
        );

    auto result = table->InsertRow(executionContext, fields);

        std::cout << "Inserted identity column to master db" << std::endl;

    return result;
    }

    Errors::RuntimeStatus SystemCatalog::InsertDefaultValuesToMasterDb(
    const ExecutionContext& executionContext,
    const Int columnId,
    const Value &value,
    const Int version,
    const bool isDeleted) const{

        auto* table = this->masterDb->OpenTable(CatalogTables::SysDefaultValues);
        const auto currentDate = DataTypes::DateTime::Now();

        const auto fields = DataStructures::PolymorphicArray<Value>::From(executionContext.GetAllocator(),
        Value(columnId, executionContext.GetAllocator(), static_cast<column_index_t>(SysDefaultValues::ColumnId)),
        Value(
            std::string(reinterpret_cast<const char*>(value.Data()), value.Size()),
            executionContext.GetAllocator(),
            static_cast<column_index_t>(SysDefaultValues::Value)
        ),
        Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysDefaultValues::Version)),
        Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysDefaultValues::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysDefaultValues::DeletedAt))
        );

        auto result = table->InsertRow(executionContext, fields);

        std::cout << "Inserted default value " << value << " to master db" << std::endl;

        return result;
    }

    Errors::RuntimeStatus SystemCatalog::InsertTableStatisticsToMasterDb(
    const ExecutionContext& executionContext,
    const Int tableId,
    const int64_t& rowCount,
    const Int rowSize,
    const Int pageCount
    ) const{

    StorageTypes::Table* table = this->masterDb->OpenTable(CatalogTables::SysTableStats);

    const auto fields = DataStructures::PolymorphicArray<Value>::From(executionContext.GetAllocator(),
        Value(tableId, executionContext.GetAllocator(), static_cast<column_index_t>(SysTableStats::TableId)),
        Value(rowCount, executionContext.GetAllocator(), static_cast<column_index_t>(SysTableStats::RowCount)),
        Value(rowSize, executionContext.GetAllocator(), static_cast<column_index_t>(SysTableStats::AvgRowSize)),
        Value(pageCount, executionContext.GetAllocator(), static_cast<column_index_t>(SysTableStats::PageCount)),
        Value(DataTypes::DateTime::Now(), executionContext.GetAllocator(), static_cast<column_index_t>(SysTableStats::LastUpdatedAt))
    );

    auto result = table->InsertRow(executionContext, fields);

    std::cout << "Inserted table stats for table with id: " << tableId << std::endl;

    return result;
    }

    Errors::RuntimeStatus SystemCatalog::InsertColumnStatisticsToMasterDb(
    const ExecutionContext& executionContext,
    const Int columnId,
    const int64_t &distinctCount,
    const int64_t &nullCount
    ) const{

    StorageTypes::Table* table = this->masterDb->OpenTable(CatalogTables::SysColumnStats);

    const auto fields = DataStructures::PolymorphicArray<Value>::From(executionContext.GetAllocator(),
        Value(columnId, executionContext.GetAllocator(), static_cast<column_index_t>(SysColumnStats::ColumnId)),
        Value(distinctCount, executionContext.GetAllocator(), static_cast<column_index_t>(SysColumnStats::DistinctCount)),
        Value::Null(static_cast<column_index_t>(SysColumnStats::MinimumValue)),
        Value::Null(static_cast<column_index_t>(SysColumnStats::MaximumValue)),
        Value(nullCount, executionContext.GetAllocator(), static_cast<column_index_t>(SysColumnStats::NullCount))
    );

    auto result = table->InsertRow(executionContext, fields);

    std::cout << "Inserted column stats for column with id: " << columnId << std::endl;

    return result;
    }

    Errors::RuntimeStatus SystemCatalog::InsertColumnHistogramsToMasterDb(
        const ExecutionContext& executionContext,
        const Int columnId,
        const Value &min,
        const Value &max,
        const Int rowCount,
        const BigInt distinctCount
    ) const {
        const auto* allocator = executionContext.GetAllocator();

        const auto fields = DataStructures::PolymorphicArray<Value>::From(allocator,
            Value(columnId, allocator, static_cast<column_index_t>(SysColumnHistograms::ColumnId)),
            Value(std::string(reinterpret_cast<const char*>(min.Data()), min.Size()), allocator, static_cast<column_index_t>(SysColumnHistograms::RangeStart)),
            Value(std::string(reinterpret_cast<const char*>(max.Data()), max.Size()), allocator, static_cast<column_index_t>(SysColumnHistograms::RangeEnd)),
            Value(rowCount, allocator, static_cast<column_index_t>(SysColumnHistograms::RowCount)),
            Value(distinctCount, allocator, static_cast<column_index_t>(SysColumnHistograms::DistinctCount))
        );

        auto* table = this->masterDb->OpenTable(CatalogTables::SysColumnHistograms);

        auto result = table->InsertRow(executionContext, fields);
        std::cout << "Inserted histogram Bucket for column: " << columnId << std::endl;
        return result;
    }

    Errors::RuntimeStatus SystemCatalog::InsertIndexStatisticsToMasterDb(
        const ExecutionContext& executionContext,
        const Int tableId,
        const Int indexId,
        const BigInt leafPages,
        const TinyInt depth,
        const DataTypes::Decimal &averageFragmentation
    ) const{
        auto* table = this->masterDb->OpenTable(CatalogTables::SysIndexStats);

        const auto fields = DataStructures::PolymorphicArray<Value>::From(executionContext.GetAllocator(),
            Value(tableId, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexStats::TableId)),
            Value(indexId, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexStats::IndexId)),
            Value(leafPages, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexStats::LeafPages)),
            Value(depth, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexStats::Depth)),
            Value(averageFragmentation, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexStats::AverageFragmentation)),
            Value(DataTypes::DateTime::Now(), executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexStats::LastUpdated))
        );

        auto result = table->InsertRow(executionContext, fields);

        std::cout << "Inserted index statistics for index: " << indexId << std::endl;

        return result;
    }

    Errors::RuntimeStatus SystemCatalog::InsertRoleToMasterDb(
        const ExecutionContext& executionContext,
        const DataTypes::StringView& roleName,
        const Security::Permission &permissions,
        const bool isSystem,
        const Int version,
        const bool isDeleted
    ) const{
        auto* table = this->masterDb->OpenTable(CatalogTables::SysRoles);
        const auto currentDate = DataTypes::DateTime::Now();

        static constexpr DataTypes::StringView LAST_MODIFIED_BY = "system";

        const auto fields = DataStructures::PolymorphicArray<Value>::From(executionContext.GetAllocator(),
            Value(roleName, executionContext.GetAllocator(), static_cast<column_index_t>(SysRoles::RoleName)),
            Value(static_cast<Int>(permissions), executionContext.GetAllocator(), static_cast<column_index_t>(SysRoles::Permissions)),
            Value(isSystem, executionContext.GetAllocator(), static_cast<column_index_t>(SysRoles::IsSystemRole)),
            Value(DataTypes::DateTime::Now(), executionContext.GetAllocator(), static_cast<column_index_t>(SysRoles::CreatedAt)),
            Value(DataTypes::DateTime::Now(), executionContext.GetAllocator(), static_cast<column_index_t>(SysRoles::LastModifiedAt)),
            Value(LAST_MODIFIED_BY, executionContext.GetAllocator(), static_cast<column_index_t>(SysRoles::LastModifiedBy)),
            Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysRoles::Version)),
            Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysRoles::IsDeleted)),
            Value::Null(static_cast<column_index_t>(SysRoles::DeletedAt))
        );

        auto result = table->InsertRow(executionContext, fields);
        std::cout << "Inserted Role " << roleName << std::endl;
        return result;
    }

    Errors::RuntimeStatus SystemCatalog::InsertUserToMasterDb(
        const ExecutionContext& executionContext,
        const DataTypes::StringView& username,
        const DataTypes::StringView& passwordHash,
        const Int roleId,
        const bool isActive,
        const Int version,
        const bool isDeleted
    ) const{

        auto* table = this->masterDb->OpenTable(CatalogTables::SysUsers);
        const auto currentDate = DataTypes::DateTime::Now();

        static constexpr DataTypes::StringView LAST_MODIFIED_BY = "system";

        const auto fields = DataStructures::PolymorphicArray<Value>::From(executionContext.GetAllocator(),
            Value(username, executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::UserName)),
            Value(passwordHash, executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::PasswordHash)),
            Value(roleId, executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::RoleId)),
            Value(isActive, executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::IsActive)),
            Value(DataTypes::DateTime::Now(), executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::CreatedAt)),
            Value(DataTypes::DateTime::Now(), executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::LastModifiedAt)),
            Value(LAST_MODIFIED_BY, executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::LastModifiedBy)),
            Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::Version)),
            Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::IsDeleted)),
            Value::Null(static_cast<column_index_t>(SysUsers::DeletedAt))
        );

        auto result = table->InsertRow(executionContext, fields);

        std::cout << "Inserted User " << username << std::endl;

        return result;
    }

    DataStructures::PolymorphicArray<Security::Role> SystemCatalog::SelectRoles(const ::Memory::IAllocator* allocator) const{
        DataStructures::PolymorphicArray<StorageTypes::RID> rows(allocator);

        auto* table = this->masterDb->OpenTable(CatalogTables::SysRoles);
        table->SystemClusteredIndexScan(allocator, &rows, nullptr);

        DataStructures::PolymorphicArray<Security::Role> roles(allocator, rows.Size());

        for (const auto& row : rows) {
            const auto materializedRow = table->MaterializeFromPage(allocator, &row);
            const auto& data = materializedRow.Data();

            const auto view = data[static_cast<column_index_t>(SysRoles::RoleName)].AsStringView();

            auto role = Security::Role(
                data[static_cast<column_index_t>(SysRoles::RoleId)].AsInt(),
                DataTypes::String::FromView(view, allocator),
                static_cast<Security::Permission>(data[static_cast<column_index_t>(SysRoles::Permissions)].AsInt()),
                data[static_cast<column_index_t>(SysRoles::IsSystemRole)].AsBool()
            );

            roles.Push(std::move(role));
        }

        return roles;
    }

    DataStructures::PolymorphicArray<Security::User> SystemCatalog::SelectUsers(const ::Memory::IAllocator* allocator) const{
        DataStructures::PolymorphicArray<StorageTypes::RID> rows(allocator);

        auto* table = this->masterDb->OpenTable(CatalogTables::SysUsers);
        table->SystemClusteredIndexScan(allocator, &rows, nullptr);

        DataStructures::PolymorphicArray<Security::User> users(allocator, rows.Size());

        for (const auto& row : rows) {
            const auto materializedRow = table->MaterializeFromPage(allocator, &row);
            const auto& data = materializedRow.Data();

            const auto view = data[static_cast<column_index_t>(SysUsers::UserName)].AsStringView();
            const auto passwordHash = data[static_cast<column_index_t>(SysUsers::PasswordHash)].AsStringView();

            auto user = Security::User(
                data[static_cast<column_index_t>(SysUsers::UserId)].AsInt(),
                DataTypes::String::FromView(view, allocator),
                DataTypes::String::FromView(passwordHash, allocator),
                data[static_cast<column_index_t>(SysUsers::RoleId)].AsInt(),
                nullptr,
                data[static_cast<column_index_t>(SysUsers::IsActive)].AsBool()
            );

            users.Push(std::move(user));
        }

        return users;
    }

    bool SystemCatalog::DatabaseExists(const ::Memory::IAllocator* allocator, const DataTypes::StringView& dbName) const{
        auto* sysDatabases = this->masterDb->OpenTable(CatalogTables::SysDatabases);
        DataStructures::PolymorphicArray<StorageTypes::RID> selectedDatabases(allocator);

        auto columnExpr = Expressions::ColumnExpression(static_cast<column_index_t>(SysDatabases::Name), DataType::String);
        auto constantExpr = Expressions::ConstantExpression(Value(dbName, allocator, static_cast<column_index_t>(SysDatabases::Name)));
        Expressions::BinaryExpression binaryExpr(&columnExpr, &constantExpr, Expressions::BinaryOperator::EqualIgnoreOrdinalCase);
        Expressions::BindExpressionKernel(&binaryExpr, Constants::ExecutionMode::Row);

        sysDatabases->SystemClusteredIndexScan(allocator, &selectedDatabases, &binaryExpr);

        return !selectedDatabases.Empty();
}

    Headers::DatabaseHeader SystemCatalog::SelectDatabase(const ::Memory::IAllocator* allocator, const DataTypes::StringView& name) const{
        auto columnExpr = Expressions::ColumnExpression(static_cast<column_index_t>(SysDatabases::Name), DataType::String);
        auto constantExpr = Expressions::ConstantExpression(Value(name, allocator, static_cast<column_index_t>(SysDatabases::Name)));

        Expressions::BinaryExpression binaryExpr(
                &columnExpr,
            &constantExpr,
            Expressions::BinaryOperator::EqualIgnoreOrdinalCase
        );
        Expressions::BindExpressionKernel(&binaryExpr, Constants::ExecutionMode::Row);

        auto* tablePtr = this->masterDb->OpenTable(CatalogTables::SysDatabases);
        DataStructures::PolymorphicArray<StorageTypes::RID> selectedDatabases(allocator);

        tablePtr->SystemClusteredIndexScan(allocator, &selectedDatabases, &binaryExpr);

        if (selectedDatabases.Empty()) return { .additionalInfo = Headers::AuditInformation() };

        return SystemCatalog::ToDatabaseHeader(allocator, &selectedDatabases[0], tablePtr);
    }

Headers::DatabaseHeader SystemCatalog::SelectDatabaseById(const ::Memory::IAllocator* allocator, const Int databaseId) const{
    auto* tablePtr = this->masterDb->OpenTable(CatalogTables::SysDatabases);
    DataStructures::PolymorphicArray<StorageTypes::RID> selectedDatabases(allocator);

    DataTypes::Indexing::Key key(allocator);
    key.InsertKey(DataTypes::Indexing::Key(&databaseId, sizeof(databaseId), DataType::Int, allocator));

    tablePtr->SystemClusteredIndexSeek(allocator, &selectedDatabases, key, nullptr);

    if (selectedDatabases.Empty())
    return {.additionalInfo = Headers::AuditInformation()};

    return SystemCatalog::ToDatabaseHeader(allocator, &selectedDatabases[0], tablePtr);
}

DataStructures::PolymorphicArray<Headers::SchemaHeader> SystemCatalog::SelectSchemas(const ::Memory::IAllocator* allocator, const Int databaseId) const{
    auto* tablePtr = this->masterDb->OpenTable(CatalogTables::SysSchemas);
    DataStructures::PolymorphicArray<StorageTypes::RID> selectedSchemas(allocator, 2);

    DataTypes::Indexing::Key key(allocator);
    key.InsertKey(DataTypes::Indexing::Key(&databaseId, sizeof(databaseId), DataType::Int, allocator));

    tablePtr->SystemClusteredIndexSeek(allocator, &selectedSchemas, key, nullptr);

    if (selectedSchemas.Empty()) return {};

        DataStructures::PolymorphicArray<Headers::SchemaHeader> schemas(allocator, selectedSchemas.Size());

    for (const auto& row : selectedSchemas)
        schemas.Push(SystemCatalog::ToSchemaHeader(allocator, &row, tablePtr));

        return schemas;
    }

    Dictionary<DataTypes::String, Headers::SchemaHeader> SystemCatalog::SelectSchemasToDictionary(const ::Memory::IAllocator* allocator, const Int databaseId) const{
    const auto& schemas = this->SelectSchemas(allocator, databaseId);

    Dictionary<DataTypes::String, Headers::SchemaHeader> selectedSchemas;

    for (const auto& schema : schemas)
        selectedSchemas.Add(schema.name, schema);

    return selectedSchemas;
    }

    bool SystemCatalog::SchemaExists(
        const ::Memory::IAllocator* allocator,
        const Int databaseId,
        const DataTypes::StringView& schema,
        Int* schemaId
    ) const{
        DataStructures::PolymorphicArray<StorageTypes::RID> selectedSchemas(allocator, 1);
        auto* table = this->masterDb->OpenTable(CatalogTables::SysSchemas);

        Expressions::ColumnExpression columnExpr(static_cast<column_index_t>(SysSchemas::Name), DataType::String);
        Expressions::ConstantExpression constantExpr(Value(schema, allocator, static_cast<column_index_t>(SysSchemas::Name)));
        Expressions::BinaryExpression binaryExpr(&columnExpr, &constantExpr, Expressions::BinaryOperator::EqualIgnoreOrdinalCase);
        Expressions::BindExpressionKernel(&binaryExpr, Constants::ExecutionMode::Row);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&databaseId, sizeof(databaseId), DataType::Int, allocator));

        table->SystemClusteredIndexSeek(allocator, &selectedSchemas, key, &binaryExpr);
        if (selectedSchemas.Empty()) return false;

        const auto materializedRow = table->MaterializeFromPage(allocator, &selectedSchemas[0]);
        *schemaId = materializedRow.GetColumnAt(static_cast<column_index_t>(SysSchemas::SchemaId)).AsInt();

        return true;
    }

    DataStructures::PolymorphicArray<Headers::TableHeader> SystemCatalog::SelectTables(
        const ::Memory::IAllocator* allocator,
        const DataTypes::StringView& dbName
    ) const{
        const auto databaseHeader = this->SelectDatabase(allocator, dbName);
        return this->SelectTables(allocator, databaseHeader.id);
    }

    DataStructures::PolymorphicArray<Headers::TableHeader> SystemCatalog::SelectTables(
        const ::Memory::IAllocator* allocator,
        const Int databaseId
    ) const{
        DataStructures::PolymorphicArray<StorageTypes::RID> selectedTables(allocator, 10);

        auto* tablePtr = this->masterDb->OpenTable(CatalogTables::SysTables);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&databaseId, sizeof(databaseId), DataType::Int, allocator));

        tablePtr->SystemClusteredIndexSeek(allocator, &selectedTables, key, nullptr);

        if (selectedTables.Empty())
            return {};

        DataStructures::PolymorphicArray<Headers::TableHeader> selectedTableHeaders(allocator, selectedTables.Size());
        for (const auto& row : selectedTables)
            selectedTableHeaders.Push(SystemCatalog::ToTableHeader(allocator, &row, tablePtr));

        std::ranges::sort(selectedTableHeaders,
            [](const Headers::TableHeader& a, const Headers::TableHeader& b) {
                return a.ordinalPosition < b.ordinalPosition;
            }
        );

        return selectedTableHeaders;
    }

    Headers::TableHeader SystemCatalog::SelectTable(
        const ::Memory::IAllocator* allocator,
        const DataTypes::StringView& dbName,
        const DataTypes::StringView& tableName
    ) const{
        const auto databaseHeader = this->SelectDatabase(allocator, dbName);
        return this->SelectTable(allocator, databaseHeader.id, tableName, Constants::DEFAULT_SCHEMA_NAME.Data());
    }

    Headers::TableHeader SystemCatalog::SelectTable(
        const ::Memory::IAllocator* allocator,
        const Int databaseId,
        const DataTypes::StringView& tableName,
        const DataTypes::StringView& schema
    ) const{
        Int schemaId = -1;
        if (!this->SchemaExists(allocator, databaseId, schema, &schemaId) && !schema.Empty())
            return {};

        DataStructures::PolymorphicArray<StorageTypes::RID> selectedTables(allocator);
        auto* sysTablesPtr = this->masterDb->OpenTable(CatalogTables::SysTables);

        auto leftColumnExpr = Expressions::ColumnExpression(static_cast<column_index_t>(SysTables::SchemaId), DataType::Int);
        auto leftConstantExpr = Expressions::ConstantExpression(Value(schemaId, allocator, static_cast<column_index_t>(SysTables::SchemaId)));

        auto leftBinaryExpr = Expressions::BinaryExpression(&leftColumnExpr, &leftConstantExpr, Expressions::BinaryOperator::Equal);

        auto rightColumnExpr = Expressions::ColumnExpression(static_cast<column_index_t>(SysTables::Name), DataType::String);
        auto rightConstantExpr = Expressions::ConstantExpression(Value(tableName, allocator, static_cast<column_index_t>(SysTables::Name)));

        auto rightBinaryExpr = Expressions::BinaryExpression(&rightColumnExpr, &rightConstantExpr, Expressions::BinaryOperator::EqualIgnoreOrdinalCase);

        auto logicalExpr = Expressions::LogicalExpression(&leftBinaryExpr, &rightBinaryExpr, Expressions::LogicalType::And);

        Expressions::BindExpressionKernel(&logicalExpr, Constants::ExecutionMode::Row);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&databaseId, sizeof(databaseId), DataType::Int, allocator));

        sysTablesPtr->SystemClusteredIndexSeek(allocator, &selectedTables, key, &logicalExpr);

        if (selectedTables.Empty())
            return {};

        return SystemCatalog::ToTableHeader(allocator, &selectedTables[0], sysTablesPtr);
    }

    DataStructures::PolymorphicArray<Headers::ConstraintsHeader> SystemCatalog::SelectConstraints(
        const ::Memory::IAllocator* allocator,
        const Int tableId
    ) const{
        DataStructures::PolymorphicArray<StorageTypes::RID> selectedConstraints(allocator);
        auto* constraintsTable = this->masterDb->OpenTable(CatalogTables::SysConstraints);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int, allocator));

        constraintsTable->SystemClusteredIndexSeek(allocator, &selectedConstraints, key, nullptr);

        if (selectedConstraints.Empty()) return {};

        DataStructures::PolymorphicArray<Headers::ConstraintsHeader> selectedConstraintsHeader(allocator, selectedConstraints.Size());

        for (const auto& row : selectedConstraints) {
            const auto materializedRow = constraintsTable->MaterializeFromPage(allocator, &row);
            const auto& data = materializedRow.Data();

            auto constraintColumns = this->SelectConstraintColumnsByConstraintId(allocator, data[0].AsInt());

            const auto indexId =(data[static_cast<column_index_t>(SysConstraints::IndexId)].IsNull())
                ? -1
                : data[static_cast<column_index_t>(SysConstraints::IndexId)].AsInt();

            Headers::IndexHeader index;
            if(indexId != -1)
                index = this->SelectIndexById(allocator, indexId);

            selectedConstraintsHeader.Push(SystemCatalog::ToConstraintsHeader(allocator, &row, constraintsTable, constraintColumns, index));
        }

        return selectedConstraintsHeader;
    }

    Headers::ColumnHeader SystemCatalog::SelectColumnById(
        const ::Memory::IAllocator* allocator,
        const Int tableId,
        const Int columnId
    ) const{
        DataStructures::PolymorphicArray<StorageTypes::RID> selectedColumns(allocator, 1);
        auto* sysColumns = this->masterDb->OpenTable(CatalogTables::SysColumns);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int, allocator));
        key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int, allocator));

        sysColumns->SystemClusteredIndexSeek(allocator, &selectedColumns, key, nullptr);

        if (selectedColumns.Empty())
            return {.additionalInfo = Headers::AuditInformation()};

        return SystemCatalog::ToColumnHeader(allocator, &selectedColumns.Start(), sysColumns);
    }

    DataStructures::PolymorphicArray<Headers::ColumnHeader> SystemCatalog::SelectColumns(
        const ::Memory::IAllocator* allocator,
        const Int tableId
    ) const{
        DataStructures::PolymorphicArray<StorageTypes::RID> selectedColumns(allocator, 10);
        auto* sysColumns = this->masterDb->OpenTable(CatalogTables::SysColumns);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int, allocator));

        sysColumns->SystemClusteredIndexSeek(allocator, &selectedColumns, key, nullptr);

        if (selectedColumns.Empty()) return {};

        DataStructures::PolymorphicArray<Headers::ColumnHeader> selectedColumnHeaders(allocator, selectedColumns.Size());
        for (const auto& row : selectedColumns)
            selectedColumnHeaders.Push(SystemCatalog::ToColumnHeader(allocator, &row, sysColumns));

        std::ranges::sort(selectedColumnHeaders,
        [](const Headers::ColumnHeader& a, const Headers::ColumnHeader& b) {
                return a.ordinalPosition < b.ordinalPosition;
            }
        );

        return selectedColumnHeaders;
    }

    Dictionary<DataTypes::String, Headers::ColumnHeader> SystemCatalog::SelectColumnsToDictionary(
        const ::Memory::IAllocator* allocator,
        const Int tableId
    ) const{
        const auto columns = this->SelectColumns(allocator, tableId);

        Dictionary<DataTypes::String, Headers::ColumnHeader> selectedColumns;
        for (const auto& column: columns)
            selectedColumns.Add(column.name.ToLower(), column);

        return selectedColumns;
    }

    DataStructures::PolymorphicArray<Headers::IndexHeader> SystemCatalog::SelectIndexes(
        const ::Memory::IAllocator* allocator,
        const Int tableId
    ) const{
        auto* sysIndexes = this->masterDb->OpenTable(CatalogTables::SysIndexes);
        DataStructures::PolymorphicArray<StorageTypes::RID> selectedIndexes(allocator);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int, allocator));

        sysIndexes->SystemClusteredIndexSeek(allocator, &selectedIndexes, key, nullptr);

        DataStructures::PolymorphicArray<Headers::IndexHeader> selectedIndexHeaders(allocator, selectedIndexes.Size());
        for (const auto& row : selectedIndexes)
            selectedIndexHeaders.Push(SystemCatalog::ToIndexHeader(allocator, &row, sysIndexes));

        //get the clustered first
        std::ranges::sort(selectedIndexHeaders,
        [](const Headers::IndexHeader& a, const Headers::IndexHeader& b) {
            return a.isClustered > b.isClustered;
        });
        return selectedIndexHeaders;
    }

    Headers::IndexHeader SystemCatalog::SelectIndexById(const ::Memory::IAllocator* allocator, const Int indexId) const{
        auto* sysIndexes = this->masterDb->OpenTable(CatalogTables::SysIndexes);
        DataStructures::PolymorphicArray<StorageTypes::RID> selectedIndexes(allocator, 1);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&indexId, sizeof(indexId), DataType::Int, allocator));

        sysIndexes->SystemClusteredIndexSeek(allocator, &selectedIndexes, key, nullptr);

        if(selectedIndexes.Empty()) return {.additionalInfo = Headers::AuditInformation()};

        auto indexColumns = this->SelectIndexColumnsByIndexId(allocator, indexId);

        auto header = SystemCatalog::ToIndexHeader(allocator, &selectedIndexes[0], sysIndexes);
        header.columns = std::move(indexColumns);

        return header;
    }

    DataStructures::PolymorphicArray<Headers::IndexColumnsHeader> SystemCatalog::SelectIndexColumnsByIndexId(
        const ::Memory::IAllocator* allocator,
        const Int indexId
    ) const{
        auto* sysIndexes = this->masterDb->OpenTable(CatalogTables::SysIndexColumns);
        DataStructures::PolymorphicArray<StorageTypes::RID> rows(allocator);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&indexId, sizeof(indexId), DataType::Int, allocator));

        sysIndexes->SystemClusteredIndexSeek(allocator, &rows, key, nullptr);

        if(rows.Empty()) return {};

        DataStructures::PolymorphicArray<Headers::IndexColumnsHeader> indexColumns(allocator, rows.Size());
        for (const auto& row : rows)
            indexColumns.Push(SystemCatalog::ToIndexColumnsHeader(allocator, &row, sysIndexes));

        //get them sorted by ordinal position
        std::ranges::sort(indexColumns,
        [](const Headers::IndexColumnsHeader& a, const Headers::IndexColumnsHeader& b) {
                return a.ordinalPosition < b.ordinalPosition;
        });

        return indexColumns;
    }

    Dictionary<Int, Headers::IndexColumnsHeader> SystemCatalog::SelectIndexColumnsByIndexIdToDictionary(
        const ::Memory::IAllocator* allocator,
        const Int indexId
    ) const{
        const auto indexColumns = this->SelectIndexColumnsByIndexId(allocator, indexId);

        Dictionary<Int, Headers::IndexColumnsHeader> indexColumnsDict;

        for (const auto& indexColumn : indexColumns)
            indexColumnsDict.Add(indexColumn.columnId, indexColumn);

        return indexColumnsDict;
    }

    DataStructures::PolymorphicArray<Headers::IdentityColumnsHeader> SystemCatalog::SelectIdentityColumnsByTableId(
        const ::Memory::IAllocator* allocator,
        const Int tableId
    ) const{
        auto* table = this->masterDb->OpenTable(CatalogTables::SysIdentityColumns);
        DataStructures::PolymorphicArray<StorageTypes::RID> rows(allocator, 10);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int, allocator));

        table->SystemClusteredIndexSeek(allocator, &rows, key, nullptr);

        if(rows.Empty()) return {};

        DataStructures::PolymorphicArray<Headers::IdentityColumnsHeader> columns(allocator, rows.Size());
        for (const auto& row : rows)
            columns.Push(SystemCatalog::ToIdentityColumnsHeader(allocator, &row, table));

        //get them sorted by ordinal position
        std::ranges::sort(columns,
            [](const Headers::IdentityColumnsHeader& a, const Headers::IdentityColumnsHeader& b) {
            return a.columnId > b.columnId;
            });

        return columns;
    }

    Dictionary<Int , Headers::IdentityColumnsHeader> SystemCatalog::SelectIdentityColumnsByTableIdToDictionary(
        const ::Memory::IAllocator* allocator,
        const Int tableId
    ) const{
        const auto columns = this->SelectIdentityColumnsByTableId(allocator, tableId);

        Dictionary<Int, Headers::IdentityColumnsHeader> dict;
        for(const auto& column : columns)
            dict.Add(column.columnId, column);

        return dict;
    }

    DataStructures::PolymorphicArray<Headers::ConstraintsColumnsHeader> SystemCatalog::SelectConstraintColumnsByConstraintId(
        const ::Memory::IAllocator* allocator,
        const Int constraintId
    ) const{
        auto* sysIndexes = this->masterDb->OpenTable(CatalogTables::SysConstraintColumns);
        DataStructures::PolymorphicArray<StorageTypes::RID> rows(allocator, 2);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&constraintId, sizeof(constraintId), DataType::Int, allocator));

        sysIndexes->SystemClusteredIndexSeek(allocator, &rows, key, nullptr);

        if(rows.Empty()) return {};

        DataStructures::PolymorphicArray<Headers::ConstraintsColumnsHeader> constraintColumns(allocator, rows.Size());
        for(const auto& row : rows)
            constraintColumns.Push(SystemCatalog::ToConstraintsColumnsHeader(allocator, &row, sysIndexes));

        std::ranges::sort(constraintColumns,
        [](const Headers::ConstraintsColumnsHeader& a, const Headers::ConstraintsColumnsHeader& b) {
                return a.ordinalPosition < b.ordinalPosition;
            }
        );

        return constraintColumns;
    }

    Dictionary<Int, Headers::ConstraintsColumnsHeader> SystemCatalog::SelectConstraintColumnsByConstraintIdToDictionary(
        const ::Memory::IAllocator* allocator,
        const Int constraintId
    ) const{
        const auto columns = this->SelectConstraintColumnsByConstraintId(allocator, constraintId);

        Dictionary<Int, Headers::ConstraintsColumnsHeader> constraintColumns;
        for (const auto& constraint: columns)
            constraintColumns.Add(constraint.columnId, constraint);

        return constraintColumns;
    }

    Headers::DefaultValuesHeader SystemCatalog::SelectDefaultValueByColumnId(
        const ::Memory::IAllocator* allocator,
        const Int columnId
    ) const{
        auto* sysValues = this->masterDb->OpenTable(CatalogTables::SysDefaultValues);
        DataStructures::PolymorphicArray<StorageTypes::RID> rows(allocator, 1);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int, allocator));

        sysValues->SystemClusteredIndexSeek(allocator, &rows, key, nullptr);

        if(rows.Empty()) return {.additionalInfo = Headers::AuditInformation()};

        return SystemCatalog::ToDefaultValuesHeader(allocator, &rows[0], sysValues);
    }

    Headers::TableStatistics SystemCatalog::SelectTableStatisticsById(
        const ::Memory::IAllocator* allocator,
        const Int tableId
    ) const{
        auto* sysIndexes = this->masterDb->OpenTable(CatalogTables::SysTableStats);
        DataStructures::PolymorphicArray<StorageTypes::RID> rows(allocator, 1);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int, allocator));

        sysIndexes->SystemClusteredIndexSeek(allocator, &rows, key, nullptr);

        if (rows.Empty()) return {};

        return SystemCatalog::ToTableStatistics(allocator, &rows.Start(), sysIndexes);
    }

    Headers::ColumnStatistics SystemCatalog::SelectColumnStatisticsById(
        const ::Memory::IAllocator* allocator,
        const Int columnId,
        const DataType columnType
    ) const{
        auto* sysColumnStats = this->masterDb->OpenTable(CatalogTables::SysColumnStats);
        DataStructures::PolymorphicArray<StorageTypes::RID> rows(allocator, 1);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int, allocator));

        sysColumnStats->SystemClusteredIndexSeek(allocator, &rows, key, nullptr);

        if (rows.Empty()) return Headers::ColumnStatistics();

        return SystemCatalog::ToColumnStatistics(allocator, &rows.Start(), sysColumnStats, columnType);
    }

    DataStructures::PolymorphicArray<Headers::ColumnHistograms> SystemCatalog::SelectColumnHistogramsByColumnId(
        const ::Memory::IAllocator* allocator,
        const Int tableId,
        const Int columnId
    ) const {
        auto columnHeader = this->SelectColumnById(allocator, tableId, columnId);

        DataStructures::PolymorphicArray<Headers::ColumnHistograms> result(allocator, NUMBER_OF_HISTOGRAM_BUCKETS);
        auto* table = this->masterDb->OpenTable(CatalogTables::SysColumnHistograms);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int, allocator));

        DataStructures::PolymorphicArray<StorageTypes::RID> rows(allocator, NUMBER_OF_HISTOGRAM_BUCKETS);
        table->SystemClusteredIndexSeek(allocator, &rows, key, nullptr);

        for (const auto& row : rows)
            result.Push(SystemCatalog::ToColumnHistograms(allocator, &row, table, static_cast<DataType>(columnHeader.dataType)));

        return result;
    }

    DataStructures::PolymorphicArray<Headers::IndexStatistics> SystemCatalog::SelectIndexStatisticsByTableId(
        const ::Memory::IAllocator* allocator,
        const Int tableId
    ) const {
        DataStructures::PolymorphicArray<Headers::IndexStatistics> result(allocator);
        auto* table = this->masterDb->OpenTable(CatalogTables::SysIndexStats);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int, allocator));

        DataStructures::PolymorphicArray<StorageTypes::RID> rows(allocator);
        table->SystemClusteredIndexSeek(allocator, &rows, key, nullptr);

        for (const auto& row : rows){
            result.Push(SystemCatalog::ToIndexStatistics(allocator, &row, table));
        }

        return result;
    }

    void SystemCatalog::UpdateIdentityByColumnId(
        const ::Memory::IAllocator* allocator,
        const Int tableId,
        const Int columnId,
        const BigInt lastValue
    )const{
        auto* table = this->masterDb->OpenTable(CatalogTables::SysIdentityColumns);

        const auto updates = DataStructures::PolymorphicArray<Value>::From(
            allocator,
            Value(lastValue, allocator, static_cast<column_index_t>(SysIdentityColumns::LastValue))
        );

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int, allocator));
        key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int, allocator));

        const auto _ = table->SystemClusteredIndexSeekUpdate(allocator, key, updates);
    }

    void SystemCatalog::UpdateTableStatisticsById(
        const ::Memory::IAllocator* allocator,
        const Int tableId,
        const BigInt rowCount,
        const Int rowSize,
        const Int pageCount
    ) const{

        const auto updates = DataStructures::PolymorphicArray<Value>::From(
            allocator,
            Value(rowCount, allocator, static_cast<column_index_t>(SysTableStats::RowCount)),
            Value(rowSize, allocator, static_cast<column_index_t>(SysTableStats::AvgRowSize)),
            Value(pageCount, allocator, static_cast<column_index_t>(SysTableStats::PageCount)),
            Value(DataTypes::DateTime::Now(), allocator, static_cast<column_index_t>(SysTableStats::LastUpdatedAt))
        );

        auto* table = this->masterDb->OpenTable(CatalogTables::SysTableStats);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int, allocator));

        const auto _ = table->SystemClusteredIndexSeekUpdate(allocator, key, updates);
    }

    void SystemCatalog::UpdateColumnStatisticsById(
        const ::Memory::IAllocator* allocator,
        const Int columnId,
        const BigInt distinctCount,
        const BigInt nullCount,
        const Value& min,
        const Value& max
    ) const{

        const auto updates = DataStructures::PolymorphicArray<Value>::From(
            allocator,
            Value(distinctCount, allocator, static_cast<column_index_t>(SysColumnStats::DistinctCount)),
            Value(nullCount, allocator, static_cast<column_index_t>(SysColumnStats::NullCount)),
            Value(
            std::string(reinterpret_cast<const char*>(min.Data()), min.Size()),
                allocator,
                static_cast<column_index_t>(SysColumnStats::MinimumValue)
            ),
            Value(
                std::string(reinterpret_cast<const char*>(max.Data()), max.Size()),
                allocator,
                static_cast<column_index_t>(SysColumnStats::MaximumValue)
            )
        );

        auto* table = this->masterDb->OpenTable(CatalogTables::SysColumnStats);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int, allocator));

        const auto _ = table->SystemClusteredIndexSeekUpdate(allocator, key, updates);
    }

    void SystemCatalog::UpdateIndexStatisticsById(
        const ::Memory::IAllocator* allocator,
        const Int tableId,
        const Int indexId,
        const BigInt leafPages,
        const TinyInt depth,
        const DataTypes::Decimal &averageFragmentation
    ) const {
        const auto updates = DataStructures::PolymorphicArray<Value>::From(
            allocator,
            Value(leafPages, allocator, static_cast<column_index_t>(SysIndexStats::LeafPages)),
            Value(depth, allocator, static_cast<column_index_t>(SysIndexStats::Depth)),
            Value(averageFragmentation, allocator, static_cast<column_index_t>(SysIndexStats::AverageFragmentation)),
            Value(DataTypes::DateTime::Now(), allocator, static_cast<column_index_t>(SysIndexStats::LastUpdated))
        );

        auto* table = this->masterDb->OpenTable(CatalogTables::SysIndexStats);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int, allocator));
        key.InsertKey(DataTypes::Indexing::Key(&indexId, sizeof(indexId), DataType::Int, allocator));

        const auto _ = table->SystemClusteredIndexSeekUpdate(allocator, key, updates);
    }

    Errors::RuntimeStatus SystemCatalog::UpdateHistogramBucket(
        const ::Memory::IAllocator* allocator,
        const Int columnId,
        const Int histogramId,
        const Value& min,
        const Value& max,
        const Int rowCount,
        const BigInt& distinctCount
    ) const{
        const auto updates = DataStructures::PolymorphicArray<Value>::From(
            allocator,
            Value(
            std::string(reinterpret_cast<const char*>(min.Data()), min.Size()),
                allocator,
                static_cast<column_index_t>(SysColumnHistograms::RangeStart)
            ),
            Value(
            std::string(reinterpret_cast<const char*>(max.Data()), max.Size()),
                allocator,
                static_cast<column_index_t>(SysColumnHistograms::RangeEnd)
            ),
            Value(rowCount, allocator, static_cast<column_index_t>(SysColumnHistograms::RowCount)),
            Value(distinctCount, allocator, static_cast<column_index_t>(SysColumnHistograms::DistinctCount))
        );

        auto* table = this->masterDb->OpenTable(CatalogTables::SysColumnHistograms);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int, allocator));
        key.InsertKey(DataTypes::Indexing::Key(&histogramId, sizeof(histogramId), DataType::Int, allocator));

        auto result = table->SystemClusteredIndexSeekUpdate(allocator, key, updates);

        std::cout << "Updated histogram Bucket for column: " << columnId << " and id: " << histogramId << std::endl;

        return result;
    }

    Errors::RuntimeStatus SystemCatalog::UpdateColumnById(
        const ::Memory::IAllocator* allocator,
        const Int columnId,
        const DataStructures::PolymorphicArray<Value> &updates
    ) const{
        auto* table = this->masterDb->OpenTable(CatalogTables::SysColumns);

        DataTypes::Indexing::Key key(allocator);
        key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int, allocator));

        return table->SystemClusteredIndexSeekUpdate(allocator, key, updates);
    }

    Errors::RuntimeStatus SystemCatalog::UpdateUserById(
        const ExecutionContext& context,
        const DataTypes::StringView& username,
        const Int userId,
        const Int roleId
    ) const {
        auto* table = this->masterDb->OpenTable(CatalogTables::SysUsers);

        const auto currentDate = DataTypes::DateTime::Now();

        const auto updates = DataStructures::PolymorphicArray<Value>::From(
            context.GetAllocator(),
            Value(roleId, context.GetAllocator(), static_cast<column_index_t>(SysUsers::RoleId)),
            Value(currentDate, context.GetAllocator(), static_cast<column_index_t>(SysUsers::LastModifiedAt)),
            Value(username, context.GetAllocator(), static_cast<column_index_t>(SysUsers::LastModifiedBy))
        );

        DataTypes::Indexing::Key key(context.GetAllocator());
        key.InsertKey(DataTypes::Indexing::Key(&userId, sizeof(userId), DataType::Int, context.GetAllocator()));

        return table->ClusteredIndexSeekUpdate(context, key, updates);
    }
}