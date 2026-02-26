#include "../../include/SystemDatabases/SystemCatalog.h"

#include <fstream>

#include "../../include/SystemDatabases/CatalogSchema.h"
#include "../../include/Database.h"

#include <iostream>
#include <nlohmann/json.hpp>

#include "DataStorage/Table.h"
#include "Managers/GlobalMemoryManager.h"

namespace Headers {
 void from_json(const nlohmann::json& j, sysColumn& c) {
   j.at("name").get_to(c.name);
   j.at("type").get_to(c.type);

  if (j.contains("size"))
   j.at("size").get_to(c.size);
  if (j.contains("default"))
   j.at("default").get_to(c._default);
  if (j.contains("nullable"))
   j.at("nullable").get_to(c.nullable);
  if (j.contains("hasIdentity"))
   j.at("hasIdentity").get_to(c.hasIdentity);
 }

 void from_json(const nlohmann::json& j, sysTable& t) {
   j.at("name").get_to(t.name);
   j.at("id").get_to(t.id);
   j.at("columns").get_to(t.columns);
   j.at("primaryKey").get_to(t.primaryKey);
 }
}

namespace DatabaseEngine {
    SystemCatalog::SystemCatalog() {
        this->masterDb = nullptr;
    }

    SystemCatalog::~SystemCatalog() = default;

    void SystemCatalog::ReadConfiguration(const std::string_view configPath) {
        std::ifstream file(configPath.data());

        if (!file.is_open())
            throw std::runtime_error("System Tables file: " + std::string(configPath) + " could not be opened");

        nlohmann::json jsonFile;

        try {
            file >> jsonFile;
        }
        catch (std::exception &e)
        {
            throw std::runtime_error(e.what());
        }

        this->sysDbName = jsonFile.at("db_name");
        this->sysDbPath = jsonFile.at("db_path");

        jsonFile.at("tables").get_to(this->sysTables);
    }

    bool SystemCatalog::CatalogExists()const{
        return std::filesystem::exists(static_cast<std::filesystem::path>(this->sysDbPath));
    }

    void SystemCatalog::UseCatalogDatabase() {
        this->masterDb = AllocateMiscEntity<Database>(this->sysDbName, this->sysTables);
        this->masterDb->GetColumnsHeaders();
        this->masterDb->GetIdentityColumns();
    }

 void SystemCatalog::CreateCatalogDatabase() {
  CreateDatabase(this->sysDbName);
  this->masterDb = AllocateMiscEntity<Database>(this->sysDbName, true);

  for (int i = 0;i < this->sysTables.size(); i++) {
   const auto& tableHeader = this->sysTables[i];

   std::vector<StorageTypes::Column *> columns;
   std::vector<column_index_t> primaryKeyIndexes;

   for (int columnIndex = 0;columnIndex < tableHeader.columns.size(); columnIndex++) {
    const auto& columnHeader = tableHeader.columns[columnIndex];

    block_size_t columnSize = 0;

    const auto normalizedColumnType = Functions::String::NormalizeString(columnHeader.type);

    if (!ColumnTypeSizes.TryGetValue(normalizedColumnType, columnSize))
     throw std::runtime_error("Column type " + columnHeader.type + " does not exist");

    if (columnSize == 0)
     columnSize = columnHeader.size;

    const auto columnType = ColumnTypesDictionary.Get(normalizedColumnType);

    for (const auto& key: tableHeader.primaryKey) {
     if (columnHeader.name != key)
      continue;

     primaryKeyIndexes.push_back(columnIndex);
    }

    auto* columnPtr = AllocateMiscEntity<StorageTypes::Column>(
        columnHeader.name,
        columnType, columnSize,
        columnIndex,
        columnHeader.nullable
    );

    if (columnHeader.hasIdentity)
     columnPtr->SetIdentity(Headers::IdentityColumnsHeader(tableHeader.id, columnIndex, 1, 1, 1, true, 10000));

    columns.push_back(columnPtr);
   }

   if (primaryKeyIndexes.empty())
    throw std::runtime_error("All tables in masterDb must have a primary key");

   Headers::Index index(primaryKeyIndexes);
   this->masterDb->CreateTable(tableHeader.id, i, columns, &index);
  }
 }

 void SystemCatalog::StoreSystemTablesToCatalog()const {
    const auto dbInsertResult = this->InsertDbToMasterDb(
        this->baseExecutionContext,
        this->sysDbName,
        this->sysDbPath,
        true
    );

    const auto databaseId = dbInsertResult.primaryKey.AsInt();

    const auto schemaInsertResult = this->InsertSchemaToMasterDb(
      this->baseExecutionContext,
      databaseId,
      "dbo"
    );

    const auto schemaId = schemaInsertResult.primaryKey.AsInt(1);

    Dictionary<std::string, column_index_t> columnNameToIndex;

    int counter=  0;
    for (int i = 0;i < this->sysTables.size(); i++) {
      const auto& table = this->sysTables[i];

      const auto tableResult =
        this->InsertTableToMasterDb(
            this->baseExecutionContext,
          databaseId,
          schemaId,
          table.name,
          static_cast<SmallInt>(i),
          true
        );

      // const auto tableStatsResult =
      //   this->InsertTableStatisticsToMasterDb(
      //     this->baseProperties,
      //     tableResult.primaryKey.AsInt()
      //   );

      int columnPos = 0;
      const auto tableId = tableResult.primaryKey.AsInt(1);

      Dictionary<std::string, Int> columnIdsDict;

      for (auto& column: table.columns) {
        counter++;
        const auto normalizedColumnType = Functions::String::NormalizeString(column.type);

        auto columnSize = ColumnTypeSizes.Get(normalizedColumnType);

        if (columnSize == 0)
          columnSize = column.size;

        const auto& type = ColumnTypesDictionary.Get(normalizedColumnType);

        const auto columnResult =
          this->InsertColumnToMasterDb(
              this->baseExecutionContext,
             tableId,
             column.name,
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

        if (column.hasIdentity)
          const auto _ = this->InsertIdentityColumnToMasterDb(
                this->baseExecutionContext,
                tableId,
                columnId,
                1,
                1,
                1,
                true,
                1000
              );

        // const auto columnStatsResult =
        //   this->InsertColumnStatisticsToMasterDb(
        //     this->baseProperties,
        //     columnResult.primaryKey.AsInt()
        //   );

        columnNameToIndex.Add(column.name, columnPos);
        columnIdsDict.Add(column.name,columnId);

        columnPos++;
      }

      std::string concatenatedColumns;
      std::string _columns;

      for (int j = 0; j < table.primaryKey.size(); j++) {
        const auto& key = columnNameToIndex.Get(table.primaryKey[j]);

        concatenatedColumns +=  j > 0  ? "," + std::to_string(key) : std::to_string(key);
        _columns +="_" + table.primaryKey[j];
      }

      //TODO keep the last value keys
      const auto indexResult =
        this->InsertIndexToMasterDb(
          this->baseExecutionContext,
          tableId,
          "PK" + _columns,
          true
      );

      auto indexId = indexResult.primaryKey.AsInt(1);

      const auto constraintResult =
        this->InsertConstraintToMasterDb(
          this->baseExecutionContext,
          tableId,
          "PK" + _columns,
          Headers::ConstraintType::PrimaryKey,
          false,
          &indexId
      );

      for(int j = 0;j < table.primaryKey.size(); j++){
        auto _ = this->InsertIndexColumnToMasterDb(
            this->baseExecutionContext,
            indexId,
            columnIdsDict.Get(table.primaryKey[j]),
            static_cast<int16_t>(j),
            true
        );

        _ = this->InsertConstraintColumnToMasterDb(
          this->baseExecutionContext,
          constraintResult.primaryKey.AsInt(1),
          columnIdsDict.Get(table.primaryKey[j]),
          static_cast<int16_t>(j)
        );
      }
    }

    this->masterDb->GetColumnsHeaders();
    this->masterDb->UpdateIdentityManagersIds();
 }

  Headers::DatabaseHeader SystemCatalog::ToDatabaseHeader(const ::Memory::IAllocator* allocator, const Pages::RowReference& rowPtr){
    const auto materializedRow = rowPtr.Materialize(allocator);
    const auto& data = materializedRow.Data();

    return Headers::DatabaseHeader{
      .id = data[static_cast<column_index_t>(SysDatabases::DatabaseId)].AsInt(),
      .name = data[static_cast<column_index_t>(SysDatabases::Name)].AsString(),
      .filepath = data[static_cast<column_index_t>(SysDatabases::FilePath)].AsString(),
      .isSystem = data[static_cast<column_index_t>(SysDatabases::IsSystem)].AsBool(),
    };
  }

  Headers::DatabaseHeader SystemCatalog::ToDatabaseHeader(
    const ::Memory::IAllocator* allocator,
    const Pages::RowReference& rowPtr,
    std::vector<Headers::TableHeader> &dbTables,
    std::vector<Headers::SchemaHeader> &schemas
  ) {
    const auto materializedRow = rowPtr.Materialize(allocator);
    const auto& data = materializedRow.Data();

   return  Headers::DatabaseHeader{
      .id = data[static_cast<column_index_t>(SysDatabases::DatabaseId)].AsInt(),
      .name = data[static_cast<column_index_t>(SysDatabases::Name)].AsString(),
      .filepath = data[static_cast<column_index_t>(SysDatabases::FilePath)].AsString(),
      .isSystem = data[static_cast<column_index_t>(SysDatabases::IsSystem)].AsBool(),
      .additionalInfo = {
        .createdAt = data[static_cast<column_index_t>(SysDatabases::CreatedAt)].AsDateTime(),
        .lastModified = data[static_cast<column_index_t>(SysDatabases::LastModifiedAt)].AsDateTime(),
        .lastModifiedBy = data[static_cast<column_index_t>(SysDatabases::LastModifiedBy)].AsString(),
        .version = data[static_cast<column_index_t>(SysDatabases::Version)].AsInt(),
        .isDeleted = data[static_cast<column_index_t>(SysDatabases::IsDeleted)].AsBool(),
        .deletedAt = data[static_cast<column_index_t>(SysDatabases::DeletedAt)].IsNull()
                  ? DataTypes::DateTime()
                  : data[static_cast<column_index_t>(SysDatabases::DeletedAt)].AsDateTime(),
        },
      .tables = std::move(dbTables),
      .schemas = std::move(schemas)
    };
}

  Headers::SchemaHeader SystemCatalog::ToSchemaHeader(const ::Memory::IAllocator* allocator, const Pages::RowReference& rowPtr){
    const auto materializedRow = rowPtr.Materialize(allocator);
    const auto& data = materializedRow.Data();

    return Headers::SchemaHeader{
      data[static_cast<column_index_t>(SysSchemas::SchemaId)].AsInt(),
      data[static_cast<column_index_t>(SysSchemas::DatabaseId)].AsInt(),
      data[static_cast<column_index_t>(SysSchemas::Name)].AsString(),
      data[static_cast<column_index_t>(SysSchemas::CreatedAt)].AsDateTime(),
      data[static_cast<column_index_t>(SysSchemas::LastModifiedAt)].AsDateTime(),
      data[static_cast<column_index_t>(SysSchemas::LastModifiedBy)].AsString()
    };
  }

  Headers::TableHeader SystemCatalog::ToTableHeader(const ::Memory::IAllocator* allocator, const Pages::RowReference& rowPtr) {
    const auto materializedRow = rowPtr.Materialize(allocator);
    const auto& data = materializedRow.Data();

    return Headers::TableHeader{
      data[static_cast<column_index_t>(SysTables::DatabaseId)].AsInt(),
      data[static_cast<column_index_t>(SysTables::TableId)].AsInt(),
      data[static_cast<column_index_t>(SysTables::SchemaId)].AsInt(),
      data[static_cast<column_index_t>(SysTables::Name)].AsString(),
      data[static_cast<column_index_t>(SysTables::OrdinalPosition)].AsSmallInt(),
      data[static_cast<column_index_t>(SysTables::IsSystemTable)].AsBool(),
      data[static_cast<column_index_t>(SysTables::CreatedAt)].AsDateTime(),
      data[static_cast<column_index_t>(SysTables::LastModifiedAt)].AsDateTime(),
      data[static_cast<column_index_t>(SysTables::LastModifiedBy)].AsString()
      };
  }

  Headers::ColumnHeader SystemCatalog::ToColumnHeader(const ::Memory::IAllocator* allocator, const Pages::RowReference& rowPtr) {
      const auto materializedRow = rowPtr.Materialize(allocator);
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
          .additionalInfo{
            .createdAt = data[static_cast<column_index_t>(SysColumns::CreatedAt)].AsDateTime(),
            .lastModified = data[static_cast<column_index_t>(SysColumns::LastModifiedAt)].AsDateTime(),
            .lastModifiedBy = data[static_cast<column_index_t>(SysColumns::LastModifiedBy)].AsString(),
            .version = data[static_cast<column_index_t>(SysColumns::Version)].AsInt(),
            .isDeleted = data[static_cast<column_index_t>(SysColumns::IsDeleted)].AsBool(),
            .deletedAt = data[static_cast<column_index_t>(SysColumns::DeletedAt)].IsNull()
                      ? DataTypes::DateTime::Now()
                      : data[static_cast<column_index_t>(SysColumns::DeletedAt)].AsDateTime(),
            }
      };
  }

  Headers::IndexHeader SystemCatalog::ToIndexHeader(const ::Memory::IAllocator* allocator, const Pages::RowReference& rowPtr) {
      const auto materializedRow = rowPtr.Materialize(allocator);
      const auto& data = materializedRow.Data();

      return Headers::IndexHeader{
        .tableId = data[static_cast<column_index_t>(SysIndexes::TableId)].AsInt(),
        .id = data[static_cast<column_index_t>(SysIndexes::IndexId)].AsInt(),
        .name = data[static_cast<column_index_t>(SysIndexes::Name)].AsString(),
        .isClustered = data[static_cast<column_index_t>(SysIndexes::IsClustered)].AsBool(),
        .isDisabled = data[static_cast<column_index_t>(SysIndexes::IsDisabled)].AsBool(),
          .additionalInfo{
          .createdAt = data[static_cast<column_index_t>(SysIndexes::CreatedAt)].AsDateTime(),
          .lastModified = data[static_cast<column_index_t>(SysIndexes::LastModifiedAt)].AsDateTime(),
          .lastModifiedBy = data[static_cast<column_index_t>(SysIndexes::LastModifiedBy)].AsString(),
          .version = data[static_cast<column_index_t>(SysIndexes::Version)].AsInt(),
          .isDeleted = data[static_cast<column_index_t>(SysIndexes::IsDeleted)].AsBool(),
          .deletedAt = data[static_cast<column_index_t>(SysIndexes::DeletedAt)].IsNull()
                ? DataTypes::DateTime()
                : data[static_cast<column_index_t>(SysIndexes::DeletedAt)].AsDateTime()
          },
    };
  }

  Headers::IndexColumnsHeader SystemCatalog::ToIndexColumnsHeader(const ::Memory::IAllocator* allocator, const Pages::RowReference& rowPtr) {
    const auto materializedRow = rowPtr.Materialize(allocator);
    const auto& data = materializedRow.Data();

    return Headers::IndexColumnsHeader{
      .indexId = data[static_cast<column_index_t>(SysIndexColumns::IndexId)].AsInt(),
      .columnId = data[static_cast<column_index_t>(SysIndexColumns::ColumnId)].AsInt(),
      .ordinalPosition = data[static_cast<column_index_t>(SysIndexColumns::OrdinalPosition)].AsSmallInt(),
      .isIncluded = data[static_cast<column_index_t>(SysIndexColumns::IsIncluded)].AsBool(),
      .additionalInfo{
        .version = data[static_cast<column_index_t>(SysIndexColumns::Version)].AsInt(),
        .isDeleted = data[static_cast<column_index_t>(SysIndexColumns::IsDeleted)].AsBool(),
        .deletedAt = data[static_cast<column_index_t>(SysIndexColumns::DeletedAt)].IsNull()
              ? DataTypes::DateTime()
              : data[static_cast<column_index_t>(SysIndexColumns::DeletedAt)].AsDateTime()
      }
    };
  }

  Headers::IdentityColumnsHeader SystemCatalog::ToIdentityColumnsHeader(const ::Memory::IAllocator* allocator, const Pages::RowReference& rowPtr) {
    const auto materializedRow = rowPtr.Materialize(allocator);
    const auto& data = materializedRow.Data();

    return Headers::IdentityColumnsHeader{
      .tableId = data[static_cast<column_index_t>(SysIdentityColumns::TableId)].AsInt(),
      .columnId = data[static_cast<column_index_t>(SysIdentityColumns::ColumnId)].AsInt(),
      .seedValue = data[static_cast<column_index_t>(SysIdentityColumns::SeedValue)].AsInt(),
      .increment = data[static_cast<column_index_t>(SysIdentityColumns::IncrementValue)].AsInt(),
      .lastValue = data[static_cast<column_index_t>(SysIdentityColumns::LastValue)].AsBigInt(),
      .isCached = data[static_cast<column_index_t>(SysIdentityColumns::IsCached)].AsBool(),
      .cacheBlock = data[static_cast<column_index_t>(SysIdentityColumns::CacheBlock)].AsInt(),
      .additionalInfo{
        .version = data[static_cast<column_index_t>(SysIdentityColumns::Version)].AsInt(),
        .isDeleted = data[static_cast<column_index_t>(SysIdentityColumns::IsDeleted)].AsBool(),
        .deletedAt = data[static_cast<column_index_t>(SysIdentityColumns::DeletedAt)].IsNull()
              ? DataTypes::DateTime()
              : data[static_cast<column_index_t>(SysIdentityColumns::DeletedAt)].AsDateTime()
      }
    };
  }

  Headers::ConstraintsHeader SystemCatalog::ToConstraintsHeader(
    const ::Memory::IAllocator* allocator,
    const Pages::RowReference& rowPtr,
    std::vector<Headers::ConstraintsColumnsHeader> &constraintColumns,
    Headers::IndexHeader &indexHeader
  ) {
    const auto materializedRow = rowPtr.Materialize(allocator);
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
      .additionalInfo{
        .createdAt = data[static_cast<column_index_t>(SysConstraints::CreatedAt)].AsDateTime(),
        .lastModified = data[static_cast<column_index_t>(SysConstraints::LastModifiedAt)].AsDateTime(),
        .lastModifiedBy = data[static_cast<column_index_t>(SysConstraints::LastModifiedBy)].AsString(),
        .version = data[static_cast<column_index_t>(SysConstraints::Version)].AsInt(),
        .isDeleted = data[static_cast<column_index_t>(SysConstraints::IsDeleted)].AsBool(),
        .deletedAt = data[static_cast<column_index_t>(SysConstraints::DeletedAt)].IsNull()
              ? DataTypes::DateTime()
              : data[static_cast<column_index_t>(SysConstraints::DeletedAt)].AsDateTime() //might crash, is nullable
      },
    };
  }

  Headers::ConstraintsColumnsHeader SystemCatalog::ToConstraintsColumnsHeader(const ::Memory::IAllocator* allocator, const Pages::RowReference& rowPtr){
    const auto materializedRow = rowPtr.Materialize(allocator);
    const auto& data = materializedRow.Data();

    return Headers::ConstraintsColumnsHeader{
      .constraintId = data[static_cast<column_index_t>(SysConstraintColumns::ConstraintId)].AsInt(),
      .columnId = data[static_cast<column_index_t>(SysConstraintColumns::ColumnId)].AsInt(),
      .ordinalPosition = data[static_cast<column_index_t>(SysConstraintColumns::OrdinalPosition)].AsInt(),
      .additionalInfo{
        .version = data[static_cast<column_index_t>(SysConstraintColumns::Version)].AsInt(),
        .isDeleted = data[static_cast<column_index_t>(SysConstraintColumns::IsDeleted)].AsBool(),
        .deletedAt = data[static_cast<column_index_t>(SysConstraintColumns::DeletedAt)].IsNull()
              ? DataTypes::DateTime()
              : data[static_cast<column_index_t>(SysConstraintColumns::DeletedAt)].AsDateTime()
      },
    };
  }

  Headers::DefaultValuesHeader SystemCatalog::ToDefaultValuesHeader(const ::Memory::IAllocator* allocator, const Pages::RowReference& rowPtr) {
    const auto materializedRow = rowPtr.Materialize(allocator);
    const auto& data = materializedRow.Data();

    return Headers::DefaultValuesHeader{
      .columnId = data[static_cast<column_index_t>(SysDefaultValues::ColumnId)].AsInt(),
      .value = data[static_cast<column_index_t>(SysDefaultValues::Value)].AsString(),
      .additionalInfo{
        .version = data[static_cast<column_index_t>(SysDefaultValues::Version)].AsInt(),
        .isDeleted = data[static_cast<column_index_t>(SysDefaultValues::IsDeleted)].AsBool(),
        .deletedAt = data[static_cast<column_index_t>(SysDefaultValues::DeletedAt)].IsNull()
              ? DataTypes::DateTime()
              : data[static_cast<column_index_t>(SysDefaultValues::DeletedAt)].AsDateTime()
      },
    };
  }

  Headers::TableStatistics SystemCatalog::ToTableStatistics(const ::Memory::IAllocator* allocator, const Pages::RowReference& rowPtr) {
    const auto materializedRow = rowPtr.Materialize(allocator);
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
        const Pages::RowReference& rowPtr,
        const DataType columnType
    ) {
        const auto materializedRow = rowPtr.Materialize(allocator);
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
        const Pages::RowReference& rowPtr,
        const DataType columnType
    ) {
        const auto materializedRow = rowPtr.Materialize(allocator);
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

  Headers::IndexStatistics SystemCatalog::ToIndexStatistics(const ::Memory::IAllocator* allocator, const Pages::RowReference& rowPtr) {
   const auto materializedRow = rowPtr.Materialize(allocator);
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

  Database * SystemCatalog::GetDatabase() const{ return this->masterDb; }

  bool SystemCatalog::Initialize(const std::string_view configPath) {
    this->ReadConfiguration(configPath);

    if (this->CatalogExists()) {
     this->UseCatalogDatabase();
     return false;
    }

    this->CreateCatalogDatabase();
    this->StoreSystemTablesToCatalog();

   return true;
 }

  void SystemCatalog::Shutdown(){
    const Memory::Allocator allocator;
    this->masterDb->UpdateMasterDatabase(&allocator);

    delete this->masterDb;
    this->masterDb = nullptr;
 }

 std::vector<Headers::DatabaseHeader> SystemCatalog::RetrieveCatalog() const {
   auto* sysDatabases = this->masterDb->OpenTable(CatalogTables::SysDatabases);

   DataStructures::Array<Pages::RowReference> selectedDatabases;

   IndexState state;
   sysDatabases->ClusteredIndexScan(this->baseExecutionContext, &selectedDatabases, state, nullptr);

   std::vector<Headers::DatabaseHeader> databasesHeaders;

   if (selectedDatabases.Empty())
     return {};

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
   //   databasesHeaders.emplace_back(SystemCatalog::ToDatabaseHeader(row, tables, schemas));
   // }

   return databasesHeaders;
 }

  std::vector<Security::Role*> SystemCatalog::InsertSystemRoles()const{
    const auto admin = std::string(Constants::ADMIN_NAME);
    const auto dbOwner = std::string(Constants::DB_OWNER_NAME);
    const auto dbWriter = std::string(Constants::DB_WRITER_NAME);
    const auto dbReader = std::string(Constants::DB_READER_NAME);
    const auto guest = std::string(Constants::GUEST_NAME);

   std::vector<Security::Role*> roles;

    auto result = this->InsertRoleToMasterDb(
      this->baseExecutionContext,
      admin,
      Constants::ADMIN_PERMISSIONS
    );

   roles.push_back(AllocateMiscEntity<Security::Role>(
        result.primaryKey.AsInt(),
        admin,
        Constants::ADMIN_PERMISSIONS,
        true
    ));

    result = this->InsertRoleToMasterDb(
      this->baseExecutionContext,
      dbOwner,
      Constants::DB_OWNER_PERMISSIONS
    );

   roles.push_back(AllocateMiscEntity<Security::Role>(
        result.primaryKey.AsInt(),
        dbOwner,
        Constants::DB_OWNER_PERMISSIONS,
        true
    ));

    result = this->InsertRoleToMasterDb(
      this->baseExecutionContext,
      dbWriter,
      Constants::DB_WRITER_PERMISSIONS
    );

     roles.push_back(AllocateMiscEntity<Security::Role>(
       result.primaryKey.AsInt(),
       dbWriter,
       Constants::DB_WRITER_PERMISSIONS,
       true
    ));

    result = this->InsertRoleToMasterDb(
      this->baseExecutionContext,
      dbReader,
      Constants::DB_READER_PERMISSIONS
    );

    roles.push_back(AllocateMiscEntity<Security::Role>(
      result.primaryKey.AsInt(),
      dbReader,
      Constants::DB_READER_PERMISSIONS,
      true
    ));

    result = this->InsertRoleToMasterDb(
      this->baseExecutionContext,
      guest,
      Constants::GUEST_PERMISSIONS
    );

    roles.push_back(AllocateMiscEntity<Security::Role>(
       result.primaryKey.AsInt(),
       guest,
       Constants::GUEST_PERMISSIONS,
       true
    ));

   return roles;
  }

  Security::User* SystemCatalog::InsertSystemUsers(const std::string& hashedPassword, const Int defaultRoleId)const{
    const auto admin = std::string(Constants::ADMIN_NAME);

    const auto result =
      this->InsertUserToMasterDb(
          this->baseExecutionContext,
        admin,
        hashedPassword,
        defaultRoleId,
        true
      );

   return new Security::User(
      result.primaryKey.AsInt(),
      admin,
      hashedPassword,
      defaultRoleId,
      nullptr,
      true
   );
  }

Errors::RuntimeStatus SystemCatalog::InsertDbToMasterDb(
    const ExecutionContext& executionContext,
    const std::string& dbName,
    const std::string& dbPath,
    const bool isSystem,
    const std::string& user,
    const Int version,
    const bool isDeleted
  ) const{
      auto* table = this->masterDb->OpenTable(CatalogTables::SysDatabases);

      const auto currentDate = DataTypes::DateTime::Now();

      const std::vector fields = {
        Value(dbName, executionContext.GetAllocator(), static_cast<column_index_t>(SysDatabases::Name)),
        Value(dbPath, executionContext.GetAllocator(), static_cast<column_index_t>(SysDatabases::FilePath)),
        Value(isSystem, executionContext.GetAllocator(), static_cast<column_index_t>(SysDatabases::IsSystem)),
        Value(currentDate, executionContext.GetAllocator(), static_cast<column_index_t>(SysDatabases::CreatedAt)),
        Value(currentDate, executionContext.GetAllocator(), static_cast<column_index_t>(SysDatabases::LastModifiedAt)),
        Value(user, executionContext.GetAllocator(), static_cast<column_index_t>(SysDatabases::LastModifiedBy)),
        Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysDatabases::Version)),
        Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysDatabases::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysDatabases::DeletedAt))
    };

    auto result = table->InsertRow(executionContext, fields);

    std::cout << "Inserted database: "<< dbName << " to master db" << std::endl;

    return result;
  }

  Errors::RuntimeStatus  SystemCatalog::InsertSchemaToMasterDb(
    const ExecutionContext& executionContext,
    const Int databaseId,
    const std::string &schemaName,
    const std::string &user,
    const Int version,
    const bool isDeleted
  ) const{
     auto* table = this->masterDb->OpenTable(CatalogTables::SysSchemas);
     const auto currentDate = DataTypes::DateTime::Now();

     const std::vector fields = {
        Value(databaseId, executionContext.GetAllocator(), static_cast<column_index_t>(SysSchemas::DatabaseId)),
        Value(schemaName, executionContext.GetAllocator(), static_cast<column_index_t>(SysSchemas::Name)),
        Value(currentDate, executionContext.GetAllocator(), static_cast<column_index_t>(SysSchemas::CreatedAt)),
        Value(currentDate, executionContext.GetAllocator(), static_cast<column_index_t>(SysSchemas::LastModifiedAt)),
        Value(user, executionContext.GetAllocator(), static_cast<column_index_t>(SysSchemas::LastModifiedBy)),
        Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysSchemas::Version)),
        Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysSchemas::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysSchemas::DeletedAt)),
     };

    auto result = table->InsertRow(executionContext, fields);

      std::cout << "Inserted schema: "<< schemaName << " to master db" << std::endl;

    return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertTableToMasterDb(
    const ExecutionContext& executionContext,
    const Int databaseId,
    const Int schemaId,
    const std::string& tableName,
    const SmallInt ordinalPosition,
    const bool isSystem,
    const std::string& user,
    const Int version,
    const bool isDeleted
  ) const{

      StorageTypes::Table* table = this->masterDb->OpenTable(CatalogTables::SysTables);
      const auto currentDate = DataTypes::DateTime::Now();

      const std::vector fields = {
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
        Value::Null(static_cast<column_index_t>(SysTables::DeletedAt)),
      };

      auto result = table->InsertRow(executionContext, fields);

        std::cout << "Inserted table: "<< tableName << " to master db" << std::endl;

      return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertColumnToMasterDb(
    const ExecutionContext& executionContext,
    const Int tableId,
    const std::string &columnName,
    const DataType columnType,
    const Int columnSize,
    const TinyInt precision,
    const TinyInt scale,
    const bool isNullable,
    const Int ordinalPosition,
    const bool isSystem,
    const std::string& user,
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

      const std::vector fields = {
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
        Value::Null(static_cast<column_index_t>(SysColumns::DeletedAt)),
      };

      auto result = table->InsertRow(executionContext, fields);

        std::cout << "Inserted column: "<< columnName << " to master db" << std::endl;

      return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertIndexToMasterDb(
    const ExecutionContext& executionContext,
    const Int tableId,
    const std::string &indexName,
    const bool isClustered,
    const bool isDisabled,
    const std::string &user,
    const Int version,
    const bool isDeleted
  ) const{
     StorageTypes::Table* table = this->masterDb->OpenTable(CatalogTables::SysIndexes);
     const auto currentDate = DataTypes::DateTime::Now();

     const std::vector fields = {
        Value(tableId, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexes::TableId)),
        Value(indexName, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexes::Name)),
        Value(isClustered, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexes::IsClustered)),
        Value(isDisabled, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexes::IsDisabled)),
        Value(currentDate, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexes::CreatedAt)),
        Value(currentDate, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexes::LastModifiedAt)),
        Value(user, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexes::LastModifiedBy)),
        Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexes::Version)),
        Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexes::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysIndexes::DeletedAt)),
     };

      auto result = table->InsertRow(executionContext, fields);

        std::cout << "Inserted index: "<< indexName << " to master db" << std::endl;

      return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertIndexColumnToMasterDb(
    const ExecutionContext& executionContext,
    const Int indexId,
    const Int columnId,
    const int16_t & ordinalPosition,
    const bool  isIncluded,
    const Int version,
    const bool isDeleted) const{
    StorageTypes::Table* table = this->masterDb->OpenTable(CatalogTables::SysIndexColumns);
    const auto currentDate = DataTypes::DateTime::Now();

    const std::vector fields = {
      Value(indexId, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexColumns::IndexId)),
      Value(columnId, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexColumns::ColumnId)),
      Value(ordinalPosition, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexColumns::OrdinalPosition)),
      Value(isIncluded, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexColumns::IsIncluded)),
      Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexColumns::Version)),
      Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexColumns::IsDeleted)),
      Value::Null(static_cast<column_index_t>(SysIndexColumns::DeletedAt)),
    };

    auto result = table->InsertRow(executionContext, fields);

      std::cout << "Inserted index column to master db" << std::endl;

    return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertConstraintToMasterDb(
      const ExecutionContext& executionContext,
      const Int tableId,
      const std::string & constraintName,
      const Headers::ConstraintType & constraintType,
      const bool  isDisabled,
      const Int *constraintIndexId,
      const std::string & user,
      const Int version,
      const bool isDeleted
  ) const{

    auto* table = this->masterDb->OpenTable(CatalogTables::SysConstraints);

    const auto currentDate = DataTypes::DateTime::Now();

    auto constraintField = constraintIndexId != nullptr
        ? Value(*constraintIndexId, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraints::IndexId))
        : Value::Null(static_cast<column_index_t>(SysConstraints::IndexId));

    const std::vector fields = {
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
      Value::Null(static_cast<column_index_t>(SysConstraints::DeletedAt)),
    };

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

    const std::vector fields = {
        Value(constraintId, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraintColumns::ConstraintId)),
        Value(columnId, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraintColumns::ColumnId)),
        Value(ordinalPosition, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraintColumns::OrdinalPosition)),
        Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraintColumns::Version)),
        Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysConstraintColumns::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysConstraintColumns::DeletedAt)),
    };

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

      const std::vector fields = {
        Value(tableId, executionContext.GetAllocator(), static_cast<column_index_t>(SysIdentityColumns::TableId)),
        Value(columnId, executionContext.GetAllocator(), static_cast<column_index_t>(SysIdentityColumns::ColumnId)),
        Value(seedValue, executionContext.GetAllocator(), static_cast<column_index_t>(SysIdentityColumns::SeedValue)),
        Value(increment, executionContext.GetAllocator(), static_cast<column_index_t>(SysIdentityColumns::IncrementValue)),
        Value(lastValue, executionContext.GetAllocator(), static_cast<column_index_t>(SysIdentityColumns::LastValue)),
        Value(isCached, executionContext.GetAllocator(), static_cast<column_index_t>(SysIdentityColumns::IsCached)),
        Value(cacheBlock, executionContext.GetAllocator(), static_cast<column_index_t>(SysIdentityColumns::CacheBlock)),
        Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysIdentityColumns::Version)),
        Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysIdentityColumns::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysIdentityColumns::DeletedAt)),
      };

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

      const std::vector fields = {
        Value(columnId, executionContext.GetAllocator(), static_cast<column_index_t>(SysDefaultValues::ColumnId)),
        Value(
            std::string(reinterpret_cast<const char*>(value.Data()), value.Size()),
            executionContext.GetAllocator(),
            static_cast<column_index_t>(SysDefaultValues::Value)
        ),
        Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysDefaultValues::Version)),
        Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysDefaultValues::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysDefaultValues::DeletedAt)),
      };

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

    const std::vector fields = {
      Value(tableId, executionContext.GetAllocator(), static_cast<column_index_t>(SysTableStats::TableId)),
      Value(rowCount, executionContext.GetAllocator(), static_cast<column_index_t>(SysTableStats::RowCount)),
      Value(rowSize, executionContext.GetAllocator(), static_cast<column_index_t>(SysTableStats::AvgRowSize)),
      Value(pageCount, executionContext.GetAllocator(), static_cast<column_index_t>(SysTableStats::PageCount)),
      Value(DataTypes::DateTime::Now(), executionContext.GetAllocator(), static_cast<column_index_t>(SysTableStats::LastUpdatedAt)),
    };

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

    const std::vector fields = {
      Value(columnId, executionContext.GetAllocator(), static_cast<column_index_t>(SysColumnStats::ColumnId)),
      Value(distinctCount, executionContext.GetAllocator(), static_cast<column_index_t>(SysColumnStats::DistinctCount)),
      Value::Null(static_cast<column_index_t>(SysColumnStats::MinimumValue)),
      Value::Null(static_cast<column_index_t>(SysColumnStats::MaximumValue)),
      Value(nullCount, executionContext.GetAllocator(), static_cast<column_index_t>(SysColumnStats::NullCount))
    };

    auto result = table->InsertRow(executionContext, fields);

    std::cout << "Inserted column stats for column with id: " << columnId << std::endl;

    return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertColumnHistogramsToMasterDb(
    const ::Memory::IAllocator* allocator,
    const Int columnId,
    const Value &min,
    const Value &max,
    const Int rowCount,
    const BigInt distinctCount
  ) const {

    const std::vector fields = {
      Value(columnId, allocator, static_cast<column_index_t>(SysColumnHistograms::ColumnId)),
      Value(std::string(reinterpret_cast<const char*>(min.Data()), min.Size()), allocator, static_cast<column_index_t>(SysColumnHistograms::RangeStart)),
      Value(std::string(reinterpret_cast<const char*>(max.Data()), max.Size()), allocator, static_cast<column_index_t>(SysColumnHistograms::RangeEnd)),
      Value(rowCount, allocator, static_cast<column_index_t>(SysColumnHistograms::RowCount)),
      Value(distinctCount, allocator, static_cast<column_index_t>(SysColumnHistograms::DistinctCount)),
    };

    auto* table = this->masterDb->OpenTable(CatalogTables::SysColumnHistograms);

    auto result = table->InsertRow(this->baseExecutionContext, fields);
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

   const std::vector fields = {
     Value(tableId, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexStats::TableId)),
     Value(indexId, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexStats::IndexId)),
     Value(leafPages, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexStats::LeafPages)),
     Value(depth, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexStats::Depth)),
     Value(averageFragmentation, executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexStats::AverageFragmentation)),
     Value(DataTypes::DateTime::Now(), executionContext.GetAllocator(), static_cast<column_index_t>(SysIndexStats::LastUpdated)),
   };

   auto result = table->InsertRow(executionContext, fields);

   std::cout << "Inserted index statistics for index: " << indexId << std::endl;

   return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertRoleToMasterDb(
    const ExecutionContext& executionContext,
    const std::string &roleName,
    const Security::Permission &permissions,
    const bool isSystem,
    const Int version,
    const bool isDeleted
  ) const{

    auto* table = this->masterDb->OpenTable(CatalogTables::SysRoles);
    const auto currentDate = DataTypes::DateTime::Now();

    const std::string lastModifiedBy = "system";

    const std::vector fields = {
      Value(roleName, executionContext.GetAllocator(), static_cast<column_index_t>(SysRoles::RoleName)),
      Value(static_cast<Int>(permissions), executionContext.GetAllocator(), static_cast<column_index_t>(SysRoles::Permissions)),
      Value(isSystem, executionContext.GetAllocator(), static_cast<column_index_t>(SysRoles::IsSystemRole)),
      Value(DataTypes::DateTime::Now(), executionContext.GetAllocator(), static_cast<column_index_t>(SysRoles::CreatedAt)),
      Value(DataTypes::DateTime::Now(), executionContext.GetAllocator(), static_cast<column_index_t>(SysRoles::LastModifiedAt)),
      Value(lastModifiedBy, executionContext.GetAllocator(), static_cast<column_index_t>(SysRoles::LastModifiedBy)),
      Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysRoles::Version)),
      Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysRoles::IsDeleted)),
      Value::Null(static_cast<column_index_t>(SysRoles::DeletedAt)),
    };

    auto result = table->InsertRow(executionContext, fields);
    std::cout << "Inserted Role " << roleName << std::endl;
    return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertUserToMasterDb(
    const ExecutionContext& executionContext,
    const std::string &username,
    const std::string &passwordHash,
    const Int roleId,
    const bool isActive,
    const Int version,
    const bool isDeleted
  ) const{

    auto* table = this->masterDb->OpenTable(CatalogTables::SysUsers);
    const auto currentDate = DataTypes::DateTime::Now();

    const std::string lastModifiedBy = "system";

    const std::vector fields = {
      Value(username, executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::UserName)),
      Value(passwordHash, executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::PasswordHash)),
      Value(roleId, executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::RoleId)),
      Value(isActive, executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::IsActive)),
      Value(DataTypes::DateTime::Now(), executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::CreatedAt)),
      Value(DataTypes::DateTime::Now(), executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::LastModifiedAt)),
      Value(lastModifiedBy, executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::LastModifiedBy)),
      Value(version, executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::Version)),
      Value(isDeleted, executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::IsDeleted)),
      Value::Null(static_cast<column_index_t>(SysUsers::DeletedAt)),
    };

    auto result = table->InsertRow(executionContext, fields);

    std::cout << "Inserted User " << username << std::endl;

    return result;
  }

  std::vector<Security::Role> SystemCatalog::SelectRoles(const ::Memory::IAllocator* allocator) const{
   DataStructures::Array<Pages::RowReference> rows;

   std::vector<Security::Role> roles;

   auto* table = this->masterDb->OpenTable(CatalogTables::SysRoles);

   table->SystemClusteredIndexScan(allocator, &rows, nullptr);

   for (const auto& row : rows) {
     const auto materializedRow = row.Materialize(allocator);
      const auto& data = materializedRow.Data();

     roles.emplace_back(
       data[0].AsInt(),
       data[1].AsString(),
       static_cast<Security::Permission>(data[2].AsInt()),
       data[3].AsBool()
     );
   }

   return roles;
 }

  std::vector<Security::User> SystemCatalog::SelectUsers(const ::Memory::IAllocator* allocator) const{
   DataStructures::Array<Pages::RowReference> rows;

   std::vector<Security::User> users;

   auto* table = this->masterDb->OpenTable(CatalogTables::SysUsers);

   table->SystemClusteredIndexScan(allocator, &rows, nullptr);

   for (const auto& row : rows) {
      const auto materializedRow = row.Materialize(allocator);
      const auto& data = materializedRow.Data();


     users.emplace_back(
         data[0].AsInt(),
         data[1].AsString(),
         data[2].AsString(),
         data[3].AsInt(),
         nullptr,
         data[4].AsBool()
     );
   }

   return users;
 }

 bool SystemCatalog::DatabaseExists(const ::Memory::IAllocator* allocator, const std::string &dbName) const{
      auto* sysDatabases = this->masterDb->OpenTable(CatalogTables::SysDatabases);
      DataStructures::Array<Pages::RowReference> selectedDatabases;

      auto columnExpr = Expressions::ColumnExpression(static_cast<column_index_t>(SysDatabases::Name));
      auto constantExpr = Expressions::ConstantExpression(Value(dbName, allocator, static_cast<column_index_t>(SysDatabases::Name)));

      const Expressions::BinaryExpression binaryExpr(&columnExpr, &constantExpr, Expressions::BinaryOperator::EqualIgnoreOrdinalCase);

      sysDatabases->SystemClusteredIndexScan(allocator, &selectedDatabases, &binaryExpr);

      return !selectedDatabases.Empty();
}

    Headers::DatabaseHeader SystemCatalog::SelectDatabase(const ::Memory::IAllocator* allocator, const std::string &name) const{
        auto columnExpr = Expressions::ColumnExpression(static_cast<column_index_t>(SysDatabases::Name));
        auto constantExpr = Expressions::ConstantExpression(Value(name, allocator, static_cast<column_index_t>(SysDatabases::Name)));

        const Expressions::BinaryExpression binaryExpr(
             &columnExpr,
            &constantExpr,
         Expressions::BinaryOperator::EqualIgnoreOrdinalCase
        );

        auto* sysDatabases = this->masterDb->OpenTable(CatalogTables::SysDatabases);
        DataStructures::Array<Pages::RowReference> selectedDatabases;

        sysDatabases->SystemClusteredIndexScan(allocator, &selectedDatabases, &binaryExpr);

        if (selectedDatabases.Empty()) return {};

        return SystemCatalog::ToDatabaseHeader(allocator, selectedDatabases[0]);
    }

Headers::DatabaseHeader SystemCatalog::SelectDatabaseById(const ::Memory::IAllocator* allocator, const Int databaseId) const{
  auto* sysDatabases = this->masterDb->OpenTable(CatalogTables::SysDatabases);
  DataStructures::Array<Pages::RowReference> selectedDatabases;

  DataTypes::Indexing::Key key;
  key.InsertKey(DataTypes::Indexing::Key(&databaseId, sizeof(databaseId), DataType::Int, allocator));

  sysDatabases->SystemClusteredIndexSeek(allocator, &selectedDatabases, key, nullptr);

  if (selectedDatabases.Empty())
    return {};

  return SystemCatalog::ToDatabaseHeader(allocator, selectedDatabases[0]);
}

std::vector<Headers::SchemaHeader> SystemCatalog::SelectSchemas(const ::Memory::IAllocator* allocator, const Int databaseId) const{
     auto* sysSchemas = this->masterDb->OpenTable(CatalogTables::SysSchemas);
     DataStructures::PolymorphicArray<Pages::RowReference> selectedSchemas(allocator, 2);

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&databaseId, sizeof(databaseId), DataType::Int, allocator));

    sysSchemas->SystemClusteredIndexSeek(allocator, &selectedSchemas, key, nullptr);

    if (selectedSchemas.Empty()) return {};

     std::vector<Headers::SchemaHeader> schemas;

    for (const auto& row : selectedSchemas)
      schemas.emplace_back(SystemCatalog::ToSchemaHeader(allocator, row));

     return schemas;
  }

  Dictionary<std::string, Headers::SchemaHeader> SystemCatalog::SelectSchemasToDictionary(const ::Memory::IAllocator* allocator, const Int databaseId) const{
    const auto& schemas = this->SelectSchemas(allocator, databaseId);

    Dictionary<std::string, Headers::SchemaHeader> selectedSchemas;

    for (const auto& schema : schemas)
      selectedSchemas.Add(schema.name, schema);

    return selectedSchemas;
  }

  bool SystemCatalog::SchemaExists(
        const ::Memory::IAllocator* allocator,
        const Int databaseId,
        const std::string &schema,
        int* schemaId
) const{
    DataStructures::PolymorphicArray<Pages::RowReference> selectedSchemas(allocator, 1);

    auto* sysSchemas = this->masterDb->OpenTable(CatalogTables::SysSchemas);

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&databaseId, sizeof(databaseId), DataType::Int, allocator));

    sysSchemas->SystemClusteredIndexSeek(allocator, &selectedSchemas, key, nullptr);

    for (const auto& row : selectedSchemas){
      const auto materializedRow = row.Materialize(allocator);
      const auto currentSchemaName = materializedRow.GetColumnAt(static_cast<column_index_t>(SysSchemas::Name));

      if (Functions::String::Lower(currentSchemaName.AsString())
          == Functions::String::Lower(schema)) {

        if (schemaId != nullptr)
          *schemaId = materializedRow.GetColumnAt(static_cast<column_index_t>(SysSchemas::SchemaId)).AsInt();

        return true;
      }
    }

    return false;
  }

    std::vector<Headers::TableHeader> SystemCatalog::SelectTables(const ::Memory::IAllocator* allocator, const std::string &dbName) const{
        const auto databaseHeader = this->SelectDatabase(allocator, dbName);
        return this->SelectTables(allocator, databaseHeader.id);
    }

    std::vector<Headers::TableHeader> SystemCatalog::SelectTables(
        const ::Memory::IAllocator* allocator,
        const Int databaseId
    ) const{
        DataStructures::PolymorphicArray<Pages::RowReference> selectedTables(allocator, 10);

        auto* sysTablesPtr = this->masterDb->OpenTable(CatalogTables::SysTables);

        DataTypes::Indexing::Key key;
        key.InsertKey(DataTypes::Indexing::Key(&databaseId, sizeof(databaseId), DataType::Int, allocator));

        sysTablesPtr->SystemClusteredIndexSeek(allocator, &selectedTables, key, nullptr);

        if (selectedTables.Empty())
            return {};

        std::vector<Headers::TableHeader> selectedTableHeaders;
        selectedTableHeaders.reserve(selectedTables.Size());

        for (const auto& row : selectedTables)
            selectedTableHeaders.emplace_back(SystemCatalog::ToTableHeader(allocator, row));

        std::ranges::sort(selectedTableHeaders,
            [](const Headers::TableHeader& a, const Headers::TableHeader& b) {
                return a.ordinalPosition < b.ordinalPosition;
            }
        );

        return selectedTableHeaders;
    }

    Headers::TableHeader SystemCatalog::SelectTable(
        const ::Memory::IAllocator* allocator,
        const std::string &dbName,
        const std::string &tableName
    ) const{
        const auto databaseHeader = this->SelectDatabase(allocator, dbName);
        return this->SelectTable(allocator, databaseHeader.id, tableName, Constants::DEFAULT_SCHEMA_NAME.data());
    }

  Headers::TableHeader SystemCatalog::SelectTable(
    const ::Memory::IAllocator* allocator,
    const Int databaseId,
    const std::string &tableName,
    const std::string& schema
  ) const{

    Int schemaId = -1;
    if (!this->SchemaExists(allocator, databaseId, schema, &schemaId) && !schema.empty())
      return {};

    DataStructures::Array<Pages::RowReference> selectedTables;
    auto* sysTablesPtr = this->masterDb->OpenTable(CatalogTables::SysTables);

    auto leftColumnExpr = Expressions::ColumnExpression(static_cast<column_index_t>(SysTables::SchemaId));
    auto leftConstantExpr = Expressions::ConstantExpression(Value(schemaId, allocator, static_cast<column_index_t>(SysTables::SchemaId)));

    auto leftBinaryExpr = Expressions::BinaryExpression(&leftColumnExpr, &leftConstantExpr, Expressions::BinaryOperator::Equal);

    auto rightColumnExpr = Expressions::ColumnExpression(static_cast<column_index_t>(SysTables::Name));
    auto rightConstantExpr = Expressions::ConstantExpression(Value(tableName, allocator, static_cast<column_index_t>(SysTables::Name)));

    auto rightBinaryExpr = Expressions::BinaryExpression(&rightColumnExpr, &rightConstantExpr, Expressions::BinaryOperator::EqualIgnoreOrdinalCase);

    const auto logicalExpr = Expressions::LogicalExpression(&leftBinaryExpr, &rightBinaryExpr, Expressions::LogicalType::And);

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&databaseId, sizeof(databaseId), DataType::Int, allocator));

    sysTablesPtr->SystemClusteredIndexSeek(allocator, &selectedTables, key, &logicalExpr);

    if (selectedTables.Empty())
      return {};

    return SystemCatalog::ToTableHeader(allocator, selectedTables[0]);
  }

    std::vector<Headers::ConstraintsHeader> SystemCatalog::SelectConstraints(
        const ::Memory::IAllocator* allocator,
        const Int tableId
    ) const{
        DataStructures::PolymorphicArray<Pages::RowReference> selectedConstraints(allocator);
        auto* constraintsTable = this->masterDb->OpenTable(CatalogTables::SysConstraints);

        DataTypes::Indexing::Key key;
        key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int, allocator));

        constraintsTable->ClusteredIndexSeek(this->baseExecutionContext, &selectedConstraints, key, nullptr);

        if (selectedConstraints.Empty()) return {};

        std::vector<Headers::ConstraintsHeader> selectedConstraintsHeader;
        selectedConstraintsHeader.reserve(selectedConstraints.Size());

        for (const auto& row : selectedConstraints) {
            const auto materializedRow = row.Materialize(allocator);
            const auto& data = materializedRow.Data();

            auto constraintColumns = this->SelectConstraintColumnsByConstraintId(allocator, data[0].AsInt());

            const auto indexId =(data[static_cast<column_index_t>(SysConstraints::IndexId)].IsNull())
                ? -1
                : data[static_cast<column_index_t>(SysConstraints::IndexId)].AsInt();

            Headers::IndexHeader index;
            if(indexId != -1)
                index = this->SelectIndexById(allocator, indexId);

            selectedConstraintsHeader.emplace_back(SystemCatalog::ToConstraintsHeader(allocator, row, constraintColumns, index));
        }

        return selectedConstraintsHeader;
    }

  Headers::ColumnHeader SystemCatalog::SelectColumnById(
        const ::Memory::IAllocator* allocator,
      const Int tableId,
      const Int columnId
    ) const{
    DataStructures::PolymorphicArray<Pages::RowReference> selectedColumns(allocator, 1);
    auto* sysColumns = this->masterDb->OpenTable(CatalogTables::SysColumns);

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int, allocator));
    key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int, allocator));

    sysColumns->ClusteredIndexSeek(this->baseExecutionContext, &selectedColumns, key, nullptr);

    if (selectedColumns.Empty())
      return {};

    return SystemCatalog::ToColumnHeader(allocator, selectedColumns.Start());
  }

    std::vector<Headers::ColumnHeader> SystemCatalog::SelectColumns(
        const ::Memory::IAllocator* allocator,
        const Int tableId
    ) const{
        DataStructures::PolymorphicArray<Pages::RowReference> selectedColumns(allocator, 10);
        auto* sysColumns = this->masterDb->OpenTable(CatalogTables::SysColumns);

        DataTypes::Indexing::Key key;
        key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int, allocator));

        sysColumns->SystemClusteredIndexSeek(allocator, &selectedColumns, key, nullptr);

        if (selectedColumns.Empty()) return {};

        std::vector<Headers::ColumnHeader> selectedColumnHeaders;
        selectedColumnHeaders.reserve(selectedColumns.Size());

        for (const auto& row : selectedColumns)
            selectedColumnHeaders.emplace_back(SystemCatalog::ToColumnHeader(allocator, row));

        std::ranges::sort(selectedColumnHeaders,
        [](const Headers::ColumnHeader& a, const Headers::ColumnHeader& b) {
                return a.ordinalPosition < b.ordinalPosition;
            }
        );

        return selectedColumnHeaders;
    }

    Dictionary<std::string, Headers::ColumnHeader> SystemCatalog::SelectColumnsToDictionary(
        const ::Memory::IAllocator* allocator,
        const Int tableId
    ) const{
        const auto columns = this->SelectColumns(allocator, tableId);

        Dictionary<std::string, Headers::ColumnHeader> selectedColumns;
        for (const auto& column: columns)
            selectedColumns.Add(Functions::String::Lower(column.name), column);

        return selectedColumns;
    }

   std::vector<Headers::IndexHeader> SystemCatalog::SelectIndexes(
        const ::Memory::IAllocator* allocator,
       const Int tableId
    ) const{
        auto* sysIndexes = this->masterDb->OpenTable(CatalogTables::SysIndexes);
        DataStructures::PolymorphicArray<Pages::RowReference> selectedIndexes(allocator);

        DataTypes::Indexing::Key key;
        key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int, allocator));

        sysIndexes->SystemClusteredIndexSeek(allocator, &selectedIndexes, key, nullptr);

        std::vector<Headers::IndexHeader> selectedIndexHeaders;

        for (const auto& row : selectedIndexes)
            selectedIndexHeaders.emplace_back(SystemCatalog::ToIndexHeader(allocator, row));

      //get the clustered first
        std::ranges::sort(selectedIndexHeaders,
        [](const Headers::IndexHeader& a, const Headers::IndexHeader& b) {
            return a.isClustered > b.isClustered;
        });
        return selectedIndexHeaders;
    }

    Headers::IndexHeader SystemCatalog::SelectIndexById(const ::Memory::IAllocator* allocator, const Int indexId) const{
        auto* sysIndexes = this->masterDb->OpenTable(CatalogTables::SysIndexes);
        DataStructures::PolymorphicArray<Pages::RowReference> selectedIndexes(allocator, 1);

        DataTypes::Indexing::Key key;
        key.InsertKey(DataTypes::Indexing::Key(&indexId, sizeof(indexId), DataType::Int, allocator));

        sysIndexes->SystemClusteredIndexSeek(allocator, &selectedIndexes, key, nullptr);

        if(selectedIndexes.Empty()) return {};

        auto indexColumns = this->SelectIndexColumnsByIndexId(allocator, indexId);

        std::vector<Headers::IndexHeader> selectedIndexHeaders;

        auto header = SystemCatalog::ToIndexHeader(allocator, selectedIndexes[0]);
        header.columns = std::move(indexColumns);

        return header;
    }

    std::vector<Headers::IndexColumnsHeader> SystemCatalog::SelectIndexColumnsByIndexId(
        const ::Memory::IAllocator* allocator,
        const Int indexId
    ) const{
        auto* sysIndexes = this->masterDb->OpenTable(CatalogTables::SysIndexColumns);
        DataStructures::Array<Pages::RowReference> rows;

        DataTypes::Indexing::Key key;
        key.InsertKey(DataTypes::Indexing::Key(&indexId, sizeof(indexId), DataType::Int, allocator));

        sysIndexes->SystemClusteredIndexSeek(allocator, &rows, key, nullptr);

        if(rows.Empty()) return {};

        std::vector<Headers::IndexColumnsHeader> indexColumns;

        for (const auto& row : rows)
            indexColumns.emplace_back(SystemCatalog::ToIndexColumnsHeader(allocator, row));

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

   std::vector<Headers::IdentityColumnsHeader> SystemCatalog::SelectIdentityColumnsByTableId(
        const ::Memory::IAllocator* allocator,
       const Int tableId
    ) const{
        auto* table = this->masterDb->OpenTable(CatalogTables::SysIdentityColumns);
        DataStructures::PolymorphicArray<Pages::RowReference> rows(allocator, 10);

        DataTypes::Indexing::Key key;
        key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int, allocator));

        table->SystemClusteredIndexSeek(allocator, &rows, key, nullptr);

        if(rows.Empty()) return {};

        std::vector<Headers::IdentityColumnsHeader> columns;

        for (const auto& row : rows)
            columns.emplace_back(SystemCatalog::ToIdentityColumnsHeader(allocator, row));

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

    std::vector<Headers::ConstraintsColumnsHeader> SystemCatalog::SelectConstraintColumnsByConstraintId(
        const ::Memory::IAllocator* allocator,
        const Int constraintId
    ) const{
        auto* sysIndexes = this->masterDb->OpenTable(CatalogTables::SysConstraintColumns);
        DataStructures::PolymorphicArray<Pages::RowReference> rows(allocator, 2);

        DataTypes::Indexing::Key key;
        key.InsertKey(DataTypes::Indexing::Key(&constraintId, sizeof(constraintId), DataType::Int, allocator));

        sysIndexes->SystemClusteredIndexSeek(allocator, &rows, key, nullptr);

        if(rows.Empty()) return {};

        std::vector<Headers::ConstraintsColumnsHeader> constraintColumns;

        for(const auto& row : rows)
            constraintColumns.emplace_back(SystemCatalog::ToConstraintsColumnsHeader(allocator, row));

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
        DataStructures::PolymorphicArray<Pages::RowReference> rows(allocator, 1);

        DataTypes::Indexing::Key key;
        key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int, allocator));

        sysValues->SystemClusteredIndexSeek(allocator, &rows, key, nullptr);

        if(rows.Empty()) return {};

        return SystemCatalog::ToDefaultValuesHeader(allocator, rows[0]);
    }

    Headers::TableStatistics SystemCatalog::SelectTableStatisticsById(
        const ::Memory::IAllocator* allocator,
        const Int tableId
    ) const{
        auto* sysIndexes = this->masterDb->OpenTable(CatalogTables::SysTableStats);
        DataStructures::PolymorphicArray<Pages::RowReference> rows(allocator, 1);

        DataTypes::Indexing::Key key;
        key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int, allocator));

        sysIndexes->SystemClusteredIndexSeek(allocator, &rows, key, nullptr);

        if (rows.Empty()) return {};

        return SystemCatalog::ToTableStatistics(allocator, rows.Start());
    }

    Headers::ColumnStatistics SystemCatalog::SelectColumnStatisticsById(
        const ::Memory::IAllocator* allocator,
        const Int columnId,
        const DataType columnType
    ) const{
        auto* sysColumnStats = this->masterDb->OpenTable(CatalogTables::SysColumnStats);
        DataStructures::PolymorphicArray<Pages::RowReference> rows(allocator, 1);

        DataTypes::Indexing::Key key;
        key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int, allocator));

        sysColumnStats->SystemClusteredIndexSeek(allocator, &rows, key, nullptr);

        if (rows.Empty()) return Headers::ColumnStatistics();

        return SystemCatalog::ToColumnStatistics(allocator, rows.Start(), columnType);
    }

    std::vector<Headers::ColumnHistograms> SystemCatalog::SelectColumnHistogramsByColumnId(
        const ::Memory::IAllocator* allocator,
        const Int tableId,
        const Int columnId
    ) const {
        auto columnHeader = this->SelectColumnById(allocator, tableId, columnId);

        std::vector<Headers::ColumnHistograms> result;
        result.reserve(NUMBER_OF_HISTOGRAM_BUCKETS);

        auto* table = this->masterDb->OpenTable(CatalogTables::SysColumnHistograms);

        DataTypes::Indexing::Key key;
        key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int, allocator));

        DataStructures::PolymorphicArray<Pages::RowReference> rows(allocator, NUMBER_OF_HISTOGRAM_BUCKETS);
        table->SystemClusteredIndexSeek(allocator, &rows, key, nullptr);

        for (const auto& row : rows)
            result.emplace_back(SystemCatalog::ToColumnHistograms(allocator, row, static_cast<DataType>(columnHeader.dataType)));

        return result;
    }

    std::vector<Headers::IndexStatistics> SystemCatalog::SelectIndexStatisticsByTableId(
        const ::Memory::IAllocator* allocator,
        const Int tableId
    ) const {
        std::vector<Headers::IndexStatistics> result;
        auto* table = this->masterDb->OpenTable(CatalogTables::SysIndexStats);

        DataTypes::Indexing::Key key;
        key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int, allocator));

        DataStructures::Array<Pages::RowReference> rows;
        table->SystemClusteredIndexSeek(allocator, &rows, key, nullptr);

        for (const auto& row : rows)
            result.emplace_back(SystemCatalog::ToIndexStatistics(allocator, row));

        return result;
    }

    void SystemCatalog::UpdateIdentityByColumnId(
        const ::Memory::IAllocator* allocator,
        const Int tableId,
        const Int columnId,
        const BigInt lastValue
    )const{
        auto* table = this->masterDb->OpenTable(CatalogTables::SysIdentityColumns);

        const std::vector updates = {
            Value(lastValue, allocator, static_cast<column_index_t>(SysIdentityColumns::LastValue))
        };

        DataTypes::Indexing::Key key;
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

    const std::vector updates = {
      Value(rowCount, allocator, static_cast<column_index_t>(SysTableStats::RowCount)),
      Value(rowSize, allocator, static_cast<column_index_t>(SysTableStats::AvgRowSize)),
      Value(pageCount, allocator, static_cast<column_index_t>(SysTableStats::PageCount)),
      Value(DataTypes::DateTime::Now(), allocator, static_cast<column_index_t>(SysTableStats::LastUpdatedAt))
    };

    auto* table = this->masterDb->OpenTable(CatalogTables::SysTableStats);

    DataTypes::Indexing::Key key;
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
        const std::vector updates = {
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
        };

        auto* table = this->masterDb->OpenTable(CatalogTables::SysColumnStats);

        DataTypes::Indexing::Key key;
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
   const std::vector updates = {
     Value(leafPages, allocator, static_cast<column_index_t>(SysIndexStats::LeafPages)),
     Value(depth, allocator, static_cast<column_index_t>(SysIndexStats::Depth)),
     Value(averageFragmentation, allocator, static_cast<column_index_t>(SysIndexStats::AverageFragmentation)),
     Value(DataTypes::DateTime::Now(), allocator, static_cast<column_index_t>(SysIndexStats::LastUpdated)),
   };

   auto* table = this->masterDb->OpenTable(CatalogTables::SysIndexStats);

   DataTypes::Indexing::Key key;
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
    const std::vector updates = {
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
       Value(distinctCount, allocator, static_cast<column_index_t>(SysColumnHistograms::DistinctCount)),
     };

    auto* table = this->masterDb->OpenTable(CatalogTables::SysColumnHistograms);

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int, allocator));
    key.InsertKey(DataTypes::Indexing::Key(&histogramId, sizeof(histogramId), DataType::Int, allocator));

    auto result = table->SystemClusteredIndexSeekUpdate(allocator, key, updates);

    std::cout << "Updated histogram Bucket for column: " << columnId << " and id: " << histogramId << std::endl;

    return result;
  }

  Errors::RuntimeStatus SystemCatalog::UpdateColumnById(
        const ::Memory::IAllocator* allocator,
      const Int columnId,
      const std::vector<Value> &updates
    ) const{
    auto* table = this->masterDb->OpenTable(CatalogTables::SysColumns);

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int, allocator));

    return table->SystemClusteredIndexSeekUpdate(allocator, key, updates);
  }

    Errors::RuntimeStatus SystemCatalog::UpdateUserById(
        const ExecutionContext& executionContext,
        const std::string& username,
        const Int userId,
        const Int roleId
    ) const {
        auto* table = this->masterDb->OpenTable(CatalogTables::SysUsers);

        const auto currentDate = DataTypes::DateTime::Now();

        const std::vector updates = {
            Value(roleId, executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::RoleId)),
            Value(currentDate, executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::LastModifiedAt)),
            Value(username, executionContext.GetAllocator(), static_cast<column_index_t>(SysUsers::LastModifiedBy))
        };

        DataTypes::Indexing::Key key;
        key.InsertKey(DataTypes::Indexing::Key(&userId, sizeof(userId), DataType::Int, executionContext.GetAllocator()));

        return table->ClusteredIndexSeekUpdate(executionContext, key, updates);
    }
}