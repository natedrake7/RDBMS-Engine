#include "../../include/SystemDatabases/SystemCatalog.h"

#include "../../include/SystemDatabases/CatalogSchema.h"
#include "../../include/Database.h"
#include "../../include/DataStorage/Block.h"

#include <iostream>
#include <nlohmann/json.hpp>

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

 void SystemCatalog::ReadConfiguration(const std::string &configPath) {
  std::ifstream file(configPath);

  if (!file.is_open())
   throw std::runtime_error("System Tables file: " + configPath + " could not be opened");

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

 bool SystemCatalog::CatalogExists()const { return std::filesystem::exists(this->sysDbPath); }

 void SystemCatalog::UseCatalogDatabase() {
  this->masterDb = new Database(this->sysDbName, this->sysTables);
  this->masterDb->GetColumnsHeaders();
  this->masterDb->GetIdentityColumns();
 }

 void SystemCatalog::CreateCatalogDatabase() {
  CreateDatabase(this->sysDbName);

  this->masterDb = new Database(this->sysDbName, true);

  for (int i = 0;i < this->sysTables.size(); i++) {
   const auto& tableHeader = this->sysTables[i];

   std::vector<StorageTypes::Column *> columns;
   std::vector<column_index_t> primaryKeyIndexes;

   for (int columnIndex = 0;columnIndex < tableHeader.columns.size(); columnIndex++) {
    const auto& columnHeader = tableHeader.columns[columnIndex];

    block_size_t columnSize = 0;

    const auto normalizedColumnType = Functions::String::NormalizeString(columnHeader.type);

    if (!ColumnTypeSizes.TryGetValue(normalizedColumnType, columnSize))
     throw runtime_error("Column type " + columnHeader.type + " does not exist");

    if (columnSize == 0)
     columnSize = columnHeader.size;

    const auto columnType = ColumnTypesDictionary.Get(normalizedColumnType);

    for (const auto& key: tableHeader.primaryKey) {
     if (columnHeader.name != key)
      continue;

     primaryKeyIndexes.push_back(columnIndex);
    }

    auto* columnPtr = new StorageTypes::Column(columnHeader.name, columnType, columnSize, columnIndex, columnHeader.nullable);

    if (columnHeader.hasIdentity)
     columnPtr->SetIdentity(Headers::IdentityColumnsHeader(tableHeader.id, columnIndex, 1, 1, 1, true, 10000));

    columns.push_back(columnPtr);
   }

   if (primaryKeyIndexes.empty())
    throw runtime_error("All tables in masterDb must have a primary key");

   Headers::Index index(primaryKeyIndexes);
   this->masterDb->CreateTable(tableHeader.id, i, columns, &index);
  }
 }

 void SystemCatalog::StoreSystemTablesToCatalog()const {
    const auto dbInsertResult = this->InsertDbToMasterDb(
        this->baseProperties,
        this->sysDbName,
        this->sysDbPath,
        true
    );

    const auto databaseId = dbInsertResult.primaryKey.AsInt();

    const auto schemaInsertResult = this->InsertSchemaToMasterDb(
      this->baseProperties,
      databaseId,
      "dbo"
    );

    const auto schemaId = schemaInsertResult.primaryKey.AsInt(1);

    Dictionary<string, column_index_t> columnNameToIndex;

    int counter=  0;
    for (int i = 0;i < this->sysTables.size(); i++) {
      const auto& table = this->sysTables[i];

      const auto tableResult =
        this->InsertTableToMasterDb(
            this->baseProperties,
          databaseId,
          schemaId,
          table.name,
          static_cast<int16_t>(i),
          true
        );

      // const auto tableStatsResult =
      //   this->InsertTableStatisticsToMasterDb(
      //     this->baseProperties,
      //     tableResult.primaryKey.AsInt()
      //   );

      int columnPos = 0;
      const auto tableId = tableResult.primaryKey.AsInt(1);

      Dictionary<std::string, int32_t> columnIdsDict;

      for (auto& column: table.columns) {
        counter++;
        const auto normalizedColumnType = Functions::String::NormalizeString(column.type);

        auto columnSize = ColumnTypeSizes.Get(normalizedColumnType);

        if (columnSize == 0)
          columnSize = column.size;

        const auto& type = ColumnTypesDictionary.Get(normalizedColumnType);

        const auto columnResult =
          this->InsertColumnToMasterDb(
              this->baseProperties,
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
                this->baseProperties,
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

        concatenatedColumns +=  j > 0  ? "," + to_string(key) : to_string(key);
        _columns +="_" + table.primaryKey[j];
      }

      //TODO keep the last value keys
      const auto indexResult =
        this->InsertIndexToMasterDb(
          this->baseProperties,
          tableId,
          "PK" + _columns,
          true
      );

      auto indexId = indexResult.primaryKey.AsInt(1);

      const auto constraintResult =
        this->InsertConstraintToMasterDb(
          this->baseProperties,
          tableId,
          "PK" + _columns,
          Headers::ConstraintType::PrimaryKey,
          false,
          &indexId
      );

      for(int j = 0;j < table.primaryKey.size(); j++){
        auto _ = this->InsertIndexColumnToMasterDb(
            this->baseProperties,
            indexId,
            columnIdsDict.Get(table.primaryKey[j]),
            static_cast<int16_t>(j),
            true
        );

        _ = this->InsertConstraintColumnToMasterDb(
          this->baseProperties,
          constraintResult.primaryKey.AsInt(1),
          columnIdsDict.Get(table.primaryKey[j]),
          static_cast<int16_t>(j)
        );
      }
    }

    this->masterDb->GetColumnsHeaders();
    this->masterDb->UpdateIdentityManagersIds();
 }

  Headers::DatabaseHeader SystemCatalog::ToDatabaseHeader(const StorageTypes::Row *row){
    const auto& data = row->GetData();

    return Headers::DatabaseHeader{
      .id = data[static_cast<column_index_t>(SysDatabases::DatabaseId)]->GetInt(),
      .name = data[static_cast<column_index_t>(SysDatabases::Name)]->GetString(),
      .filepath = data[static_cast<column_index_t>(SysDatabases::FilePath)]->GetString(),
      .isSystem = data[static_cast<column_index_t>(SysDatabases::IsSystem)]->GetBool(),
    };
  }

  Headers::DatabaseHeader SystemCatalog::ToDatabaseHeader(
    const StorageTypes::Row *row,
    std::vector<Headers::TableHeader> &dbTables,
    std::vector<Headers::SchemaHeader> &schemas
  ) {
    const auto& data = row->GetData();

   return  Headers::DatabaseHeader{
      .id = data[static_cast<column_index_t>(SysDatabases::DatabaseId)]->GetInt(),
      .name = data[static_cast<column_index_t>(SysDatabases::Name)]->GetString(),
      .filepath = data[static_cast<column_index_t>(SysDatabases::FilePath)]->GetString(),
      .isSystem = data[static_cast<column_index_t>(SysDatabases::IsSystem)]->GetBool(),
      .additionalInfo = {
        .createdAt = data[static_cast<column_index_t>(SysDatabases::CreatedAt)]->GetDateTime(),
        .lastModified = data[static_cast<column_index_t>(SysDatabases::LastModifiedAt)]->GetDateTime(),
        .lastModifiedBy = data[static_cast<column_index_t>(SysDatabases::LastModifiedBy)]->GetString(),
        .version = data[static_cast<column_index_t>(SysDatabases::Version)]->GetInt(),
        .isDeleted = data[static_cast<column_index_t>(SysDatabases::IsDeleted)]->GetBool(),
        .deletedAt = data[static_cast<column_index_t>(SysDatabases::DeletedAt)]->GetRawData() == nullptr
                  ? DataTypes::DateTime()
                  : data[static_cast<column_index_t>(SysDatabases::DeletedAt)]->GetDateTime(),
        },
      .tables = std::move(dbTables),
      .schemas = std::move(schemas)
    };
}

  Headers::SchemaHeader SystemCatalog::ToSchemaHeader(const StorageTypes::Row *row){
    const auto& data = row->GetData();

    return Headers::SchemaHeader{
      data[static_cast<column_index_t>(SysSchemas::SchemaId)]->GetInt(),
      data[static_cast<column_index_t>(SysSchemas::DatabaseId)]->GetInt(),
      data[static_cast<column_index_t>(SysSchemas::Name)]->GetString(),
      data[static_cast<column_index_t>(SysSchemas::CreatedAt)]->GetDateTime(),
      data[static_cast<column_index_t>(SysSchemas::LastModifiedAt)]->GetDateTime(),
      data[static_cast<column_index_t>(SysSchemas::LastModifiedBy)]->GetString()
    };
  }

  Headers::TableHeader SystemCatalog::ToTableHeader(const StorageTypes::Row *row) {
    const auto& data = row->GetData();

    return Headers::TableHeader{
      data[static_cast<column_index_t>(SysTables::DatabaseId)]->GetInt(),
      data[static_cast<column_index_t>(SysTables::TableId)]->GetInt(),
      data[static_cast<column_index_t>(SysTables::SchemaId)]->GetInt(),
      data[static_cast<column_index_t>(SysTables::Name)]->GetString(),
      data[static_cast<column_index_t>(SysTables::OrdinalPosition)]->GetSmallInt(),
      data[static_cast<column_index_t>(SysTables::IsSystemTable)]->GetBool(),
      data[static_cast<column_index_t>(SysTables::CreatedAt)]->GetDateTime(),
      data[static_cast<column_index_t>(SysTables::LastModifiedAt)]->GetDateTime(),
      data[static_cast<column_index_t>(SysTables::LastModifiedBy)]->GetString()
      };
  }

  Headers::ColumnHeader SystemCatalog::ToColumnHeader(const StorageTypes::Row *row) {
      const auto& data = row->GetData();

      return Headers::ColumnHeader{
          .tableId = data[static_cast<column_index_t>(SysColumns::TableId)]->GetInt(),
          .id = data[static_cast<column_index_t>(SysColumns::ColumnId)]->GetInt(),
          .name = data[static_cast<column_index_t>(SysColumns::Name)]->GetString(),
          .dataType = static_cast<uint8_t>(data[static_cast<column_index_t>(SysColumns::DataType)]->GetTinyInt()),
          .recordSize = data[static_cast<column_index_t>(SysColumns::RecordSize)]->GetInt(),
          .precision = data[static_cast<column_index_t>(SysColumns::Precision)]->GetRawData() == nullptr
              ? INVALID_DECIMAL_PRECISION
              : data[static_cast<column_index_t>(SysColumns::Precision)]->GetTinyInt(),
          .scale = data[static_cast<column_index_t>(SysColumns::Scale)]->GetRawData() == nullptr
              ? INVALID_DECIMAL_SCALE
              : data[static_cast<column_index_t>(SysColumns::Scale)]->GetTinyInt(),
          .isNullable = data[static_cast<column_index_t>(SysColumns::IsNullable)]->GetBool(),
          .ordinalPosition = data[static_cast<column_index_t>(SysColumns::OrdinalPosition)]->GetSmallInt(),
          .isSystem = data[static_cast<column_index_t>(SysColumns::IsSystemColumn)]->GetBool(),
          .additionalInfo{
            .createdAt = data[static_cast<column_index_t>(SysColumns::CreatedAt)]->GetDateTime(),
            .lastModified = data[static_cast<column_index_t>(SysColumns::LastModifiedAt)]->GetDateTime(),
            .lastModifiedBy = data[static_cast<column_index_t>(SysColumns::LastModifiedBy)]->GetString(),
            .version = data[static_cast<column_index_t>(SysColumns::Version)]->GetInt(),
            .isDeleted = data[static_cast<column_index_t>(SysColumns::IsDeleted)]->GetBool(),
            .deletedAt = data[static_cast<column_index_t>(SysColumns::DeletedAt)]->GetRawData() == nullptr
                      ? DataTypes::DateTime::Now()
                      : data[static_cast<column_index_t>(SysColumns::DeletedAt)]->GetDateTime(),
            }
      };
  }

  Headers::IndexHeader SystemCatalog::ToIndexHeader(const StorageTypes::Row *row) {
      const auto& data = row->GetData();

      return Headers::IndexHeader{
        .tableId = data[static_cast<column_index_t>(SysIndexes::TableId)]->GetInt(),
        .id = data[static_cast<column_index_t>(SysIndexes::IndexId)]->GetInt(),
        .name = data[static_cast<column_index_t>(SysIndexes::Name)]->GetString(),
        .isClustered = data[static_cast<column_index_t>(SysIndexes::IsClustered)]->GetBool(),
        .isDisabled = data[static_cast<column_index_t>(SysIndexes::IsDisabled)]->GetBool(),
          .additionalInfo{
          .createdAt = data[static_cast<column_index_t>(SysIndexes::CreatedAt)]->GetDateTime(),
          .lastModified = data[static_cast<column_index_t>(SysIndexes::LastModifiedAt)]->GetDateTime(),
          .lastModifiedBy = data[static_cast<column_index_t>(SysIndexes::LastModifiedBy)]->GetString(),
          .version = data[static_cast<column_index_t>(SysIndexes::Version)]->GetInt(),
          .isDeleted = data[static_cast<column_index_t>(SysIndexes::IsDeleted)]->GetBool(),
          .deletedAt = data[static_cast<column_index_t>(SysIndexes::DeletedAt)]->GetRawData() == nullptr
                ? DataTypes::DateTime()
                : data[static_cast<column_index_t>(SysIndexes::DeletedAt)]->GetDateTime()
          },
    };
  }

  Headers::IndexColumnsHeader SystemCatalog::ToIndexColumnsHeader(const StorageTypes::Row *row) {
    const auto& data = row->GetData();

    return Headers::IndexColumnsHeader{
      .indexId = data[static_cast<column_index_t>(SysIndexColumns::IndexId)]->GetInt(),
      .columnId = data[static_cast<column_index_t>(SysIndexColumns::ColumnId)]->GetInt(),
      .ordinalPosition = data[static_cast<column_index_t>(SysIndexColumns::OrdinalPosition)]->GetSmallInt(),
      .isIncluded = data[static_cast<column_index_t>(SysIndexColumns::IsIncluded)]->GetBool(),
      .additionalInfo{
        .version = data[static_cast<column_index_t>(SysIndexColumns::Version)]->GetInt(),
        .isDeleted = data[static_cast<column_index_t>(SysIndexColumns::IsDeleted)]->GetBool(),
        .deletedAt = data[static_cast<column_index_t>(SysIndexColumns::DeletedAt)]->GetRawData() == nullptr
              ? DataTypes::DateTime()
              : data[static_cast<column_index_t>(SysIndexColumns::DeletedAt)]->GetDateTime()
      }
    };
  }

  Headers::IdentityColumnsHeader SystemCatalog::ToIdentityColumnsHeader(const StorageTypes::Row *row) {
    const auto& data = row->GetData();

    return Headers::IdentityColumnsHeader{
      .tableId = data[static_cast<column_index_t>(SysIdentityColumns::TableId)]->GetInt(),
      .columnId = data[static_cast<column_index_t>(SysIdentityColumns::ColumnId)]->GetInt(),
      .seedValue = data[static_cast<column_index_t>(SysIdentityColumns::SeedValue)]->GetInt(),
      .increment = data[static_cast<column_index_t>(SysIdentityColumns::IncrementValue)]->GetInt(),
      .lastValue = data[static_cast<column_index_t>(SysIdentityColumns::LastValue)]->GetBigInt(),
      .isCached = data[static_cast<column_index_t>(SysIdentityColumns::IsCached)]->GetBool(),
      .cacheBlock = data[static_cast<column_index_t>(SysIdentityColumns::CacheBlock)]->GetInt(),
      .additionalInfo{
        .version = data[static_cast<column_index_t>(SysIdentityColumns::Version)]->GetInt(),
        .isDeleted = data[static_cast<column_index_t>(SysIdentityColumns::IsDeleted)]->GetBool(),
        .deletedAt = data[static_cast<column_index_t>(SysIdentityColumns::DeletedAt)]->GetRawData() == nullptr
              ? DataTypes::DateTime()
              : data[static_cast<column_index_t>(SysIdentityColumns::DeletedAt)]->GetDateTime()
      }
    };
  }

  Headers::ConstraintsHeader SystemCatalog::ToConstraintsHeader(
    const StorageTypes::Row *row,
    std::vector<Headers::ConstraintsColumnsHeader> &constraintColumns,
    Headers::IndexHeader &indexHeader
  ) {
    const auto& data = row->GetData();

    return Headers::ConstraintsHeader{
      .tableId = data[static_cast<column_index_t>(SysConstraints::TableId)]->GetInt(),
      .constraintId = data[static_cast<column_index_t>(SysConstraints::ConstraintId)]->GetInt(),
      .name = data[static_cast<column_index_t>(SysConstraints::Name)]->GetString(),
      .type = static_cast<Headers::ConstraintType>(data[static_cast<column_index_t>(SysConstraints::Type)]->GetTinyInt()),
      .isDisabled = data[static_cast<column_index_t>(SysConstraints::IsDisabled)]->GetBool(),
      .indexId = indexHeader.id,
      .index = std::move(indexHeader),
      .columns = std::move(constraintColumns),
      .additionalInfo{
        .createdAt = data[static_cast<column_index_t>(SysConstraints::CreatedAt)]->GetDateTime(),
        .lastModified = data[static_cast<column_index_t>(SysConstraints::LastModifiedAt)]->GetDateTime(),
        .lastModifiedBy = data[static_cast<column_index_t>(SysConstraints::LastModifiedBy)]->GetString(),
        .version = data[static_cast<column_index_t>(SysConstraints::Version)]->GetInt(),
        .isDeleted = data[static_cast<column_index_t>(SysConstraints::IsDeleted)]->GetBool(),
        .deletedAt = data[static_cast<column_index_t>(SysConstraints::DeletedAt)]->GetRawData() == nullptr
              ? DataTypes::DateTime()
              : data[static_cast<column_index_t>(SysConstraints::DeletedAt)]->GetDateTime() //might crash, is nullable
      },
    };
  }

  Headers::ConstraintsColumnsHeader SystemCatalog::ToConstraintsColumnsHeader(const StorageTypes::Row *row){
    const auto& data = row->GetData();

    return Headers::ConstraintsColumnsHeader{
      .constraintId = data[static_cast<column_index_t>(SysConstraintColumns::ConstraintId)]->GetInt(),
      .columnId = data[static_cast<column_index_t>(SysConstraintColumns::ColumnId)]->GetInt(),
      .ordinalPosition = data[static_cast<column_index_t>(SysConstraintColumns::OrdinalPosition)]->GetInt(),
      .additionalInfo{
        .version = data[static_cast<column_index_t>(SysConstraintColumns::Version)]->GetInt(),
        .isDeleted = data[static_cast<column_index_t>(SysConstraintColumns::IsDeleted)]->GetBool(),
        .deletedAt = data[static_cast<column_index_t>(SysConstraintColumns::DeletedAt)]->GetRawData() == nullptr
              ? DataTypes::DateTime()
              : data[static_cast<column_index_t>(SysConstraintColumns::DeletedAt)]->GetDateTime()
      },
    };
  }

  Headers::DefaultValuesHeader SystemCatalog::ToDefaultValuesHeader(const StorageTypes::Row *row) {
    const auto& data = row->GetData();

    return Headers::DefaultValuesHeader{
      .columnId = data[static_cast<column_index_t>(SysDefaultValues::ColumnId)]->GetInt(),
      .value = data[static_cast<column_index_t>(SysDefaultValues::Value)]->GetString(),
      .additionalInfo{
        .version = data[static_cast<column_index_t>(SysDefaultValues::Version)]->GetInt(),
        .isDeleted = data[static_cast<column_index_t>(SysDefaultValues::IsDeleted)]->GetBool(),
        .deletedAt = data[static_cast<column_index_t>(SysDefaultValues::DeletedAt)]->GetRawData() == nullptr
              ? DataTypes::DateTime()
              : data[static_cast<column_index_t>(SysDefaultValues::DeletedAt)]->GetDateTime()
      },
    };
  }

  Headers::TableStatistics SystemCatalog::ToTableStatistics(const StorageTypes::Row *row) {
    const auto& data = row->GetData();

    return {
      data[static_cast<column_index_t>(SysTableStats::TableId)]->GetInt(),
      data[static_cast<column_index_t>(SysTableStats::RowCount)]->GetBigInt(),
      data[static_cast<column_index_t>(SysTableStats::AvgRowSize)]->GetInt(),
      data[static_cast<column_index_t>(SysTableStats::PageCount)]->GetInt(),
      data[static_cast<column_index_t>(SysTableStats::LastUpdatedAt)]->GetDateTime()
    };
  }

  Headers::ColumnStatistics SystemCatalog::ToColumnStatistics(const StorageTypes::Row *row, const DataType& columnType) {
    const auto& data = row->GetData();

    return Headers::ColumnStatistics{
      .columnId = data[static_cast<column_index_t>(SysColumnStats::ColumnId)]->GetInt(),
      .distinctCount = data[static_cast<column_index_t>(SysColumnStats::DistinctCount)]->GetBigInt(),
      .min = Value(data[static_cast<column_index_t>(SysColumnStats::MinimumValue)]->GetRawData(), data[static_cast<column_index_t>(SysColumnStats::MinimumValue)]->GetSize(), columnType),
      .max = Value(data[static_cast<column_index_t>(SysColumnStats::MaximumValue)]->GetRawData(), data[static_cast<column_index_t>(SysColumnStats::MaximumValue)]->GetSize(), columnType),
      .nullCount = data[static_cast<column_index_t>(SysColumnStats::NullCount)]->GetBigInt()
    };
  }

  Headers::ColumnHistograms SystemCatalog::ToColumnHistograms(const StorageTypes::Row *row, const DataType &columnType) {
    const auto& data = row->GetData();

    return Headers::ColumnHistograms{
      .columnId = data[static_cast<column_index_t>(SysColumnHistograms::ColumnId)]->GetInt(),
      .histogramId = data[static_cast<column_index_t>(SysColumnHistograms::HistogramId)]->GetInt(),
      .rangeStart = Value(data[static_cast<column_index_t>(SysColumnHistograms::RangeStart)]->GetRawData(), data[static_cast<column_index_t>(SysColumnHistograms::RangeStart)]->GetSize(), columnType),
      .rangeEnd = Value(data[static_cast<column_index_t>(SysColumnHistograms::RangeEnd)]->GetRawData(), data[static_cast<column_index_t>(SysColumnHistograms::RangeEnd)]->GetSize(), columnType),
      .rowCount = data[static_cast<column_index_t>(SysColumnHistograms::RowCount)]->GetInt(),
      .distinctCount = data[static_cast<column_index_t>(SysColumnHistograms::DistinctCount)]->GetInt(),
    };
  }

  Headers::IndexStatistics SystemCatalog::ToIndexStatistics(const DatabaseEngine::StorageTypes::Row *row) {
   const auto& data = row->GetData();

   return {
    data[static_cast<column_index_t>(SysIndexStats::TableId)]->GetInt(),
    data[static_cast<column_index_t>(SysIndexStats::IndexId)]->GetInt(),
    data[static_cast<column_index_t>(SysIndexStats::LeafPages)]->GetInt(),
    data[static_cast<column_index_t>(SysIndexStats::Depth)]->GetTinyInt(),
    data[static_cast<column_index_t>(SysIndexStats::AverageFragmentation)]->GetDecimal(),
    data[static_cast<column_index_t>(SysIndexStats::LastUpdated)]->GetDateTime(),
   };
 }

  SystemCatalog & SystemCatalog::Get() {
   static SystemCatalog instance;
   return instance;
 }

  Database * SystemCatalog::GetDatabase() const{ return this->masterDb; }

  bool SystemCatalog::Initialize(const std::string &configPath) {
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
   this->masterDb->UpdateMasterDatabase();

   delete this->masterDb;
   this->masterDb = nullptr;
 }

 std::vector<Headers::DatabaseHeader> SystemCatalog::RetrieveCatalog() const {
   auto* sysDatabases = this->masterDb->OpenTable(CatalogTables::SysDatabases);

   std::vector<const StorageTypes::Row*> selectedDatabases;

   sysDatabases->ClusteredIndexScan(this->baseProperties, &selectedDatabases);

   vector<Headers::DatabaseHeader> databasesHeaders;

   if (selectedDatabases.empty())
     return {};

   for (const auto& row : selectedDatabases) {

     const auto& data = row->GetData();

     const auto databaseId = data[static_cast<column_index_t>(SysDatabases::DatabaseId)]->GetInt();

     auto schemas = this->SelectSchemas(databaseId);

     auto tables = this->SelectTables(databaseId);

     for (auto& table : tables) {
       table.columns = this->SelectColumns(table.id);
       table.statistics = this->SelectTableStatisticsById(table.id);

       const auto identityColumns = this->SelectIdentityColumnsByTableIdToDictionary(table.id);

       for (auto& column : table.columns) {
         Headers::IdentityColumnsHeader identityHeader;
         identityColumns.TryGetValue(column.id, identityHeader);

         column.identity = std::move(identityHeader);
         column.defaultValue = this->SelectDefaultValueByColumnId(column.id);
         column.statistics = this->SelectColumnStatisticsById(column.id, static_cast<DataType>(column.dataType));
       }

       table.constraints = this->SelectConstraints(table.id);
       // table.identity = this->SelectIdentityColumnsByTableId(table.id);
     }

     databasesHeaders.emplace_back(SystemCatalog::ToDatabaseHeader(row, tables, schemas));
   }

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
      this->baseProperties,
      admin,
      Constants::ADMIN_PERMISSIONS
    );

   roles.push_back(new Security::Role(
        result.primaryKey.AsInt(),
        admin,
        Constants::ADMIN_PERMISSIONS,
        true
    ));

    result = this->InsertRoleToMasterDb(
      this->baseProperties,
      dbOwner,
      Constants::DB_OWNER_PERMISSIONS
    );

   roles.push_back(new Security::Role(
        result.primaryKey.AsInt(),
        dbOwner,
        Constants::DB_OWNER_PERMISSIONS,
        true
    ));

    result = this->InsertRoleToMasterDb(
      this->baseProperties,
      dbWriter,
      Constants::DB_WRITER_PERMISSIONS
    );

     roles.push_back(new Security::Role(
       result.primaryKey.AsInt(),
       dbWriter,
       Constants::DB_WRITER_PERMISSIONS,
       true
    ));

    result = this->InsertRoleToMasterDb(
      this->baseProperties,
      dbReader,
      Constants::DB_READER_PERMISSIONS
    );

    roles.push_back(new Security::Role(
      result.primaryKey.AsInt(),
      dbReader,
      Constants::DB_READER_PERMISSIONS,
      true
    ));

    result = this->InsertRoleToMasterDb(
      this->baseProperties,
      guest,
      Constants::GUEST_PERMISSIONS
    );

    roles.push_back(new Security::Role(
       result.primaryKey.AsInt(),
       guest,
       Constants::GUEST_PERMISSIONS,
       true
    ));

   return roles;
  }

  Security::User* SystemCatalog::InsertSystemUsers(const std::string& hashedPassword, const int32_t& defaultRoleId)const{
    const auto admin = std::string(Constants::ADMIN_NAME);

    const auto result =
      this->InsertUserToMasterDb(
          this->baseProperties,
        admin,
        hashedPassword,
        defaultRoleId,
        true
      );

   return new Security::User{
      .id = result.primaryKey.AsInt(),
      .name = admin,
      .passwordHash =  hashedPassword,
      .roleId = defaultRoleId,
      .isActive = true
   };
  }

Errors::RuntimeStatus SystemCatalog::InsertDbToMasterDb(
    const ExecutionProperties& properties,
    const string& dbName,
    const string& dbPath,
    const bool& isSystem,
    const string& user,
    const int& version,
    const bool& isDeleted
  ) const{
      StorageTypes::Table* table = this->masterDb->OpenTable(CatalogTables::SysDatabases);

      const auto currentDate = DataTypes::DateTime::Now();

      const vector<Value> fields = {
        Value(dbName, static_cast<column_index_t>(SysDatabases::Name)),
        Value(dbPath, static_cast<column_index_t>(SysDatabases::FilePath)),
        Value(isSystem, static_cast<column_index_t>(SysDatabases::IsSystem)),
        Value(currentDate, static_cast<column_index_t>(SysDatabases::CreatedAt)),
        Value(currentDate, static_cast<column_index_t>(SysDatabases::LastModifiedAt)),
        Value(user, static_cast<column_index_t>(SysDatabases::LastModifiedBy)),
        Value(version, static_cast<column_index_t>(SysDatabases::Version)),
        Value(isDeleted, static_cast<column_index_t>(SysDatabases::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysDatabases::DeletedAt))
    };

    const auto result = table->InsertRow(properties, fields);

    cout << "Inserted database: "<< dbName << " to master db" << endl;

    return result;
  }

  Errors::RuntimeStatus  SystemCatalog::InsertSchemaToMasterDb(
    const ExecutionProperties& properties,
    const int32_t &databaseId,
    const string &schemaName,
    const string &user,
    const int& version,
    const bool& isDeleted
  ) const{
     StorageTypes::Table* table = this->masterDb->OpenTable(CatalogTables::SysSchemas);
     const auto currentDate = DataTypes::DateTime::Now();

     const std::vector<Value> fields = {
        Value(databaseId, static_cast<column_index_t>(SysSchemas::DatabaseId)),
        Value(schemaName, static_cast<column_index_t>(SysSchemas::Name)),
        Value(currentDate, static_cast<column_index_t>(SysSchemas::CreatedAt)),
        Value(currentDate, static_cast<column_index_t>(SysSchemas::LastModifiedAt)),
        Value(user, static_cast<column_index_t>(SysSchemas::LastModifiedBy)),
        Value(version, static_cast<column_index_t>(SysSchemas::Version)),
        Value(isDeleted, static_cast<column_index_t>(SysSchemas::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysSchemas::DeletedAt)),
     };

    const auto result = table->InsertRow(properties, fields);

    cout << "Inserted schema: "<< schemaName << " to master db" << endl;

    return result;
  }

  Errors::RuntimeStatus  SystemCatalog::InsertTableToMasterDb(
    const ExecutionProperties& properties,
    const int32_t & databaseId,
    const int32_t & schemaId,
    const string& tableName,
    const int16_t& ordinalPosition,
    const bool& isSystem,
    const string& user,
    const int& version,
    const bool& isDeleted
  ) const{

      StorageTypes::Table* table = this->masterDb->OpenTable(CatalogTables::SysTables);
      const auto currentDate = DataTypes::DateTime::Now();

      const std::vector<Value> fields = {
        Value(databaseId, static_cast<column_index_t>(SysTables::DatabaseId)),
        Value(schemaId, static_cast<column_index_t>(SysTables::SchemaId)),
        Value(tableName, static_cast<column_index_t>(SysTables::Name)),
        Value(ordinalPosition, static_cast<column_index_t>(SysTables::OrdinalPosition)),
        Value(isSystem, static_cast<column_index_t>(SysTables::IsSystemTable)),
        Value(currentDate, static_cast<column_index_t>(SysTables::CreatedAt)),
        Value(currentDate, static_cast<column_index_t>(SysTables::LastModifiedAt)),
        Value(user, static_cast<column_index_t>(SysTables::LastModifiedBy)),
        Value(version, static_cast<column_index_t>(SysTables::Version)),
        Value(isDeleted, static_cast<column_index_t>(SysTables::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysTables::DeletedAt)),
      };

      const auto result = table->InsertRow(properties, fields);

      cout << "Inserted table: "<< tableName << " to master db" << endl;

      return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertColumnToMasterDb(
    const ExecutionProperties& properties,
    const int32_t & tableId,
    const string &columnName,
    const DataType &columnType,
    const int &columnSize,
    const int8_t& precision,
    const int8_t& scale,
    const bool& isNullable,
    const int &ordinalPosition,
    const bool& isSystem,
    const string& user,
    const int& version,
    const bool& isDeleted
  ) const{
      auto* table = this->masterDb->OpenTable(CatalogTables::SysColumns);

      const auto currentDate = DataTypes::DateTime::Now();

      std::vector<Value> fields = {
        Value(tableId, static_cast<column_index_t>(SysColumns::TableId)),
        Value(columnName, static_cast<column_index_t>(SysColumns::Name)),
        Value(static_cast<int8_t>(columnType), static_cast<column_index_t>(SysColumns::DataType)),
        Value(columnSize, static_cast<column_index_t>(SysColumns::RecordSize)),
        Value(isNullable, static_cast<column_index_t>(SysColumns::IsNullable)),
        Value(ordinalPosition, static_cast<column_index_t>(SysColumns::OrdinalPosition)),
        Value(isSystem, static_cast<column_index_t>(SysColumns::IsSystemColumn)),
        Value(currentDate, static_cast<column_index_t>(SysColumns::CreatedAt)),
        Value(currentDate, static_cast<column_index_t>(SysColumns::LastModifiedAt)),
        Value(user, static_cast<column_index_t>(SysColumns::LastModifiedBy)),
        Value(version, static_cast<column_index_t>(SysColumns::Version)),
        Value(isDeleted, static_cast<column_index_t>(SysColumns::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysColumns::DeletedAt)),
      };

      if (precision != INVALID_DECIMAL_PRECISION) {
        fields.push_back(Value(precision, static_cast<column_index_t>(SysColumns::Precision)));
        fields.push_back(Value(scale, static_cast<column_index_t>(SysColumns::Scale)));
      }
      else {
        fields.push_back( Value::Null(static_cast<column_index_t>(SysColumns::Precision)));
        fields.push_back(Value::Null(static_cast<column_index_t>(SysColumns::Scale)));
      }

      const auto result = table->InsertRow(properties, fields);

      cout << "Inserted column: "<< columnName << " to master db" << endl;

      return result;
  }

  Errors::RuntimeStatus  SystemCatalog::InsertIndexToMasterDb(
    const ExecutionProperties& properties,
    const int32_t & tableId,
    const string &indexName,
    const bool &isClustered,
    const bool &isDisabled,
    const string &user,
    const int& version,
    const bool& isDeleted
  ) const{
     StorageTypes::Table* table = this->masterDb->OpenTable(CatalogTables::SysIndexes);
     const auto currentDate = DataTypes::DateTime::Now();

     const std::vector<Value> fields = {
       Value(tableId, static_cast<column_index_t>(SysIndexes::TableId)),
       Value(indexName, static_cast<column_index_t>(SysIndexes::Name)),
       Value(isClustered, static_cast<column_index_t>(SysIndexes::IsClustered)),
       Value(isDisabled, static_cast<column_index_t>(SysIndexes::IsDisabled)),
       Value(currentDate, static_cast<column_index_t>(SysIndexes::CreatedAt)),
       Value(currentDate, static_cast<column_index_t>(SysIndexes::LastModifiedAt)),
       Value(user, static_cast<column_index_t>(SysIndexes::LastModifiedBy)),
       Value(version, static_cast<column_index_t>(SysIndexes::Version)),
       Value(isDeleted, static_cast<column_index_t>(SysIndexes::IsDeleted)),
      Value::Null(static_cast<column_index_t>(SysIndexes::DeletedAt)),
     };

      const auto result = table->InsertRow(properties, fields);

      cout << "Inserted index: "<< indexName << " to master db" << endl;

      return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertIndexColumnToMasterDb(
    const ExecutionProperties& properties,
    const int32_t & indexId,
    const int32_t & columnId,
    const int16_t & ordinalPosition,
    const bool & isIncluded,
    const int& version,
    const bool& isDeleted) const{
    StorageTypes::Table* table = this->masterDb->OpenTable(CatalogTables::SysIndexColumns);
    const auto currentDate = DataTypes::DateTime::Now();

    const std::vector<Value> fields = {
      Value(indexId, static_cast<column_index_t>(SysIndexColumns::IndexId)),
      Value(columnId, static_cast<column_index_t>(SysIndexColumns::ColumnId)),
      Value(ordinalPosition, static_cast<column_index_t>(SysIndexColumns::OrdinalPosition)),
      Value(isIncluded, static_cast<column_index_t>(SysIndexColumns::IsIncluded)),
      Value(version, static_cast<column_index_t>(SysIndexColumns::Version)),
      Value(isDeleted, static_cast<column_index_t>(SysIndexColumns::IsDeleted)),
      Value::Null(static_cast<column_index_t>(SysIndexColumns::DeletedAt)),
    };

    const auto result = table->InsertRow(properties, fields);

    cout << "Inserted index column to master db" << endl;

    return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertConstraintToMasterDb(
      const ExecutionProperties& properties,
      const int32_t & tableId,
      const string & constraintName,
      const Headers::ConstraintType & constraintType,
      const bool & isDisabled,
      const int32_t *constraintIndexId,
      const string & user,
      const int& version,
      const bool& isDeleted
  ) const{

      auto* table = this->masterDb->OpenTable(CatalogTables::SysConstraints);

      const auto currentDate = DataTypes::DateTime::Now();

      std::vector<Value> fields = {
          Value(tableId, static_cast<column_index_t>(SysConstraints::TableId)),
          Value(constraintName, static_cast<column_index_t>(SysConstraints::Name)),
          Value(static_cast<int8_t>(constraintType), static_cast<column_index_t>(SysConstraints::Type)),
          Value(isDisabled, static_cast<column_index_t>(SysConstraints::IsDisabled)),
          Value(currentDate, static_cast<column_index_t>(SysConstraints::CreatedAt)),
          Value(currentDate, static_cast<column_index_t>(SysConstraints::LastModifiedAt)),
          Value(user, static_cast<column_index_t>(SysConstraints::LastModifiedBy)),
          Value(version, static_cast<column_index_t>(SysConstraints::Version)),
          Value(isDeleted, static_cast<column_index_t>(SysConstraints::IsDeleted)),
          Value::Null(static_cast<column_index_t>(SysConstraints::DeletedAt)),
      };

      auto indexValue = (constraintIndexId != nullptr)
          ? Value(*constraintIndexId, static_cast<column_index_t>(SysConstraints::IndexId))
          : Value::Null(static_cast<column_index_t>(SysConstraints::IndexId));

      fields.push_back(std::move(indexValue));

    const auto result = table->InsertRow(properties, fields);

    cout << "Inserted constraint: "<< constraintName <<" to master db" << endl;

    return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertConstraintColumnToMasterDb(
    const ExecutionProperties& properties,
    const int32_t & constraintId,
    const int32_t & columnId,
    const int32_t & ordinalPosition,
    const int& version,
    const bool& isDeleted
  ) const{

    auto* table = this->masterDb->OpenTable(CatalogTables::SysConstraintColumns);
    const auto currentDate = DataTypes::DateTime::Now();

    const std::vector<Value> fields = {
        Value(constraintId, static_cast<column_index_t>(SysConstraintColumns::ConstraintId)),
        Value(columnId, static_cast<column_index_t>(SysConstraintColumns::ColumnId)),
        Value(ordinalPosition, static_cast<column_index_t>(SysConstraintColumns::OrdinalPosition)),
        Value(version, static_cast<column_index_t>(SysConstraintColumns::Version)),
        Value(isDeleted, static_cast<column_index_t>(SysConstraintColumns::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysConstraintColumns::DeletedAt)),
    };

    const auto result = table->InsertRow(properties, fields);

    cout << "Inserted constraint column to master db" << endl;

    return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertIdentityColumnToMasterDb(
      const ExecutionProperties& properties,
      const int32_t & tableId,
      const int32_t & columnId,
      const int32_t & seedValue,
      const int32_t & increment,
      const int32_t & lastValue,
      const bool & isCached,
      const int32_t & cacheBlock,
      const int& version,
      const bool& isDeleted
  ) const{

      auto* table = this->masterDb->OpenTable(CatalogTables::SysIdentityColumns);
      const auto currentDate = DataTypes::DateTime::Now();

      const std::vector<Value> fields = {
        Value(tableId, static_cast<column_index_t>(SysIdentityColumns::TableId)),
        Value(columnId, static_cast<column_index_t>(SysIdentityColumns::ColumnId)),
        Value(seedValue, static_cast<column_index_t>(SysIdentityColumns::SeedValue)),
        Value(increment, static_cast<column_index_t>(SysIdentityColumns::IncrementValue)),
        Value(lastValue, static_cast<column_index_t>(SysIdentityColumns::LastValue)),
        Value(isCached, static_cast<column_index_t>(SysIdentityColumns::IsCached)),
        Value(cacheBlock, static_cast<column_index_t>(SysIdentityColumns::CacheBlock)),
        Value(version, static_cast<column_index_t>(SysIdentityColumns::Version)),
        Value(isDeleted, static_cast<column_index_t>(SysIdentityColumns::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysIdentityColumns::DeletedAt)),
      };

    const auto result = table->InsertRow(properties, fields);

    cout << "Inserted identity column to master db" << endl;

    return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertDefaultValuesToMasterDb(
    const ExecutionProperties& properties,
    const int32_t &columnId,
    const Value &value,
    const int &version,
    const bool &isDeleted) const{

      auto* table = this->masterDb->OpenTable(CatalogTables::SysDefaultValues);
      const auto currentDate = DataTypes::DateTime::Now();

      const std::vector<Value> fields = {
        Value(columnId, static_cast<column_index_t>(SysDefaultValues::ColumnId)),
        Value(
      std::string(reinterpret_cast<const char*>(value.GetRawData()), value.GetSize()),
           static_cast<column_index_t>(SysDefaultValues::Value)
        ),
        Value(version, static_cast<column_index_t>(SysDefaultValues::Version)),
        Value(isDeleted, static_cast<column_index_t>(SysDefaultValues::IsDeleted)),
        Value::Null(static_cast<column_index_t>(SysDefaultValues::DeletedAt)),
      };

      const auto result = table->InsertRow(properties, fields);

      std::cout << "Inserted default value " << value << " to master db" << std::endl;

      return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertTableStatisticsToMasterDb(
    const ExecutionProperties& properties,
    const int32_t &tableId,
    const int64_t& rowCount,
    const int32_t& rowSize,
    const int32_t& pageCount
  ) const{

    StorageTypes::Table* table = this->masterDb->OpenTable(CatalogTables::SysTableStats);

    const std::vector fields = {
      Value(tableId, static_cast<column_index_t>(SysTableStats::TableId)),
      Value(rowCount, static_cast<column_index_t>(SysTableStats::RowCount)),
      Value(rowSize, static_cast<column_index_t>(SysTableStats::AvgRowSize)),
      Value(pageCount, static_cast<column_index_t>(SysTableStats::PageCount)),
      Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(SysTableStats::LastUpdatedAt)),
    };

    const auto result = table->InsertRow(properties, fields);

    std::cout << "Inserted table stats for table with id: " << tableId << std::endl;

    return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertColumnStatisticsToMasterDb(
    const ExecutionProperties& properties,
    const int32_t &columnId,
    const int64_t &distinctCount,
    const int64_t &nullCount
  ) const{

    StorageTypes::Table* table = this->masterDb->OpenTable(CatalogTables::SysColumnStats);

    const std::vector fields = {
      Value(columnId, static_cast<column_index_t>(SysColumnStats::ColumnId)),
      Value(distinctCount, static_cast<column_index_t>(SysColumnStats::DistinctCount)),
      Value::Null(static_cast<column_index_t>(SysColumnStats::MinimumValue)),
      Value::Null(static_cast<column_index_t>(SysColumnStats::MaximumValue)),
      Value(nullCount, static_cast<column_index_t>(SysColumnStats::NullCount))
    };

    const auto result = table->InsertRow(properties, fields);

    std::cout << "Inserted column stats for column with id: " << columnId << std::endl;

    return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertColumnHistogramsToMasterDb(
    const ExecutionProperties &properties,
    const int32_t &columnId,
    const Value &min,
    const Value &max,
    const int64_t &distinctCount
  ) const {

    const std::vector<Value> fields = {
      Value(columnId, static_cast<column_index_t>(SysColumnHistograms::ColumnId)),
      Value(std::string(reinterpret_cast<const char*>(min.GetRawData()), min.GetSize()), static_cast<column_index_t>(SysColumnHistograms::RangeStart)),
      Value(std::string(reinterpret_cast<const char*>(max.GetRawData()), max.GetSize()), static_cast<column_index_t>(SysColumnHistograms::RangeEnd)),
      Value(0, static_cast<column_index_t>(SysColumnHistograms::RowCount)),
      Value(distinctCount, static_cast<column_index_t>(SysColumnHistograms::DistinctCount)),
    };

    auto* table = this->masterDb->OpenTable(CatalogTables::SysColumnHistograms);

    const auto result = table->InsertRow(properties, fields);

    std::cout << "Inserted histogram Bucket for column: " << columnId << std::endl;

    return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertIndexStatisticsToMasterDb(
    const ExecutionProperties &properties,
    const int32_t& tableId,
    const int32_t &indexId,
    const int64_t &leafPages,
    const int8_t &depth,
    const DataTypes::Decimal &averageFragmentation
  ) const{

   auto* table = this->masterDb->OpenTable(CatalogTables::SysIndexStats);

   const std::vector fields = {
     Value(tableId, static_cast<column_index_t>(SysIndexStats::TableId)),
     Value(indexId, static_cast<column_index_t>(SysIndexStats::IndexId)),
     Value(leafPages, static_cast<column_index_t>(SysIndexStats::LeafPages)),
     Value(depth, static_cast<column_index_t>(SysIndexStats::Depth)),
     Value(averageFragmentation, static_cast<column_index_t>(SysIndexStats::AverageFragmentation)),
     Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(SysIndexStats::LastUpdated)),
   };

   const auto result = table->InsertRow(properties, fields);

   std::cout << "Inserted index statistics for index: " << indexId << std::endl;

   return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertRoleToMasterDb(
    const ExecutionProperties& properties,
    const std::string &roleName,
    const Security::Permission &permissions,
    const bool& isSystem,
    const int &version,
    const bool &isDeleted
  ) const{

    auto* table = this->masterDb->OpenTable(CatalogTables::SysRoles);
    const auto currentDate = DataTypes::DateTime::Now();

    const std::string lastModifiedBy = "system";

    const vector<Value> fields = {
      Value(roleName, static_cast<column_index_t>(SysRoles::RoleName)),
      Value(static_cast<int>(permissions), static_cast<column_index_t>(SysRoles::Permissions)),
      Value(isSystem, static_cast<column_index_t>(SysRoles::IsSystemRole)),
      Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(SysRoles::CreatedAt)),
      Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(SysRoles::LastModifiedAt)),
      Value(lastModifiedBy, static_cast<column_index_t>(SysRoles::LastModifiedBy)),
      Value(version, static_cast<column_index_t>(SysRoles::Version)),
      Value(isDeleted, static_cast<column_index_t>(SysRoles::IsDeleted)),
      Value::Null(static_cast<column_index_t>(SysRoles::DeletedAt)),
    };

    const auto result = table->InsertRow(properties, fields);

    std::cout << "Inserted Role " << roleName << std::endl;

    return result;
  }

  Errors::RuntimeStatus SystemCatalog::InsertUserToMasterDb(
    const ExecutionProperties& properties,
    const std::string &username,
    const std::string &passwordHash,
    const int32_t &roleId,
    const bool& isActive,
    const int &version,
    const bool &isDeleted
  ) const{

    auto* table = this->masterDb->OpenTable(CatalogTables::SysUsers);
    const auto currentDate = DataTypes::DateTime::Now();

    const std::string lastModifiedBy = "system";

    const vector<Value> fields = {
      Value(username, static_cast<column_index_t>(SysUsers::UserName)),
      Value(passwordHash, static_cast<column_index_t>(SysUsers::PasswordHash)),
      Value(roleId, static_cast<column_index_t>(SysUsers::RoleId)),
      Value(isActive, static_cast<column_index_t>(SysUsers::IsActive)),
      Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(SysUsers::CreatedAt)),
      Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(SysUsers::LastModifiedAt)),
      Value(lastModifiedBy, static_cast<column_index_t>(SysUsers::LastModifiedBy)),
      Value(version, static_cast<column_index_t>(SysUsers::Version)),
      Value(isDeleted, static_cast<column_index_t>(SysUsers::IsDeleted)),
      Value::Null(static_cast<column_index_t>(SysUsers::DeletedAt)),
    };

    const auto result = table->InsertRow(properties, fields);

    std::cout << "Inserted User " << username << std::endl;

    return result;
  }

  std::vector<Security::Role> SystemCatalog::SelectRoles() const{
   std::vector<const StorageTypes::Row*> rows;

   std::vector<Security::Role> roles;

   auto* table = this->masterDb->OpenTable(CatalogTables::SysRoles);

   table->ClusteredIndexScan(this->baseProperties, &rows);

   for (const auto& row : rows) {
     const auto& data = row->GetData();

     roles.emplace_back(
       data[0]->GetInt(),
       data[1]->GetString(),
       static_cast<Security::Permission>(data[2]->GetInt()),
       data[3]->GetBool()
     );
   }

   return roles;
 }

  std::vector<Security::User> SystemCatalog::SelectUsers() const{
   std::vector<const StorageTypes::Row*> rows;

   std::vector<Security::User> users;

   auto* table = this->masterDb->OpenTable(CatalogTables::SysUsers);

   table->ClusteredIndexScan(this->baseProperties, &rows);

   for (const auto& row : rows) {
     const auto& data = row->GetData();
     users.emplace_back(
       Security::User{
         .id = data[0]->GetInt(),
         .name = data[1]->GetString(),
         .passwordHash = data[2]->GetString(),
         .roleId =  data[3]->GetInt(),
         .isActive = data[4]->GetBool(),
       }
     );
   }

   return users;
 }

 bool SystemCatalog::DatabaseExists(const string &dbName) const{
      using namespace StorageTypes;

      Table* sysDatabases = this->masterDb->OpenTable(CatalogTables::SysDatabases);
      std::vector<const Row*> selectedDatabases;

      auto* columnExpr = new Expressions::ColumnExpression(static_cast<column_index_t>(SysDatabases::Name));
      auto* constantExpr = new Expressions::ConstantExpression(Value(dbName, static_cast<column_index_t>(SysDatabases::Name)));

      const Expressions::BinaryExpression binaryExpr(columnExpr, constantExpr, Expressions::BinaryOperator::Equal);

      sysDatabases->ClusteredIndexScan(this->baseProperties, &selectedDatabases, &binaryExpr);

      return !selectedDatabases.empty();
}

Headers::DatabaseHeader SystemCatalog::SelectDatabase(const std::string &name) const{
  auto* columnExpr = new Expressions::ColumnExpression(static_cast<column_index_t>(SysDatabases::Name));
  auto* constantExpr = new Expressions::ConstantExpression(Value(name, static_cast<column_index_t>(SysDatabases::Name)));

  const Expressions::BinaryExpression binaryExpr(columnExpr, constantExpr, Expressions::BinaryOperator::EqualIgnoreOrdinalCase);

  auto* sysDatabases = this->masterDb->OpenTable(CatalogTables::SysDatabases);
  std::vector<const StorageTypes::Row*> selectedDatabases;

  sysDatabases->ClusteredIndexScan(this->baseProperties, &selectedDatabases, &binaryExpr);

  if (selectedDatabases.empty())
    return {};

  return SystemCatalog::ToDatabaseHeader(selectedDatabases[0]);
}

Headers::DatabaseHeader SystemCatalog::SelectDatabaseById(const int32_t & databaseId) const{
  using namespace StorageTypes;

  Table* sysDatabases = this->masterDb->OpenTable(CatalogTables::SysDatabases);
  std::vector<const Row*> selectedDatabases;

  DataTypes::Indexing::Key key;
  key.InsertKey(DataTypes::Indexing::Key(&databaseId, sizeof(databaseId), DataType::Int));

  sysDatabases->ClusteredIndexSeek(this->baseProperties, &selectedDatabases, key);

  if (selectedDatabases.empty())
    return {};

  return SystemCatalog::ToDatabaseHeader(selectedDatabases[0]);
}

  vector<Headers::SchemaHeader> SystemCatalog::SelectSchemas(const int32_t& databaseId) const{
     auto* sysSchemas = this->masterDb->OpenTable(CatalogTables::SysSchemas);
     std::vector<const StorageTypes::Row*> selectedSchemas;

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&databaseId, sizeof(databaseId), DataType::Int));

    sysSchemas->ClusteredIndexSeek(this->baseProperties, &selectedSchemas, key);

    if (selectedSchemas.empty())
      return {};

    vector<Headers::SchemaHeader> schemas;

    for (const auto& row : selectedSchemas)
      schemas.emplace_back(SystemCatalog::ToSchemaHeader(row));

     return schemas;
  }

  Dictionary<std::string, Headers::SchemaHeader> SystemCatalog::SelectSchemasToDictionary(const int32_t &databaseId) const{
    const auto& schemas = this->SelectSchemas(databaseId);

    Dictionary<string, Headers::SchemaHeader> selectedSchemas;

    for (const auto& schema : schemas)
      selectedSchemas.Add(schema.name, schema);

    return selectedSchemas;
  }

  bool SystemCatalog::SchemaExists(const int32_t &databaseId, const std::string &schema, int* schemaId) const{
    std::vector<const StorageTypes::Row*> selectedSchemas;

    auto* sysSchemas = this->masterDb->OpenTable(CatalogTables::SysSchemas);

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&databaseId, sizeof(databaseId), DataType::Int));

    sysSchemas->ClusteredIndexSeek(this->baseProperties, &selectedSchemas, key);

    for (const auto& row : selectedSchemas) {
      const auto& currentSchemaName = row->GetColumnByIndex(static_cast<column_index_t>(SysSchemas::Name));

      if (Functions::String::Lower(currentSchemaName.GetString())
          == Functions::String::Lower(schema)) {

        if (schemaId != nullptr)
          *schemaId = row->GetColumnByIndex(static_cast<column_index_t>(SysSchemas::SchemaId)).GetInt();

        return true;
      }
    }

    return false;
  }

  vector<Headers::TableHeader> SystemCatalog::SelectTables(const string &dbName) const{
    const auto databaseHeader = this->SelectDatabase(dbName);

    return this->SelectTables(databaseHeader.id);
  }

  std::vector<Headers::TableHeader> SystemCatalog::SelectTables(const int32_t & databaseId) const{
    std::vector<const StorageTypes::Row*> selectedTables;

    auto* sysTablesPtr = this->masterDb->OpenTable(CatalogTables::SysTables);

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&databaseId, sizeof(databaseId), DataType::Int));

    sysTablesPtr->ClusteredIndexSeek(this->baseProperties, &selectedTables, key);

    if (selectedTables.empty())
      return {};

    std::vector<Headers::TableHeader> selectedTableHeaders;
    selectedTableHeaders.reserve(selectedTables.size());

    for (const auto& row : selectedTables)
      selectedTableHeaders.emplace_back(SystemCatalog::ToTableHeader(row));

    ranges::sort(selectedTableHeaders,
    [](const Headers::TableHeader& a, const Headers::TableHeader& b) {
        return a.ordinalPosition < b.ordinalPosition;
      }
    );

    return selectedTableHeaders;
  }

  Headers::TableHeader SystemCatalog::SelectTable(const string &dbName, const string &tableName) const{
    using namespace StorageTypes;

    const auto databaseHeader = this->SelectDatabase(dbName);

    return this->SelectTable(databaseHeader.id, tableName, Constants::DEFAULT_SCHEMA_NAME.data());
  }

  Headers::TableHeader SystemCatalog::SelectTable(
    const int32_t &databaseId,
    const string &tableName,
    const std::string& schema
  ) const{

    int32_t schemaId = -1;
    if (!this->SchemaExists(databaseId, schema, &schemaId) && !schema.empty())
      return {};

    std::vector<const StorageTypes::Row*> selectedTables;
    auto* sysTablesPtr = this->masterDb->OpenTable(CatalogTables::SysTables);

    auto* leftColumnExpr = new Expressions::ColumnExpression(static_cast<column_index_t>(SysTables::SchemaId));
    auto* leftConstantExpr = new Expressions::ConstantExpression(Value(schemaId, static_cast<column_index_t>(SysTables::SchemaId)));

    auto* leftBinaryExpr = new Expressions::BinaryExpression(leftColumnExpr, leftConstantExpr, Expressions::BinaryOperator::Equal);

    auto* rightColumnExpr = new Expressions::ColumnExpression(static_cast<column_index_t>(SysTables::Name));
    auto* rightConstantExpr = new Expressions::ConstantExpression(Value(tableName, static_cast<column_index_t>(SysTables::Name)));

    auto* rightBinaryExpr = new Expressions::BinaryExpression(rightColumnExpr, rightConstantExpr, Expressions::BinaryOperator::EqualIgnoreOrdinalCase);

    const auto* logicalExpr = new Expressions::LogicalExpression(leftBinaryExpr, rightBinaryExpr, Expressions::LogicalType::And);

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&databaseId, sizeof(databaseId), DataType::Int));

    sysTablesPtr->ClusteredIndexSeek(this->baseProperties, &selectedTables, key, logicalExpr);

    if (selectedTables.empty())
      return {};

    return SystemCatalog::ToTableHeader(selectedTables[0]);
  }

  vector<Headers::ConstraintsHeader> SystemCatalog::SelectConstraints(const int32_t & tableId) const{
    std::vector<const StorageTypes::Row*> selectedConstraints;
    auto* constraintsTable = this->masterDb->OpenTable(CatalogTables::SysConstraints);

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int));

    constraintsTable->ClusteredIndexSeek(this->baseProperties, &selectedConstraints, key);

    if (selectedConstraints.empty())
      return {};

    vector<Headers::ConstraintsHeader> selectedConstraintsHeader;
    selectedConstraintsHeader.reserve(selectedConstraints.size());

    for (const auto& row : selectedConstraints) {
      const auto& data = row->GetData();

      auto constraintColumns = this->SelectConstraintColumnsByConstraintId(data[0]->GetInt());

      const auto indexId =(data[static_cast<column_index_t>(SysConstraints::IndexId)]->GetRawData() == nullptr)
              ? -1
              : data[static_cast<column_index_t>(SysConstraints::IndexId)]->GetInt();

      Headers::IndexHeader index;
      if(indexId != -1)
        index = this->SelectIndexById(indexId);

      selectedConstraintsHeader.emplace_back(SystemCatalog::ToConstraintsHeader(row, constraintColumns, index));
    }

    return selectedConstraintsHeader;
  }

  vector<Headers::ColumnHeader> SystemCatalog::SelectColumns(const int32_t& tableId) const{
    std::vector<const StorageTypes::Row*> selectedColumns;
    auto* sysColumns = this->masterDb->OpenTable(CatalogTables::SysColumns);

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int));

    sysColumns->ClusteredIndexSeek(this->baseProperties, &selectedColumns, key);

    if (selectedColumns.empty())
      return {};

    vector<Headers::ColumnHeader> selectedColumnHeaders;
    selectedColumnHeaders.reserve(selectedColumns.size());

    for (const auto& row : selectedColumns)
      selectedColumnHeaders.emplace_back(SystemCatalog::ToColumnHeader(row));

    ranges::sort(selectedColumnHeaders,
        [](const Headers::ColumnHeader& a, const Headers::ColumnHeader& b) {
            return a.ordinalPosition < b.ordinalPosition;
        }
    );

     return selectedColumnHeaders;
  }

  Dictionary<string, Headers::ColumnHeader> SystemCatalog::SelectColumnsToDictionary(const int32_t& tableId) const{
      const auto columns = this->SelectColumns(tableId);

      Dictionary<string, Headers::ColumnHeader> selectedColumns;

      for (const auto& column : columns)
        selectedColumns.Add(Functions::String::Lower(column.name), column);

      return selectedColumns;
    }

  vector<Headers::IndexHeader> SystemCatalog::SelectIndexes(const int32_t& tableId) const{
      using namespace StorageTypes;

      Table* sysIndexes = this->masterDb->OpenTable(CatalogTables::SysIndexes);
      std::vector<const Row*> selectedIndexes;

      DataTypes::Indexing::Key key;
      key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int));

      sysIndexes->ClusteredIndexSeek(this->baseProperties, &selectedIndexes, key);

      vector<Headers::IndexHeader> selectedIndexHeaders;

      for (const auto& row : selectedIndexes)
        selectedIndexHeaders.emplace_back(SystemCatalog::ToIndexHeader(row));

      //get the clustered first
      ranges::sort(selectedIndexHeaders,
      [](const Headers::IndexHeader& a, const Headers::IndexHeader& b) {
        return a.isClustered > b.isClustered;
      });

      return selectedIndexHeaders;
  }

  Headers::IndexHeader SystemCatalog::SelectIndexById(const int32_t & indexId) const{
    using namespace StorageTypes;

    Table* sysIndexes = this->masterDb->OpenTable(CatalogTables::SysIndexes);
    std::vector<const Row*> selectedIndexes;

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&indexId, sizeof(indexId), DataType::Int));

    sysIndexes->ClusteredIndexSeek(this->baseProperties, &selectedIndexes, key);

    if(selectedIndexes.empty())
      return {};

    auto indexColumns = this->SelectIndexColumnsByIndexId(indexId);

    vector<Headers::IndexHeader> selectedIndexHeaders;

    auto header = SystemCatalog::ToIndexHeader(selectedIndexes.at(0));
    header.columns = std::move(indexColumns);

    return header;
  }

    vector<Headers::IndexColumnsHeader> SystemCatalog::SelectIndexColumnsByIndexId(const int32_t & indexId) const{
    using namespace StorageTypes;

    Table* sysIndexes = this->masterDb->OpenTable(CatalogTables::SysIndexColumns);
    std::vector<const Row*> rows;

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&indexId, sizeof(indexId), DataType::Int));

    sysIndexes->ClusteredIndexSeek(this->baseProperties, &rows, key);

    if(rows.empty())
      return {};

    vector<Headers::IndexColumnsHeader> indexColumns;

    for (const auto& row : rows) {
      const auto& data = row->GetData();

      indexColumns.emplace_back(SystemCatalog::ToIndexColumnsHeader(row));
    }

    //get them sorted by ordinal position
    ranges::sort(indexColumns,
    [](const Headers::IndexColumnsHeader& a, const Headers::IndexColumnsHeader& b) {
      return a.ordinalPosition < b.ordinalPosition;
    });

    return indexColumns;
  }

  Dictionary<int32_t, Headers::IndexColumnsHeader> SystemCatalog::SelectIndexColumnsByIndexIdToDictionary(const int32_t &indexId) const{
    const auto indexColumns = this->SelectIndexColumnsByIndexId(indexId);

    Dictionary<int32_t, Headers::IndexColumnsHeader> indexColumnsDict;

    for (const auto& indexColumn : indexColumns)
      indexColumnsDict.Add(indexColumn.columnId, indexColumn);

    return indexColumnsDict;
  }

  vector<Headers::IdentityColumnsHeader> SystemCatalog::SelectIdentityColumnsByTableId(const int32_t & tableId) const{
      using namespace StorageTypes;

      Table* table = this->masterDb->OpenTable(CatalogTables::SysIdentityColumns);
      std::vector<const Row*> rows;

      DataTypes::Indexing::Key key;
      key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int));

      table->ClusteredIndexSeek(this->baseProperties, &rows, key);

      if(rows.empty())
        return {};

      vector<Headers::IdentityColumnsHeader> columns;

      for (const auto& row : rows)
        columns.emplace_back(SystemCatalog::ToIdentityColumnsHeader(row));

      //get them sorted by ordinal position
      ranges::sort(columns,
      [](const Headers::IdentityColumnsHeader& a, const Headers::IdentityColumnsHeader& b) {
        return a.columnId > b.columnId;
      });

      return columns;
  }

  Dictionary<int32_t , Headers::IdentityColumnsHeader> SystemCatalog::SelectIdentityColumnsByTableIdToDictionary(const int32_t & tableId) const{
    const auto columns = this->SelectIdentityColumnsByTableId(tableId);

    Dictionary<int32_t, Headers::IdentityColumnsHeader> dict;

    for(const auto& column : columns)
        dict.Add(column.columnId, column);

    return dict;
  }

  vector<Headers::ConstraintsColumnsHeader> SystemCatalog::SelectConstraintColumnsByConstraintId(const int32_t & constraintId) const{
    using namespace StorageTypes;

    Table* sysIndexes = this->masterDb->OpenTable(CatalogTables::SysConstraintColumns);
    std::vector<const Row*> rows;

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&constraintId, sizeof(constraintId), DataType::Int));

    sysIndexes->ClusteredIndexSeek(this->baseProperties, &rows, key);

    if(rows.empty())
      return {};

    vector<Headers::ConstraintsColumnsHeader> constraintColumns;

    for(const auto& row : rows)
      constraintColumns.emplace_back(SystemCatalog::ToConstraintsColumnsHeader(row));

    ranges::sort(constraintColumns,
      [](const Headers::ConstraintsColumnsHeader& a, const Headers::ConstraintsColumnsHeader& b) {
          return a.ordinalPosition < b.ordinalPosition;
      }
    );

    return constraintColumns;
  }

  Dictionary<int32_t, Headers::ConstraintsColumnsHeader> SystemCatalog::SelectConstraintColumnsByConstraintIdToDictionary(const int32_t &constraintId) const{
    const auto columns = this->SelectConstraintColumnsByConstraintId(constraintId);

    Dictionary<int32_t, Headers::ConstraintsColumnsHeader> constraintColumns;

    for (const auto& constraint: columns)
      constraintColumns.Add(constraint.columnId, constraint);

    return constraintColumns;
  }

  Headers::DefaultValuesHeader SystemCatalog::SelectDefaultValueByColumnId(const int32_t &columnId) const{
    using namespace StorageTypes;

    Table* sysValues = this->masterDb->OpenTable(CatalogTables::SysDefaultValues);
    std::vector<const Row*> rows;

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int));

    sysValues->ClusteredIndexSeek(this->baseProperties, &rows, key);

    if(rows.empty())
      return {};

    return SystemCatalog::ToDefaultValuesHeader(rows.at(0));
  }

  Headers::TableStatistics SystemCatalog::SelectTableStatisticsById(const int32_t &tableId) const{
    auto* sysIndexes = this->masterDb->OpenTable(CatalogTables::SysTableStats);
    std::vector<const StorageTypes::Row*> selectedStats;

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int));

    sysIndexes->ClusteredIndexSeek(this->baseProperties, &selectedStats, key);

    if (selectedStats.empty())
      return {};

    return SystemCatalog::ToTableStatistics(selectedStats.front());
  }

  Headers::ColumnStatistics SystemCatalog::SelectColumnStatisticsById(
    const int32_t& columnId,
    const DataType& columnType
  ) const{
    auto* sysColumnStats = this->masterDb->OpenTable(CatalogTables::SysColumnStats);
    std::vector<const StorageTypes::Row*> selectedStats;

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int));

    sysColumnStats->ClusteredIndexSeek(this->baseProperties, &selectedStats, key);

    if (selectedStats.empty())
      return {};

    return SystemCatalog::ToColumnStatistics(selectedStats.front(), columnType);
  }

  std::vector<Headers::ColumnHistograms> SystemCatalog::SelectColumnHistogramsByColumnId(
    const int32_t &columnId,
    const DataType& columnType
  ) const {

    std::vector<Headers::ColumnHistograms> result;
    result.reserve(NUMBER_OF_HISTOGRAM_BUCKETS);

    auto* table = this->masterDb->OpenTable(CatalogTables::SysColumnHistograms);

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int));

    std::vector<const StorageTypes::Row*> rows;
    table->ClusteredIndexSeek(this->baseProperties, &rows, key);

    for (const auto& row : rows)
      result.emplace_back(SystemCatalog::ToColumnHistograms(row, columnType));

    return result;
  }

  std::vector<Headers::IndexStatistics> SystemCatalog::SelectIndexStatisticsByTableId(const int32_t &tableId) const {
   std::vector<Headers::IndexStatistics> result;

   auto* table = this->masterDb->OpenTable(CatalogTables::SysIndexStats);

   DataTypes::Indexing::Key key;
   key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int));

   std::vector<const StorageTypes::Row*> rows;
   table->ClusteredIndexSeek(this->baseProperties, &rows, key);

   for (const auto& row : rows)
     result.emplace_back(SystemCatalog::ToIndexStatistics(row));

   return result;
 }

  void SystemCatalog::UpdateIdentityByColumnId(const int32_t & tableId, const int32_t& columnId, const int64_t& lastValue)const{
    auto* table = this->masterDb->OpenTable(CatalogTables::SysIdentityColumns);

    const std::vector<Value> updates{
      Value(lastValue, static_cast<column_index_t>(SysIdentityColumns::LastValue))
    };

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int));
    key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int));

    const auto _ = table->ClusteredIndexSeekUpdate(this->baseProperties, key, updates);
  }

void SystemCatalog::UpdateTableStatisticsById(
    const int32_t &tableId,
    const int64_t& rowCount,
    const int32_t& rowSize,
    const int32_t& pageCount
  ) const{

    const std::vector updates = {
      Value(rowCount, static_cast<column_index_t>(SysTableStats::RowCount)),
      Value(rowSize, static_cast<column_index_t>(SysTableStats::AvgRowSize)),
      Value(pageCount, static_cast<column_index_t>(SysTableStats::PageCount)),
      Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(SysTableStats::LastUpdatedAt))
    };

    auto* table = this->masterDb->OpenTable(CatalogTables::SysTableStats);

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int));

    const auto _ = table->ClusteredIndexSeekUpdate(this->baseProperties, key, updates);
  }

  void SystemCatalog::UpdateColumnStatisticsById(
    const int32_t &columnId,
    const int64_t& distinctCount,
    const int64_t& nullCount,
    const Value& min,
    const Value& max
  ) const{
    const std::vector updates = {
      Value(distinctCount, static_cast<column_index_t>(SysColumnStats::DistinctCount)),
      Value(nullCount, static_cast<column_index_t>(SysColumnStats::NullCount)),
      Value(std::string(reinterpret_cast<const char*>(min.GetRawData()), min.GetSize()), static_cast<column_index_t>(SysColumnStats::MinimumValue)),
      Value(std::string(reinterpret_cast<const char*>(max.GetRawData()), max.GetSize()), static_cast<column_index_t>(SysColumnStats::MaximumValue))
    };

    auto* table = this->masterDb->OpenTable(CatalogTables::SysColumnStats);

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int));

    const auto _ = table->ClusteredIndexSeekUpdate(this->baseProperties, key, updates);
  }

  void SystemCatalog::UpdateIndexStatisticsById(
    const int32_t &tableId,
    const int32_t &indexId,
    const int64_t &leafPages,
    const int8_t &depth,
    const DataTypes::Decimal &averageFragmentation
  ) const {
   const std::vector updates = {
     Value(leafPages, static_cast<column_index_t>(SysIndexStats::LeafPages)),
     Value(depth, static_cast<column_index_t>(SysIndexStats::Depth)),
     Value(averageFragmentation, static_cast<column_index_t>(SysIndexStats::AverageFragmentation)),
     Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(SysIndexStats::LastUpdated)),
   };

   auto* table = this->masterDb->OpenTable(CatalogTables::SysIndexStats);

   DataTypes::Indexing::Key key;
   key.InsertKey(DataTypes::Indexing::Key(&tableId, sizeof(tableId), DataType::Int));
   key.InsertKey(DataTypes::Indexing::Key(&indexId, sizeof(indexId), DataType::Int));

   const auto _ = table->ClusteredIndexSeekUpdate(this->baseProperties, key, updates);
 }

  Errors::RuntimeStatus SystemCatalog::UpdateColumnById(const int32_t &columnId, const std::vector<Value> &updates) const{
    using namespace StorageTypes;

    Table* table = this->masterDb->OpenTable(CatalogTables::SysColumns);

    DataTypes::Indexing::Key key;
    key.InsertKey(DataTypes::Indexing::Key(&columnId, sizeof(columnId), DataType::Int));

    return table->ClusteredIndexSeekUpdate(this->baseProperties, key, updates);
  }

  Errors::RuntimeStatus SystemCatalog::UpdateUserById(
    const std::string& username,
    const int32_t &userId,
    const int32_t &roleId
  ) const {
   auto* table = this->masterDb->OpenTable(CatalogTables::SysUsers);

   const auto currentDate = DataTypes::DateTime::Now();

   const std::vector<Value> updates = {
     Value(roleId, static_cast<column_index_t>(SysUsers::RoleId)),
     Value(currentDate, static_cast<column_index_t>(SysUsers::LastModifiedAt)),
     Value(username, static_cast<column_index_t>(SysUsers::LastModifiedBy))
   };

   DataTypes::Indexing::Key key;
   key.InsertKey(DataTypes::Indexing::Key(&userId, sizeof(userId), DataType::Int));

   return table->ClusteredIndexSeekUpdate(this->baseProperties, key, updates);
 }
}