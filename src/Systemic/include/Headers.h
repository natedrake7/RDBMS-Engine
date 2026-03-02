#pragma once
#include "Constants.h"
#include "DataTypes/DateTime.h"
#include <string>
#include <vector>

#include "DataTypes/Decimal.h"
#include "DataTypes/Value.h"

namespace Headers {
  enum ConstraintType: UnsignedTinyInt {
    PrimaryKey = 0,
    ForeignKey = 1,
    Unique = 2,
    IndexKey = 3,
    Check = 4,
    NotNull = 5
  };

    struct AuditInformation{
        DataTypes::DateTime createdAt;
        DataTypes::DateTime lastModified;
        DataTypes::String lastModifiedBy;
        Int version;
        bool isDeleted;
        DataTypes::DateTime deletedAt;

        AuditInformation() {
            this->createdAt = DataTypes::DateTime::Now();
            this->lastModified = DataTypes::DateTime::Now();
            this->version = 0;
            this->isDeleted = false;
            this->deletedAt = DataTypes::DateTime::Now();
        }

        AuditInformation(
            const DataTypes::DateTime& createdAt,
            const DataTypes::DateTime& lastModified,
            const DataTypes::String& lastModifiedBy,
            const Int version,
            const bool isDeleted,
            const DataTypes::DateTime& deletedAt
        ) : createdAt(createdAt),
            lastModified(lastModified),
            lastModifiedBy(lastModifiedBy),
            version(version),
            isDeleted(isDeleted),
            deletedAt(deletedAt){}

        // Partial constructor — for headers that only store version/isDeleted/deletedAt
        AuditInformation(
            const Int version,
            const bool isDeleted,
            const DataTypes::DateTime& deletedAt
        ) : version(version),
            isDeleted(isDeleted),
            deletedAt(deletedAt){
            this->createdAt = DataTypes::DateTime::Now();
            this->lastModified = DataTypes::DateTime::Now();
        }
    };

  struct IndexColumnsHeader{
    Int indexId = INVALID_INDEX_ID;
    Int columnId;
    SmallInt ordinalPosition;
    bool isIncluded;

    AuditInformation additionalInfo;
  };

    struct IdentityColumnsHeader{
        Int tableId = INVALID_TABLE_ID;
        Int columnId = INVALID_COLUMN_ID;
        Int seedValue;
        Int increment;
        BigInt lastValue;

        bool isCached;
        Int cacheBlock;
        AuditInformation additionalInfo;

        IdentityColumnsHeader() {
            this->seedValue = 0;
            this->increment = 1;
            this->lastValue = 0;
            this->isCached = false;
            this->cacheBlock = 0;
        }

        IdentityColumnsHeader(
            const Int tableId,
            const Int columnId,
            const Int seedValue,
            const Int increment,
            const BigInt lastValue,
            const bool isCached,
            const Int cacheBlock
        ):
        tableId(tableId),
        columnId(columnId),
        seedValue(seedValue),
        increment(increment),
        lastValue(lastValue),
        isCached(isCached),
        cacheBlock(cacheBlock){}

        IdentityColumnsHeader(
            const Int tableId,
            const Int columnId,
            const Int seedValue,
            const Int increment,
            const BigInt lastValue,
            const bool isCached,
            const Int cacheBlock,
            const AuditInformation& additionalInfo
        ):
        tableId(tableId),
        columnId(columnId),
        seedValue(seedValue),
        increment(increment),
        lastValue(lastValue),
        isCached(isCached),
        cacheBlock(cacheBlock),
        additionalInfo(additionalInfo){}
    };

  struct IndexHeader {
    Int tableId;
    Int id = -1;
    DataTypes::String name;
    bool isClustered;
    bool isDisabled;
    AuditInformation additionalInfo;

    std::vector<IndexColumnsHeader> columns;
    IdentityColumnsHeader identity;
  };

  struct ConstraintsColumnsHeader{
    Int constraintId = INVALID_CONSTRAINT_ID;
    Int columnId;
    Int ordinalPosition;
    AuditInformation additionalInfo;
  };

  struct ConstraintsHeader{
    Int tableId;
    Int constraintId = INVALID_CONSTRAINT_ID;
    DataTypes::String name;
    ConstraintType type;
    bool isDisabled;
    Int indexId = INVALID_INDEX_ID;
    IndexHeader index;
    std::vector<ConstraintsColumnsHeader> columns;

    AuditInformation additionalInfo;
  };

  struct DefaultValuesHeader {
    Int columnId = INVALID_COLUMN_ID;
    DataTypes::String value;
    //add size here

    AuditInformation additionalInfo;
  };

  struct TableStatistics {
    Int tableId ;

    BigInt rowCount;
    Int averageRowSize;
    Int pageCount;

    DataTypes::DateTime lastModified;

    TableStatistics() {
      this->tableId = INVALID_TABLE_ID;
      this->rowCount = 0;
      this->averageRowSize = 0;
      this->pageCount = 0;
      this->lastModified = DataTypes::DateTime::Now();
    }

    explicit TableStatistics(const Int tableId)
      : tableId(tableId),rowCount(0),averageRowSize(0), pageCount(0), lastModified(DataTypes::DateTime::Now()) {}

    TableStatistics(
      const Int tableId,
      const BigInt& rowCount,
      const Int averageRowSize,
      const Int pageCount,
      const DataTypes::DateTime& lastModified
    ) : tableId(tableId),
        rowCount(rowCount),
        averageRowSize(averageRowSize),
        pageCount(pageCount),
        lastModified(lastModified) {}
  };

  struct ColumnStatistics {
    Int columnId;

    BigInt distinctCount;
    Value min;
    Value max;
    BigInt nullCount;
  };

  struct ColumnHistograms {
    Int columnId;
    Int histogramId;

    Value rangeStart;
    Value rangeEnd;

    Int rowCount;
    Int distinctCount;

    ColumnHistograms(
      const Int columnId,
      const Value& rangeStart,
      const Value& rangeEnd,
      const Int rowCount,
      const Int distinctCount
    )
      : columnId(columnId),
        rangeStart(rangeStart),
        rangeEnd(rangeEnd),
        rowCount(rowCount),
        distinctCount(distinctCount)
    {
      this->histogramId = INVALID_HISTOGRAM_ID;
    }

    ColumnHistograms(
      const Int columnId,
      const Int histogramId,
      const Value& rangeStart,
      const Value& rangeEnd,
      const Int rowCount,
      const Int distinctCount
    )
      : columnId(columnId),
        histogramId(histogramId),
        rangeStart(rangeStart),
        rangeEnd(rangeEnd),
        rowCount(rowCount),
        distinctCount(distinctCount){}
  };

  struct IndexStatistics {
    Int tableId;
    Int indexId;
    Int leafPages;
    TinyInt depth;
    DataTypes::Decimal averageFragmentation;
    DataTypes::DateTime lastUpdated;

    IndexStatistics() {
      this->tableId = INVALID_TABLE_ID;
      this->indexId = INVALID_INDEX_ID;
      this->leafPages = 0;
      this->depth = 0;
      this->averageFragmentation = DataTypes::Decimal(0);
      this->lastUpdated = DataTypes::DateTime::Now();
    }

    IndexStatistics(
      const Int tableId,
      const Int indexId
    ) : IndexStatistics() {
      this->tableId = tableId;
      this->indexId = indexId;
    }

    IndexStatistics(
      const Int tableId,
      const Int indexId,
      const Int leafPages,
      const TinyInt& depth,
      const DataTypes::Decimal& averageFragmentation,
      const DataTypes::DateTime& lastUpdated
    )
      : tableId(tableId),
        indexId(indexId),
        leafPages(leafPages),
        depth(depth),
        averageFragmentation(averageFragmentation),
        lastUpdated(lastUpdated) {}

    void Reset() {
      this->depth = 0;
      this->leafPages = 0;
      this->averageFragmentation = DataTypes::Decimal(0);
    }
  };

  struct ColumnHeader {
    Int tableId;
    Int id = INVALID_COLUMN_ID;
    DataTypes::String name;
    UnsignedTinyInt dataType;
    Int recordSize;
    TinyInt precision = INVALID_DECIMAL_PRECISION;
    TinyInt scale = INVALID_DECIMAL_SCALE;
    bool isNullable;
    SmallInt ordinalPosition;
    bool isSystem;

    // IdentityColumnsHeader identity;
    // DefaultValuesHeader defaultValue;
    // ColumnStatistics statistics;
    AuditInformation additionalInfo;
  };

    struct TableHeader {
        Int databaseId;
        Int id;
        Int schemaId;
        DataTypes::String name;
        SmallInt ordinalPosition;
        bool isSystem;

        AuditInformation additionalInfo;

        TableStatistics statistics;

        std::vector<ColumnHeader> columns;
        std::vector<ConstraintsHeader> constraints;

        std::vector<IdentityColumnsHeader> identity;

        TableHeader()
            : databaseId(INVALID_DATABASE_ID), id(INVALID_TABLE_ID), schemaId(INVALID_SCHEMA_ID), ordinalPosition(0), isSystem(false){}
    };

    struct SchemaHeader {
        Int id = -1;
        Int databaseId;
        DataTypes::String name;
        AuditInformation additionalInfo;

        SchemaHeader(){
            this->id = INVALID_SCHEMA_ID;
            this->databaseId = INVALID_DATABASE_ID;
        }

        SchemaHeader(
            const Int id,
            const Int databaseId,
            const DataTypes::String& name
        ) : id(id), databaseId(databaseId), name(name){}

    };

  struct DatabaseHeader {
    Int id = -1;
    DataTypes::String name;
    DataTypes::String filepath;
    bool isSystem;
    AuditInformation additionalInfo;

    std::vector<TableHeader> tables;
    std::vector<SchemaHeader> schemas;
  };

  struct sysColumn {
    std::string name;
    std::string type;
    Int id;
    int size = 0;
    int _default = 0;
    bool nullable = false;
    bool hasIdentity = false;
  };

  struct sysTable {
    std::string name;
    Int id;
    bool hasIdentity;
    std::vector<sysColumn> columns;
    std::vector<std::string> primaryKey;
  };

  struct Index{
    std::vector<UnsignedTinyInt> columns;

    explicit Index(std::vector<UnsignedTinyInt>& columns)
      : columns(std::move(columns)) {}

    Index() = default;
  };
}
