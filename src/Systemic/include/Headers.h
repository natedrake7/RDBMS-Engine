#pragma once
#include "Constants.h"
#include <string>
#include "DataTypes/DateTime.h"
#include <vector>
#include <ostream>
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
    std::string lastModifiedBy;
    Int version;
    bool isDeleted;
    DataTypes::DateTime deletedAt;
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
  };

  struct IndexHeader {
    Int tableId;
    Int id = -1;
    std::string name;
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
    std::string name;
    ConstraintType type;
    bool isDisabled;
    Int indexId = INVALID_INDEX_ID;
    IndexHeader index;
    std::vector<ConstraintsColumnsHeader> columns;

    AuditInformation additionalInfo;
  };

  struct DefaultValuesHeader {
    Int columnId = INVALID_COLUMN_ID;
    std::string value;
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
    std::string name;
    UnsignedTinyInt dataType;
    Int recordSize;
    TinyInt precision = INVALID_DECIMAL_PRECISION;
    TinyInt scale = INVALID_DECIMAL_SCALE;
    bool isNullable;
    SmallInt ordinalPosition;
    bool isSystem;

    IdentityColumnsHeader identity;
    DefaultValuesHeader defaultValue;
    ColumnStatistics statistics;
    AuditInformation additionalInfo;
  };

  struct TableHeader {
    Int databaseId;
    Int id = INVALID_TABLE_ID;
    Int schemaId;
    std::string name;
    SmallInt ordinalPosition;
    bool isSystem;

    AuditInformation additionalInfo;

    TableStatistics statistics;

    std::vector<ColumnHeader> columns;
    std::vector<ConstraintsHeader> constraints;

    std::vector<IdentityColumnsHeader> identity;
  };

  struct SchemaHeader {
    Int id = -1;
    Int databaseId;
    std::string name;
    AuditInformation additionalInfo;
  };

  struct DatabaseHeader {
    Int id = -1;
    std::string name;
    std::string filepath;
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

  struct RowIdentifier {
    page_id_t pageId;
    Int indexId;

    RowIdentifier() {
      this->pageId = INVALID_PAGE_ID;
      this->indexId = INVALID_PAGE_INDEX_ID;
    }

    RowIdentifier(const Int pageId, const Int indexId) {
      this->pageId = pageId;
      this->indexId = indexId;
    }

    RowIdentifier(const RowIdentifier& rowId) {
      this->pageId = rowId.pageId;
      this->indexId = rowId.indexId;
    }

    ~RowIdentifier() = default;

    [[nodiscard]] inline bool IsInvalid() const{
      return this->pageId == INVALID_PAGE_ID && this->indexId == INVALID_PAGE_INDEX_ID;
    }
  };

  inline std::ostream& operator<<(std::ostream& os, const RowIdentifier& rowId) {
      os << "(" << rowId.pageId << "," << rowId.indexId << ")";
      return os;
  }

  inline bool operator==(const RowIdentifier& lhs, const RowIdentifier& rhs) {
    return lhs.pageId == rhs.pageId && lhs.indexId == rhs.indexId;
  }

  inline bool operator!=(const RowIdentifier& lhs, const RowIdentifier& rhs) {
    return !(lhs == rhs);
  }

  inline bool operator>(const RowIdentifier& lhs, const RowIdentifier& rhs) {
    if (lhs.pageId > rhs.pageId)
      return true;

    return lhs.pageId == rhs.pageId && lhs.indexId > rhs.indexId;
  }

  inline bool operator<(const RowIdentifier& lhs, const RowIdentifier& rhs) {
    if (lhs.pageId < rhs.pageId)
      return true;

    return lhs.pageId == rhs.pageId && lhs.indexId < rhs.indexId;
  }

  inline bool operator>=(const RowIdentifier& lhs, const RowIdentifier& rhs) {
    return !(lhs < rhs);
  }

  inline bool operator<=(const RowIdentifier& lhs, const RowIdentifier& rhs) {
    return !(rhs > lhs);
  }

}