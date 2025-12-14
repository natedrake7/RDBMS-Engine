#pragma once
#include "Constants.h"
#include <string>
#include "DataTypes/DateTime.h"
#include <vector>
#include <ostream>
#include "DataTypes/Value.h"

namespace Headers {
  enum ConstraintType: uint8_t {
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
    int32_t version;
    bool isDeleted;
    DataTypes::DateTime deletedAt;
  };

  struct IndexColumnsHeader{
    int32_t indexId = INVALID_INDEX_ID;
    int32_t columnId;
    int16_t ordinalPosition;
    bool isIncluded;

    AuditInformation additionalInfo;
  };

  struct IdentityColumnsHeader{
    int32_t tableId = INVALID_TABLE_ID;
    int32_t columnId = INVALID_COLUMN_ID;
    int32_t seedValue;
    int32_t increment;
    int64_t lastValue;

    bool isCached;
    int32_t cacheBlock;
    AuditInformation additionalInfo;
  };

  struct IndexHeader {
    int32_t tableId;
    int32_t id = -1;
    std::string name;
    bool isClustered;
    bool isDisabled;
    AuditInformation additionalInfo;

    std::vector<IndexColumnsHeader> columns;
    IdentityColumnsHeader identity;
  };

  struct ConstraintsColumnsHeader{
    int32_t constraintId = INVALID_CONSTRAINT_ID;
    int32_t columnId;
    int32_t ordinalPosition;
    AuditInformation additionalInfo;
  };

  struct ConstraintsHeader{
    int32_t tableId;
    int32_t constraintId = INVALID_CONSTRAINT_ID;
    std::string name;
    ConstraintType type;
    bool isDisabled;
    int32_t indexId = INVALID_INDEX_ID;
    IndexHeader index;
    vector<ConstraintsColumnsHeader> columns;

    AuditInformation additionalInfo;
  };

  struct DefaultValuesHeader {
    int32_t columnId = INVALID_COLUMN_ID;
    std::string value;
    //add size here

    AuditInformation additionalInfo;
  };

  struct TableStatistics {
    int32_t tableId ;

    int64_t rowCount;
    int32_t averageRowSize;
    int32_t pageCount;

    DataTypes::DateTime lastModified;

    TableStatistics() {
      this->tableId = INVALID_TABLE_ID;
      this->rowCount = 0;
      this->averageRowSize = 0;
      this->pageCount = 0;
      this->lastModified = DataTypes::DateTime::Now();
    }

    explicit TableStatistics(const int32_t& tableId)
      : tableId(tableId),rowCount(0),averageRowSize(0), pageCount(0), lastModified(DataTypes::DateTime::Now()) {}

    TableStatistics(
      const int32_t& tableId,
      const int64_t& rowCount,
      const int32_t& averageRowSize,
      const int32_t& pageCount,
      const DataTypes::DateTime& lastModified
    ) : tableId(tableId),
        rowCount(rowCount),
        averageRowSize(averageRowSize),
        pageCount(pageCount),
        lastModified(lastModified) {}
  };

  struct ColumnStatistics {
    int32_t columnId;

    int64_t distinctCount;
    Value min;
    Value max;
    int64_t nullCount;
  };

  struct ColumnHistograms {
    int32_t columnId;
    int32_t histogramId;

    Value rangeStart;
    Value rangeEnd;

    int32_t rowCount;
    int32_t distinctCount;
  };

  struct IndexStatistics {
    int32_t indexId;
    int32_t leafPages;
    int8_t depth;
    DataTypes::Decimal averageFragmentation;
    DataTypes::DateTime lastUpdated;
  };

  struct ColumnHeader {
    int32_t tableId;
    int32_t id = INVALID_COLUMN_ID;
    std::string name;
    uint8_t dataType;
    int32_t recordSize;
    int8_t precision = INVALID_DECIMAL_PRECISION;
    int8_t scale = INVALID_DECIMAL_SCALE;
    bool isNullable;
    int16_t ordinalPosition;
    bool isSystem;

    IdentityColumnsHeader identity;
    DefaultValuesHeader defaultValue;
    ColumnStatistics statistics;
    AuditInformation additionalInfo;
  };

  struct TableHeader {
    int32_t databaseId;
    int32_t id = INVALID_TABLE_ID;
    int32_t schemaId;
    std::string name;
    int16_t ordinalPosition;
    bool isSystem;

    AuditInformation additionalInfo;

    TableStatistics statistics;

    vector<ColumnHeader> columns;
    vector<ConstraintsHeader> constraints;

    vector<IdentityColumnsHeader> identity;
  };

  struct SchemaHeader {
    int32_t id = -1;
    int32_t databaseId;
    std::string name;
    AuditInformation additionalInfo;
  };

  struct DatabaseHeader {
    int32_t id = -1;
    std::string name;
    std::string filepath;
    bool isSystem;
    AuditInformation additionalInfo;

    std::vector<TableHeader> tables;
    std::vector<SchemaHeader> schemas;
  };

  struct sysColumn {
    string name;
    string type;
    int32_t id;
    int size = 0;
    int _default = 0;
    bool nullable = false;
    bool hasIdentity = false;
  };

  struct sysTable {
    string name;
    int32_t id;
    bool hasIdentity;
    std::vector<sysColumn> columns;
    std::vector<string> primaryKey;
  };

  struct Index{
    vector<uint8_t> columns;

    explicit Index(vector<uint8_t>& columns)
      : columns(std::move(columns)) {}

    Index() = default;
  };

  struct RowIdentifier {
    uint32_t pageId = INVALID_PAGE_ID;
    int32_t indexId = INVALID_PAGE_INDEX_ID;

    RowIdentifier() {
      this->pageId = INVALID_PAGE_ID;
      this->indexId = INVALID_PAGE_INDEX_ID;
    }

    RowIdentifier(const uint32_t& pageId, const int32_t& indexId) {
      this->pageId = pageId;
      this->indexId = indexId;
    }

    RowIdentifier(const RowIdentifier& rowId) {
      this->pageId = rowId.pageId;
      this->indexId = rowId.indexId;
    }

    ~RowIdentifier() = default;
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