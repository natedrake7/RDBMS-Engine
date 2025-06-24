#pragma once
#include <string>
#include "../DateTime/DateTime.h"
#include <vector>
#include <ostream>

namespace Headers {
  enum ConstraintType: uint8_t {
    PrimaryKey = 0,
    ForeignKey = 1,
    Unique = 2,
    IndexKey = 3,
    Check = 4,
    NotNull = 5
  };

  struct AdditionalInformation{
    DataTypes::DateTime createdAt;
    DataTypes::DateTime lastModified;
    std::string lastModifiedBy;
    int32_t version;
    bool isDeleted;
    DataTypes::DateTime deletedAt;
  };

  struct IndexColumnsHeader{
    int32_t indexId = -1;
    int32_t columnId;
    int16_t ordinalPosition;
    bool isIncluded;
    AdditionalInformation additionalInfo;
  };

  struct IdentityColumnsHeader{
    int32_t tableId = -1;
    int32_t columnId = -1;
    int32_t seedValue;
    int32_t increment;
    int32_t lastValue;

    bool isCached;
    int32_t cacheBlock;
    AdditionalInformation additionalInfo;
  };

  struct IndexHeader {
    int32_t id = -1;
    int32_t tableId;
    std::string name;
    bool isClustered;
    bool isDisabled;
    AdditionalInformation additionalInfo;

    std::vector<IndexColumnsHeader> columns;
    IdentityColumnsHeader identity;
  };

  struct ConstraintsColumnsHeader{
    int32_t constraintId = -1;
    int32_t columnId;
    int32_t ordinalPosition;
    AdditionalInformation additionalInfo;
  };

  struct ConstraintsHeader{
    int32_t constraintId = -1;
    int32_t tableId;
    std::string name;
    ConstraintType type;
    bool isDisabled;
    int32_t indexId = -1;
    IndexHeader index;
    vector<ConstraintsColumnsHeader> columns;

    AdditionalInformation additionalInfo;
  };

  struct DefaultValuesHeader {
    int32_t columnId = -1;
    std::string value;

    AdditionalInformation additionalInfo;
  };

  struct ColumnHeader {
    int32_t id = -1;
    int32_t tableId;
    std::string name;
    uint8_t dataType;
    int16_t recordSize;
    bool isNullable;
    int16_t ordinalPosition;
    bool isSystem;

    IdentityColumnsHeader identity;
    DefaultValuesHeader defaultValue;
    AdditionalInformation additionalInfo;
  };

  struct TableHeader {
    int32_t id = -1;
    int32_t databaseId;
    int32_t schemaId;
    std::string name;
    int16_t ordinalPosition;
    bool isSystem;

    AdditionalInformation additionalInfo;
    
    vector<ColumnHeader> columns;
    vector<ConstraintsHeader> constraints;

    vector<IdentityColumnsHeader> identity;
  };

  struct SchemaHeader {
    int32_t id = -1;
    int32_t databaseId;
    std::string name;
    AdditionalInformation additionalInfo;
  };

  struct DatabaseHeader {
    int32_t id = -1;
    std::string name;
    std::string filepath;
    bool isSystem;
    AdditionalInformation additionalInfo;

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
    uint32_t pageId = -1;
    int32_t indexId = -1;

    RowIdentifier() {
      this->pageId = -1;
      this->indexId = -1;
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