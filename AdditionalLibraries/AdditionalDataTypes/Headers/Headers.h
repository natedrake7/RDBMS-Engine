#pragma once
#include <string>
#include "../DateTime/DateTime.h"
#include <vector>

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

  struct ColumnHeader {
    int32_t id = -1;
    int32_t tableId;
    std::string name;
    uint8_t dataType;
    int16_t recordSize;
    bool isNullable;
    int16_t ordinalPosition;
    bool isSystem;
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
    std::vector<sysColumn> columns;
    std::vector<string> primaryKey;
  };

  struct Index{
    vector<uint8_t> columns;

    explicit Index(vector<uint8_t>& columns)
      : columns(std::move(columns)) {}

    Index() = default;
  };
}