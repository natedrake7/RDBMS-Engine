#pragma once
#include <string>
#include "../DateTime/DateTime.h"
#include <vector>

namespace Headers {
  struct SchemaHeader {
    int32_t id;
    int32_t databaseId;
    std::string name;
    DataTypes::DateTime createdAt;
    DataTypes::DateTime lastModified;
    std::string lastModifiedBy;
  };

  struct ColumnHeader {
    int32_t id;
    int32_t tableId;
    std::string name;
    std::string dataType;
    int16_t recordSize;
    bool isNullable;
    int16_t tablePosition;
    bool isSystem;
    DataTypes::DateTime createdAt;
    DataTypes::DateTime lastModified;
    std::string lastModifiedBy;
  };

  typedef struct IndexHeader {
    int32_t id;
    int32_t tableId;
    std::string name;
    std::vector<uint8_t> columns;
    bool isClustered;
    int32_t seed;
    int32_t autoIncrement;
    int32_t lastValue;
    DataTypes::DateTime createdAt;
    DataTypes::DateTime lastModified;
    std::string lastModifiedBy;
  }IndexHeader;

  typedef struct TableHeader {
    int32_t id = -1;
    int32_t databaseId;
    int32_t schemaId;
    std::string name;
    int16_t tablePosition;
    bool isSystem;
    DataTypes::DateTime createdAt;
    DataTypes::DateTime lastModified;
    std::string lastModifiedBy;
    
    vector<ColumnHeader> columns;
    vector<IndexHeader> indexes;

  }TableHeader;

  typedef struct DatabaseHeader {
    int32_t id;
    std::string name;
    std::string filepath;
    bool isSystem;
    DataTypes::DateTime createdAt;
    DataTypes::DateTime lastModified;
    std::string lastModifiedBy;
    std::vector<TableHeader> tables;
    std::vector<SchemaHeader> schemas;
  }DatabaseHeader;

  struct sysColumn {
    string name;
    string type;
    int size = 0;
  };

  struct sysTable {
    string name;
    std::vector<sysColumn> columns;
    std::vector<string> primaryKey;
  };

  struct Index{
    vector<uint8_t> columns;
    int32_t seed;
    int32_t incrementFactor;
    int64_t lastValue;

    Index(vector<uint8_t>& columns, const int32_t& seed, const int32_t& incrementFactor)
      : columns(std::move(columns)), seed(seed), incrementFactor(incrementFactor), lastValue(seed) {}

    Index(){
      this->seed = 0;
      this->incrementFactor = 0;
      this->columns.emplace_back(0);
    }
  };
}