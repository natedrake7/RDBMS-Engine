#pragma once
#include <string>
#include "../DateTime/DateTime.h"
#include <vector>

namespace Headers {
  struct SchemaHeader {
    std::string dbName;
    std::string name;
    DataTypes::DateTime createdAt;
    DataTypes::DateTime lastModified;
    std::string lastModifiedBy;
  };

  struct ColumnHeader {
    std::string dbName;
    std::string tableName;
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
    std::string dbName;
    std::string schemaName;
    std::string tableName;
    std::string name;
    std::vector<uint8_t> columns;
    bool isClustered;
    int32_t seed;
    int32_t autoIncrement;
    DataTypes::DateTime createdAt;
    DataTypes::DateTime lastModified;
    std::string lastModifiedBy;
  }IndexHeader;

  typedef struct TableHeader {
    std::string dbName;
    std::string name;
    int16_t id;
    std::string schemaName;
    bool isSystem;
    DataTypes::DateTime createdAt;
    DataTypes::DateTime lastModified;
    std::string lastModifiedBy;
    
    vector<ColumnHeader> columns;
    vector<IndexHeader> indexes;
  }TableHeader;

  typedef struct DatabaseHeader {
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
}