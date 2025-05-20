#pragma once
#include <string>
#include "../DateTime/DateTime.h"

#include <vector>

namespace Headers {
  typedef struct DatabaseHeader {
    std::string name;
    std::string filepath;
    bool isSystem;
    DataTypes::DateTime createdAt;
    DataTypes::DateTime lastModified;
    std::string lastModifiedBy;
  }DatabaseHeader;

  typedef struct TableHeader {
    std::string dbName;
    std::string name;
    std::string schemaName;
    bool isSystem;
    DataTypes::DateTime createdAt;
    DataTypes::DateTime lastModified;
    std::string lastModifiedBy;
  }TableHeader;

  struct ColumnHeader {
    std::string dbName;
    std::string tableName;
    std::string name;
    std::string dataType;
    int16_t recordSize;
    bool isNullable;
    int16_t tablePosition;
    DataTypes::DateTime createdAt;
    DataTypes::DateTime lastModified;
    std::string lastModifiedBy;
  };

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