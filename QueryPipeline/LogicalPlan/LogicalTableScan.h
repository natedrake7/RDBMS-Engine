#pragma once
#include "LogicalOperator.h"
#include <string>

namespace QueryPipeline {
  class LogicalTableScan : public LogicalOperator {
    public:
      std::string tableName;
      explicit LogicalTableScan(const std::string& name) : tableName(name) {}
  };
}