#pragma once
#include "LogicalOperator.h"

#include <string>
#include <vector>

namespace QueryPipeline {
  class LogicalProject: public LogicalOperator {
  public:
    LogicalOperator* child;
    std::vector<std::string> columns;
    LogicalProject(LogicalOperator* child, const std::vector<std::string>& columns) : child(child), columns(std::move(columns)) {}
    ~LogicalProject() override {
      delete child;
    }
  };
}