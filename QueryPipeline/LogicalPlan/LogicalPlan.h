#pragma once
#include "../PhysicalPlan/PhysicalPlan.h"

namespace QueryPipeline {
  class LogicalPlan {
  public:
    std::string dbName;
    explicit LogicalPlan(const std::string& dbName);
    LogicalPlan() = default;
    virtual ~LogicalPlan();
    virtual PhysicalPlan::PhysicalOperator* ToPhysical() = 0;
  };

  class LogicalCreateDatabase final : public LogicalPlan {
    public:
      std::string dbName;
      explicit LogicalCreateDatabase(std::string dbName);
      PhysicalPlan::PhysicalCreateDatabase* ToPhysical()override;
  };

  class LogicalProject final: public LogicalPlan {
    public:
      LogicalPlan* child;
      std::vector<column_index_t> columns;
      LogicalProject(const std::string& dbName, LogicalPlan* child, const std::vector<column_index_t>& columns);
      ~LogicalProject() override;
      PhysicalPlan::PhysicalProject* ToPhysical()override;
  };

  class LogicalTableScan final : public LogicalPlan {
    public:
      std::string tableName;
      Statements::Expression* expression;
      explicit LogicalTableScan(const std::string& dbName, std::string  name, Statements::Expression* expression);
      PhysicalPlan::PhysicalOperator* ToPhysical() override;
  };

  class LogicalTableIndexSeek final : public LogicalPlan {
    public:
      std::string tableName;
      Field minValue;
      Field maxValue;
      explicit LogicalTableIndexSeek(const std::string& dbName, std::string tableName, const Field& minValue, const Field& maxValue);
      PhysicalPlan::PhysicalIndexSeek* ToPhysical()override;
  };

  class LogicalFilter final : public LogicalPlan {
    public:
      LogicalPlan* child;
      Statements::Expression* filter;
      explicit LogicalFilter(const std::string& dbName, LogicalPlan* child, Statements::Expression* filter);
      PhysicalPlan::PhysicalFilter* ToPhysical()override;
  };

  class LogicalInsert final : public LogicalPlan {
    public:
      std::string tableName;
      std::vector<Field> fields;
      explicit LogicalInsert(const std::string& dbName, std::string  tableName, const std::vector<Field>& fields);
      PhysicalPlan::PhysicalInsert* ToPhysical()override;
  };

  class LogicalTableCreate final : public LogicalPlan {
    public:
      std::string name;
      std::vector<Statements::AddColumn> columns;
      std::vector<column_index_t> primaryKey;
      explicit LogicalTableCreate(const std::string& dbName, std::string  name, std::vector<Statements::AddColumn>& columns, std::vector<column_index_t>& primaryKey);
      PhysicalPlan::PhysicalTableCreate* ToPhysical()override;
  };
}

