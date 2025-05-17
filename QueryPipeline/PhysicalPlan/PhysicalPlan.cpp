#include "PhysicalPlan.h"
#include "../../Database/Database.h"
#include "../../Database/Table/Table.h"
#include "../LogicalPlan/LogicalPlan.h"

namespace QueryPipeline::PhysicalPlan {
  PhysicalOperator* BuildPhysicalPlan(LogicalPlan* logicalOperator) {
    if (auto project = dynamic_cast<LogicalProject*>(logicalOperator))
      return new PhysicalProject(BuildPhysicalPlan(project->child), project->columns);

    if (auto scan = dynamic_cast<LogicalTableScan*>(logicalOperator))
      return new PhysicalTableScan(scan->tableName);

    if (auto createDb = dynamic_cast<LogicalCreateDatabase*>(logicalOperator))
      return new PhysicalCreateDatabase(createDb->dbName);

    throw std::runtime_error("Unknown logical operator.");
  }

  std::vector<DatabaseEngine::StorageTypes::Row> PhysicalCreateDatabase::Execute(){
    DatabaseEngine::CreateDatabase(this->dbName);
    return {};
  }

   PhysicalProject::PhysicalProject(PhysicalOperator *child, std::vector<std::string> columns)
    : columns(std::move(columns)), child(child) {}

  PhysicalProject::~PhysicalProject(){ delete this->child; }

  std::vector<DatabaseEngine::StorageTypes::Row> PhysicalProject::Execute(){
      auto rows = child->Execute();

      return rows;
  }

  std::vector<DatabaseEngine::StorageTypes::Row> PhysicalTableScan::Execute(){
      using namespace DatabaseEngine;
      using namespace DatabaseEngine::StorageTypes;
      Database* db = nullptr;

      UseDatabase("masterDb", &db);

      Table* table = db->OpenTable(this->tableName);
      vector<Row> rows;

      constexpr vector<column_index_t> columnIndices;
      table->Select(rows, columnIndices);

      return rows;
    }
}