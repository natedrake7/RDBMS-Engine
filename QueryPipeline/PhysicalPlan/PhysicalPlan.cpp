#include "PhysicalPlan.h"
#include "../../Database/Database.h"
#include "../../Database/Table/Table.h"
#include "../../Server/Server.h"
#include "../LogicalPlan/LogicalPlan.h"

namespace QueryPipeline::PhysicalPlan {
  PhysicalCreateDatabase::PhysicalCreateDatabase(const std::string &name) : dbName(name){}

  std::vector<DatabaseEngine::StorageTypes::Row> PhysicalCreateDatabase::Execute(){
    Server::ServerInstance::Get().InsertDbToMasterDb(this->dbName, this->dbName + ".db");

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

  PhysicalTableScan::PhysicalTableScan(const std::string &tableName): tableName(tableName) {}

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