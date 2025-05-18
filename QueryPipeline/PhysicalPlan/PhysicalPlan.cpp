#include "PhysicalPlan.h"
#include "../../Database/Database.h"
#include "../../Database/Block/Block.h"
#include "../../Database/Table/Table.h"
#include "../../Server/Server.h"
#include "../LogicalPlan/LogicalPlan.h"

namespace QueryPipeline::PhysicalPlan {
  PhysicalCreateDatabase::PhysicalCreateDatabase(const std::string &name) : dbName(name){}

  PhysicalPlanResult PhysicalCreateDatabase::Execute(){
    Server::ServerInstance::Get().InsertDbToMasterDb(this->dbName, this->dbName + ".db");

    DatabaseEngine::CreateDatabase(this->dbName);

    return {};
  }

  PhysicalProject::PhysicalProject(PhysicalOperator *child, std::vector<std::string> columns)
    : columns(std::move(columns)), child(child) {}

  PhysicalProject::~PhysicalProject(){ delete this->child; }

  PhysicalPlanResult PhysicalProject::Execute(){
      return this->child->Execute();
  }

  PhysicalFilter::PhysicalFilter(PhysicalOperator *child, const vector<Expression> &filters): child(child) {
    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary("masterDb", "sys_columns");

    for (const auto &filter : filters) {
      Server::ColumnHeader header;
      columnsDict.TryGetValue(filter.column, header);
      
      this->filters.emplace_back(PhysicalExpression{
        static_cast<column_index_t>(header.tablePosition),
        filter.operation,
        filter.value,
      });
    }
  }

  PhysicalFilter::~PhysicalFilter(){
    delete this->child;
  }

  bool PhysicalFilter::EvaluateExpression(const DatabaseEngine::StorageTypes::Row &row)const{
      const auto& data = row.GetData();
    
      for (const auto& filter: this->filters) {
        const auto columnIndex = filter.column;

        if (data[columnIndex]->GetString() != filter.value)
          return false;
      }

    return true;
  }

  PhysicalPlanResult PhysicalFilter::Execute(){
    const auto childResult = child->Execute();
    
    PhysicalPlanResult result;
    for (const auto &row : childResult.rows) {
      
      if (!this->EvaluateExpression(row))
        continue;

      result.rows.emplace_back(row);
    }

    return result;
  }

  PhysicalTableScan::PhysicalTableScan(const std::string &tableName): tableName(tableName) {}

  PhysicalPlanResult PhysicalTableScan::Execute(){
      using namespace DatabaseEngine;
      using namespace DatabaseEngine::StorageTypes;
      Database* db = nullptr;

      UseDatabase("masterDb", &db);

      Table* table = db->OpenTable(this->tableName);

      PhysicalPlanResult result;

      const vector<column_index_t> columnIndices;
      table->Select(result.rows, columnIndices);

      return result;
    }
}