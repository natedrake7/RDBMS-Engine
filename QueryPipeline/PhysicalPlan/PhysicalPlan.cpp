#include "PhysicalPlan.h"
#include "../../Database/Database.h"
#include "../../Database/Block/Block.h"
#include "../../Database/Table/Table.h"
#include "../../Server/Server.h"
#include "../Statements/Statements.h"

namespace QueryPipeline::PhysicalPlan {
  PhysicalCreateDatabase::PhysicalCreateDatabase(const std::string &name) : dbName(name){}

  PhysicalPlanResult PhysicalCreateDatabase::Execute(){
    Server::ServerInstance::Get().InsertDbToMasterDb(this->dbName, this->dbName + ".db");

    DatabaseEngine::CreateDatabase(this->dbName);

    return {};
  }

  PhysicalProject::PhysicalProject(PhysicalOperator *child, const std::vector<column_index_t>& columns)
    : columns(std::move(columns)), child(child) {}

  PhysicalProject::~PhysicalProject(){ delete this->child; }

  PhysicalPlanResult PhysicalProject::Execute(){
      auto result = this->child->Execute();

      for (auto& row: result.rows) {
          auto& data = row.GetData();

        vector<DatabaseEngine::StorageTypes::Block*> newData;

        for (int i = 0;i < data.size(); i++) {
          if (!columns.Contains(i)) {
            delete data[i];
            continue;
          }

          newData.push_back(std::move(data[i]));
        }

        data = std::move(newData);
      }

    return result;
  }

  PhysicalFilter::PhysicalFilter(PhysicalOperator *child, Statements::Expression* filter): child(child) , filter(filter) {}

  PhysicalFilter::~PhysicalFilter(){
    delete this->child;
    delete this->filter;
  }


bool PhysicalFilter::EvaluateExpression(const Statements::Expression* filter, const DatabaseEngine::StorageTypes::Row &row){
    switch (filter->type) {
    case Statements::ExpressionType::Predicate: {

      const auto actualData = row.GetData()[filter->columnIndex];

      const auto& expected = filter->value;
      const std::string& op = filter->operation;

      if (op == "=") return *actualData == expected;
      if (op == "!=" || op == "<>") return *actualData != expected;
      if (op == "<") return *actualData < expected;
      if (op == ">") return *actualData > expected;
      if (op == "<=") return *actualData <= expected;
      if (op == ">=") return *actualData >= expected;

      throw std::runtime_error("Unknown operator: " + op);
    }
    case Statements::ExpressionType::And:
      return PhysicalFilter::EvaluateExpression(filter->left, row) && PhysicalFilter::EvaluateExpression(filter->right, row);
    case Statements::ExpressionType::Or:
      return PhysicalFilter::EvaluateExpression(filter->left, row) || PhysicalFilter::EvaluateExpression(filter->right, row);
    default:
      throw std::runtime_error("Invalid expression type");
    }
  }

  PhysicalPlanResult PhysicalFilter::Execute(){
    const auto childResult = child->Execute();
    
    PhysicalPlanResult result;
    for (const auto &row : childResult.rows) {
      
      if (!PhysicalFilter::EvaluateExpression(this->filter, row))
        continue;

      result.rows.emplace_back(row);
    }

    return result;
  }

  PhysicalTableScan::PhysicalTableScan(const std::string &tableName): tableName(tableName) {}

  PhysicalPlanResult PhysicalTableScan::Execute(){
      using namespace DatabaseEngine::StorageTypes;
      const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase("masterDb");

      Table* table = db->OpenTable(this->tableName);

      PhysicalPlanResult result;

      table->Select(result.rows, {});

      return result;
    }

  PhysicalInsert::PhysicalInsert(const std::string &tableName, const std::vector<Field> &fields): tableName(tableName), fields(fields) {}

  PhysicalPlanResult PhysicalInsert::Execute(){
    using namespace DatabaseEngine::StorageTypes;
    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase("masterDb");
    
    Table* table = db->OpenTable(this->tableName);

    table->InsertRows({fields});

    return {};
  }
}