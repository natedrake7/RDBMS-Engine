#include "PhysicalPlan.h"

#include <utility>
#include "../../Database/Database.h"
#include "../../Database/Block/Block.h"
#include "../../Database/Table/Table.h"
#include "../../Server/Server.h"
#include "../Statements/Statements.h"

namespace QueryPipeline::PhysicalPlan {
  PhysicalCreateDatabase::PhysicalCreateDatabase(std::string name) : dbName(std::move(name)){}

  PhysicalPlanResult* PhysicalCreateDatabase::Execute(){
    Server::ServerInstance::Get().InsertDbToMasterDb(this->dbName, this->dbName + ".db");

    DatabaseEngine::CreateDatabase(this->dbName);

    return nullptr;
  }

  PhysicalProject::PhysicalProject(const std::string& dbName, PhysicalOperator *child, const std::vector<column_index_t>& columns)
    : PhysicalOperator(dbName), columns(columns), child(child) {}

  PhysicalProject::~PhysicalProject(){ delete this->child; }

  PhysicalPlanResult* PhysicalProject::Execute(){
      auto* result = this->child->Execute();

      for (auto& row: result->rows) {
          auto& data = row.GetData();

        vector<DatabaseEngine::StorageTypes::Block*> newData;

        for (int i = 0;i < data.size(); i++) {
          if (!columns.Contains(i)) {
            delete data[i];
            continue;
          }

          newData.push_back(data[i]);
        }

        data = std::move(newData);
      }

    return result;
  }

  PhysicalFilter::PhysicalFilter(const std::string& dbName, PhysicalOperator *child, Statements::Expression* filter)
        : PhysicalOperator(dbName), filter(filter) , child(child) {}

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

  PhysicalPlanResult* PhysicalFilter::Execute(){
    auto* result = child->Execute();

    for (const auto &row : result->rows) {
      
      if (!PhysicalFilter::EvaluateExpression(this->filter, row))
        continue;

      result->rows.emplace_back(row);
    }

    return result;
  }

  PhysicalTableScan::PhysicalTableScan(const std::string& dbName, std::string tableName): PhysicalOperator(dbName), tableName(std::move(tableName)) {}

  PhysicalPlanResult* PhysicalTableScan::Execute(){
      using namespace DatabaseEngine::StorageTypes;
      const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->dbName);

      Table* table = db->OpenTable(this->tableName);

      auto* result = new PhysicalPlanResult();

      table->Select(result->rows, {});

      return result;
    }

  PhysicalIndexScan::PhysicalIndexScan(const std::string &dbName, std::string &tableName, const bool& isClustered)
    : PhysicalOperator(dbName), tableName(std::move(tableName)), isClustered(isClustered) {}

  PhysicalPlanResult * PhysicalIndexScan::Execute(){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->dbName);

    Table* table = db->OpenTable(this->tableName);

    if (isClustered) {
      table->ClusteredIndexScan(&result->rows, {});
      return result;
    }

    return result;
  }

  PhysicalIndexSeek::PhysicalIndexSeek(const std::string &dbName, std::string& tableName, const Field& minValue, const Field& maxValue)
    : PhysicalOperator(dbName), tableName(std::move(tableName)), minValue(minValue), maxValue(maxValue) {}

  PhysicalPlanResult* PhysicalIndexSeek::Execute(){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->dbName);

    Table* table = db->OpenTable(this->tableName);

    const Indexing::Key minKey(minValue);
    const Indexing::Key maxKey(maxValue);

    //select if to use clustered or non clustered index here

    table->ClusteredIndexSeek(&result->rows,&minKey, &maxKey , {});

    return result;
  }

  PhysicalInsert::PhysicalInsert(const std::string& dbName, std::string tableName, const std::vector<Field> &fields): PhysicalOperator(dbName), tableName(std::move(tableName)), fields(fields) {}

  PhysicalPlanResult* PhysicalInsert::Execute(){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->dbName);
    
    Table* table = db->OpenTable(this->tableName);

    table->InsertRows({fields});

    return result;
  }

  PhysicalTableCreate::PhysicalTableCreate(const std::string& dbName, std::string &name, std::vector<Statements::AddColumn> &columns, std::vector<column_index_t>& primaryKey)
    : PhysicalOperator(dbName), name(std::move(name)), columns(std::move(columns)), primaryKey(std::move(primaryKey)) {}

  PhysicalPlanResult* PhysicalTableCreate::Execute(){
    
    DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->dbName);

    vector<DatabaseEngine::StorageTypes::Column*> columnsPtrs;
    columnsPtrs.reserve(columns.size());
    
    for (const auto& column: this->columns)
      columnsPtrs.push_back(new DatabaseEngine::StorageTypes::Column(
        column.name,
        ColumnTypesDictionary.Get(column.type.name),
        column.type.size,
        column.index,
        column.isNullable
        ));

    const auto tables = Server::ServerInstance::Get().SelectTables(this->dbName);

    const auto index = tables.empty() ? 0 : tables[tables.size() - 1].id + 1;

    db->CreateTable(this->name, index, columnsPtrs, &this->primaryKey);

    Server::ServerInstance::Get().InsertTableToMasterDb(dbName, this->name, index);

    std::string indexColumns;
    std::string indexKey = "PK_";

    for (const auto& column: this->columns) {
      Server::ServerInstance::Get().InsertColumnToMasterDb(
        dbName,
        this->name,
        column.name,
        column.type.name,
        column.type.size,
        column.isNullable,
        column.index
        );

      if (column.isPrimaryKey) {
        indexColumns += "," + column.name;
        indexKey += "_" + column.name;
      }
    }

    if (!indexColumns.empty())
      Server::ServerInstance::Get().InsertIndexToMasterDb(dbName, this->name, indexKey, indexColumns, true);

    return nullptr;
  }
}