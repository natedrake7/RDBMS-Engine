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
    Server::ServerInstance::Get().InsertSchemaToMasterDb(this->dbName, "dbo");
    
    DatabaseEngine::CreateDatabase(this->dbName);

    return new PhysicalPlanResult();
  }

PhysicalSchemaCreate::PhysicalSchemaCreate(const std::string &dbName, std::string &schemaName)
  : PhysicalOperator(dbName), schemaName(std::move(schemaName)) {}

  PhysicalPlanResult * PhysicalSchemaCreate::Execute(){
    Server::ServerInstance::Get().InsertSchemaToMasterDb(this->dbName, this->schemaName);

    return new PhysicalPlanResult();
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

  PhysicalFilter::PhysicalFilter(const std::string& dbName, PhysicalOperator *child, Expressions::Expression* filter)
        : PhysicalOperator(dbName), filter(filter) , child(child) {}

  PhysicalFilter::~PhysicalFilter(){
    delete this->child;
    delete this->filter;
  }


  PhysicalPlanResult* PhysicalFilter::Execute(){
    auto* result = child->Execute();

    if(dynamic_cast<PhysicalIndexScan*>(child) != nullptr
      || dynamic_cast<PhysicalIndexSeek*>(child) != nullptr)
      return result;

    for (const auto &row : result->rows) {
      
      if (!row.Evaluate(this->filter))
        continue;

      result->rows.emplace_back(row);
    }

    return result;
  }

  PhysicalTableScan::PhysicalTableScan(const std::string& dbName, Statements::TableName* table): PhysicalOperator(dbName), table(table) {}

  PhysicalPlanResult* PhysicalTableScan::Execute(){
      using namespace DatabaseEngine::StorageTypes;
      const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->dbName);

      Table* tablePtr = db->OpenTable(this->table->schema, this->table->name);

      auto* result = new PhysicalPlanResult();

      tablePtr->HeapScan(&result->rows, -1);

      return result;
    }

  PhysicalIndexScan::PhysicalIndexScan(const std::string &dbName, Statements::TableName* table, const bool& isClustered)
    : PhysicalOperator(dbName), table(table), isClustered(isClustered), expression(nullptr) {}

  PhysicalIndexScan::PhysicalIndexScan(const string & dbName, Statements::TableName *table, Expressions::Expression *expression, const bool & isClustered)
    : PhysicalOperator(dbName), table(table), expression(expression), isClustered(isClustered) {}

  PhysicalPlanResult * PhysicalIndexScan::Execute(){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->dbName);

    Table* tablePtr = db->OpenTable(this->table->schema, this->table->name);

    if (isClustered) {
      tablePtr->ClusteredIndexScan(&result->rows, this->expression);
      return result;
    }

    return result;
  }

  PhysicalIndexSeek::PhysicalIndexSeek(const std::string &dbName, Statements::TableName* table, const Field& minValue, const Field& maxValue)
    : PhysicalOperator(dbName), table(table), minValue(minValue), maxValue(maxValue) {}

  PhysicalPlanResult* PhysicalIndexSeek::Execute(){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->dbName);

    Table* tablePtr = db->OpenTable(this->table->schema, this->table->name);

    const Indexing::Key minKey(minValue);
    const Indexing::Key maxKey(maxValue);

    //select if to use clustered or non clustered index here

    tablePtr->ClusteredIndexSeek(&result->rows,&minKey, &maxKey);

    return result;
  }

  PhysicalInsert::PhysicalInsert(const std::string& dbName, Statements::TableName* table, const std::vector<Field> &fields)
    : PhysicalOperator(dbName), table(table), fields(fields) {}

  PhysicalPlanResult* PhysicalInsert::Execute(){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->dbName);
    
    Table* tablePtr = db->OpenTable(this->table->schema, this->table->name);


    for(int i = 0;i < 1000; i++){
      this->fields[0].SetData(i);

      const auto smallStr = std::string(1000, 'w');

      this->fields[1].SetData(smallStr);

      const auto str = std::string(10000, 'a');
      this->fields[2].SetData(str);

      const auto medStr = std::string(7500, 'u');
      this->fields[3].SetData(medStr);

      const auto insertResult = tablePtr->InsertRow(fields);
    }



    // const auto insertResult = tablePtr->InsertRow(fields);
    //
    // result->code = insertResult.code;
    // result->message = insertResult.message;

    return result;
  }

  PhysicalHeapDelete::PhysicalHeapDelete(const std::string &dbName, Statements::TableName *table, Expressions::Expression *expression)
    : PhysicalOperator(dbName), table(table), expression(expression) {}

  PhysicalHeapDelete::~PhysicalHeapDelete(){
    delete this->expression;
    delete this->table;
  }

  PhysicalPlanResult * PhysicalHeapDelete::Execute(){
    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->dbName);

    const DatabaseEngine::StorageTypes::Table* tablePtr = db->OpenTable(this->table->schema, this->table->name);

    tablePtr->HeapDelete(this->expression);

    return result;
  }

  PhysicalIndexScanDelete::PhysicalIndexScanDelete(const std::string &dbName, Statements::TableName *table, Expressions::Expression *expression)
    : PhysicalOperator(dbName), table(table), expression(expression) {}

  PhysicalIndexScanDelete::~PhysicalIndexScanDelete(){
      delete this->expression;
      delete this->table;
  }

  PhysicalPlanResult * PhysicalIndexScanDelete::Execute(){
    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->dbName);

    DatabaseEngine::StorageTypes::Table* tablePtr = db->OpenTable(this->table->schema, this->table->name);

    tablePtr->ClusteredIndexScanDelete(this->expression);

    return result;
  }

  PhysicalIndexSeekDelete::PhysicalIndexSeekDelete(const std::string &dbName, Statements::TableName *table, Expressions::Expression *expression)
    : PhysicalOperator(dbName), table(table), expression(expression) {}

  PhysicalIndexSeekDelete::~PhysicalIndexSeekDelete(){
    delete this->expression;
    delete this->table;
  }

  PhysicalPlanResult * PhysicalIndexSeekDelete::Execute(){
    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->dbName);

    DatabaseEngine::StorageTypes::Table* tablePtr = db->OpenTable(this->table->schema, this->table->name);

    tablePtr->ClusteredIndexSeekDelete(expression);

    return result;
  }

  PhysicalTableCreate::PhysicalTableCreate(const std::string& dbName, Statements::TableName*  table, std::vector<Statements::AddColumn> &columns, std::vector<column_index_t>& primaryKey, std::string& constraintName)
    : PhysicalOperator(dbName), table(table), constraintName(std::move(constraintName)), columns(std::move(columns)), primaryKey(std::move(primaryKey)) {}

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

    db->CreateTable(this->table->name, this->table->schema, index, columnsPtrs, &this->primaryKey);

    Server::ServerInstance::Get().InsertTableToMasterDb(dbName, this->table->name, index, this->table->schema);

    std::string indexColumns;
    for (const auto& column: this->columns) {
      Server::ServerInstance::Get().InsertColumnToMasterDb(
        dbName,
        this->table->name,
        column.name,
        column.type.name,
        column.type.size,
        column.isNullable,
        column.index
        );
    }

    const bool isConstraintEmpty = this->constraintName.empty();

    for (const auto& column: this->primaryKey) {
      indexColumns += indexColumns.empty() ? to_string(column) : "," + to_string(column);

      if (isConstraintEmpty)
        this->constraintName += this->constraintName.empty() ? "PK_" + this->columns[column].name :"_" + this->columns[column].name;
    }

    if (!indexColumns.empty())
      Server::ServerInstance::Get().InsertIndexToMasterDb(dbName, this->table->name, this->constraintName, indexColumns, true);

    return nullptr;
  }

  PhysicalHeapUpdate::PhysicalHeapUpdate(const string & dbName, Statements::TableName *table, Expressions::Expression *expression, vector<Field> & fields)
  : PhysicalOperator(dbName), table(table), expression(expression), fields(std::move(fields)) {}

  PhysicalHeapUpdate::~PhysicalHeapUpdate(){
    delete this->expression;
    delete this->table;
  }

  PhysicalPlanResult* PhysicalHeapUpdate::Execute(){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->dbName);

    Table* tablePtr = db->OpenTable(this->table->schema, this->table->name);

    tablePtr->HeapUpdate(this->expression, this->fields);

    return result;
  }

  PhysicalIndexScanUpdate::PhysicalIndexScanUpdate(const string & dbName, Statements::TableName *table, Expressions::Expression *expression, vector<Field> & fields)
  : PhysicalOperator(dbName), table(table), expression(expression), fields(std::move(fields)) {}

  PhysicalIndexScanUpdate::~PhysicalIndexScanUpdate(){
    delete this->expression;
    delete this->table;
  }

  PhysicalPlanResult* PhysicalIndexScanUpdate::Execute(){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->dbName);

    Table* tablePtr = db->OpenTable(this->table->schema, this->table->name);

    tablePtr->ClusteredIndexScanUpdate(this->expression, this->fields);

    return result;
  }


  PhysicalIndexSeekUpdate::PhysicalIndexSeekUpdate(const string & dbName, Statements::TableName *table, Expressions::Expression *expression, vector<Field> & fields)
    : PhysicalOperator(dbName), table(table), expression(expression), fields(std::move(fields)) {}

  PhysicalIndexSeekUpdate::~PhysicalIndexSeekUpdate(){
    delete this->expression;
    delete this->table;
  }

  PhysicalPlanResult* PhysicalIndexSeekUpdate::Execute(){
    using namespace DatabaseEngine::StorageTypes;

    auto* result = new PhysicalPlanResult();

    const DatabaseEngine::Database* db = Server::ServerInstance::Get().UseDatabase(this->dbName);

    Table* tablePtr = db->OpenTable(this->table->schema, this->table->name);

    tablePtr->ClusteredIndexScanUpdate(this->expression, this->fields);

    return result;
  }

}