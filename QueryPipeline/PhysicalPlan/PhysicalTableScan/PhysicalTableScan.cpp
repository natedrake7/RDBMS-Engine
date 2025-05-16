#include "PhysicalTableScan.h"

#include "../../../Database/Database.h"
#include "../../../Database/Table/Table.h"
#include "../../../Database/Row/Row.h"

namespace QueryPipeline::PhysicalPlan {
  std::vector<DatabaseEngine::StorageTypes::Row> PhysicalTableScan::Execute(){
    using namespace DatabaseEngine;
    using namespace DatabaseEngine::StorageTypes;
    Database* db = nullptr;
    
    UseDatabase("masterDb", &db);

    Table* table = db->OpenTable(this->tableName);
    vector<Row> rows;

    vector<column_index_t> columnIndices;
    table->Select(rows, columnIndices);

    return rows;
  }

}