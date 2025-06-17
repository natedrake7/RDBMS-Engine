#include "PhysicalPlan.h"
#include "../../Server/Server.h"

namespace QueryPipeline::PhysicalPlan{

  PhysicalAddColumn::PhysicalAddColumn(const int32_t &databaseId, Statements::TableName *table, Statements::AddColumn *column)
    : PhysicalOperator(databaseId), table(table), column(column){}

  PhysicalAddColumn::~PhysicalAddColumn(){
    delete this->table;
    delete this->column;
  }

  PhysicalPlanResult * PhysicalAddColumn::Execute(){
    return nullptr;
  }

  PhysicalDropColumn::PhysicalDropColumn(const int32_t &databaseId, Statements::TableName *table, Statements::DropColumn *column)
    : PhysicalOperator(databaseId), table(table), column(column){}

  PhysicalDropColumn::~PhysicalDropColumn(){
    delete this->table;
    delete this->column;
  }

  PhysicalPlanResult * PhysicalDropColumn::Execute(){
    return nullptr;
  }

  PhysicalRenameColumn::PhysicalRenameColumn(const int32_t &databaseId, Statements::TableName *table, Statements::RenameColumn *column)
  : PhysicalOperator(databaseId), table(table), column(column){}

  PhysicalRenameColumn::~PhysicalRenameColumn(){
    delete this->table;
    delete this->column;
  }

  PhysicalPlanResult * PhysicalRenameColumn::Execute(){
    auto* result = new PhysicalPlanResult();

    const auto* db = Server::ServerInstance::Get().UseDatabase(this->databaseId);

    const auto* table = db->OpenTable(this->table->ordinalPosition);

    const std::vector<Field> updates = {
      Field(this->column->newName.name, 2),
    };

    Server::ServerInstance::Get().UpdateColumnById(this->column->columnId, updates);

    table->UpdateColumnName(this->column->ordinalPosition, this->column->newName.name);

    return result;
  }

  PhysicalAlterColumn::PhysicalAlterColumn(const int32_t &databaseId, Statements::TableName *table, Statements::AlterColumn *column)
    : PhysicalOperator(databaseId), table(table), column(column){}

  PhysicalAlterColumn::~PhysicalAlterColumn(){
    delete this->table;
    delete this->column;
  }

  PhysicalPlanResult * PhysicalAlterColumn::Execute(){
    return nullptr;
  }

};