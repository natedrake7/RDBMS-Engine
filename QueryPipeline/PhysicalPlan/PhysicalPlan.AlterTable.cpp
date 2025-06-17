#include "PhysicalPlan.h"
#include "../../Server/Server.h"
#include "../../AdditionalLibraries/StringFunctions/StringFunctions.h"

namespace QueryPipeline::PhysicalPlan{

  PhysicalAddColumn::PhysicalAddColumn(const int32_t &databaseId, Statements::TableName *table, Statements::AddColumn *column)
    : PhysicalOperator(databaseId), table(table), column(column){}

  PhysicalAddColumn::~PhysicalAddColumn(){
    delete this->table;
    delete this->column;
  }

  PhysicalPlanResult * PhysicalAddColumn::Execute(){

    const auto columnType = ColumnTypesDictionary.Get(AdditionalLibraries::NormalizeString(this->column->type.name));

    //if add occurs in a different index pos chaos ensues
    const auto columnResult =
        Server::ServerInstance::Get().InsertColumnToMasterDb(
          this->table->tableId,
          this->column->name.name,
          columnType,
          this->column->type.size,
          this->column->isNullable,
          this->column->index
          );

    const auto* db = Server::ServerInstance::Get().UseDatabase(this->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    auto* columnPtr = new DatabaseEngine::StorageTypes::Column(this->column->name.name, columnType, this->column->type.size, this->column->index, this->column->isNullable);
    columnPtr->SetColumnId(columnResult.primaryKeyVal);

    tablePtr->AddColumn(columnPtr);
    tablePtr->GetIdentityColumnById(columnResult.primaryKeyVal);

    tablePtr->PopulateColumn(this->column->index, this->column->defaultValue);

    //should be by id (to not disrupt the other column identities etc)

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