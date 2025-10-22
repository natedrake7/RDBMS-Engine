#include "PhysicalPlan.h"
#include "../../Server/Server.h"
#include "../../Systemic/Functions/StringFunctions.h"

#include <cstring>
#include <iostream>

namespace QueryPipeline::PhysicalPlan{

  PhysicalAddColumn::PhysicalAddColumn(Statements::TableName *table, Statements::NewColumn *column)
    : table(table), column(column){}

  PhysicalAddColumn::~PhysicalAddColumn(){
    delete this->table;
    delete this->column;
  }

  PhysicalPlanResult * PhysicalAddColumn::Execute(const int& batchSize){

    const auto columnType = ColumnTypesDictionary.Get(Functions::String::NormalizeString(this->column->type.name));

    //if add occurs in a different index pos chaos ensues
    const auto columnResult =
        Server::ServerInstance::Get().InsertColumnToMasterDb(
          this->table->tableId,
          this->column->name.name,
          columnType,
          this->column->type.size,
          this->column->type.decimal.precision,
          this->column->type.decimal.scale,
          this->column->isNullable,
          this->column->index
          );

      if (columnResult.code != Errors::ResultCode::Ok) {
        auto* result = new PhysicalPlanResult();
        result->code = columnResult.code;
        result->message = columnResult.message;
        return result;
      }

    if (!this->column->defaultValue.GetIsNull()) {
      const auto value = this->column->defaultValue.GetString();
      const auto defaultValueResult = Server::ServerInstance::Get().InsertDefaultValuesToMasterDb(columnResult.primaryKey.GetKeyAsInt(), this->column->defaultValue);
    }

    const auto* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    auto* columnPtr = new DatabaseEngine::StorageTypes::Column(
      this->column->name.name,
      columnType,
      this->column->type.size,
      this->column->index,
      this->column->isNullable
    );

    columnPtr->SetColumnId(columnResult.primaryKey.GetKeyAsInt());

    tablePtr->AddColumn(columnPtr);
    tablePtr->GetIdentityColumnById(columnResult.primaryKey.GetKeyAsInt());

    tablePtr->PopulateColumn(this->column->index, this->column->defaultValue);
    tablePtr->GetDefaultValuesHeaders();

    return nullptr;
  }

  PhysicalDropColumn::PhysicalDropColumn(Statements::TableName *table, Statements::DropColumn *column)
    : table(table), column(column){}

  PhysicalDropColumn::~PhysicalDropColumn(){
    delete this->table;
    delete this->column;
  }

  PhysicalPlanResult * PhysicalDropColumn::Execute(const int& batchSize){
    auto* result = new PhysicalPlanResult();

    //update master db set isDeleted to 1
    //remove it from table, remove it from rows. Adjust column indexes if need be.
    const auto* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->RemoveColumn(this->column->index);

    return result;
  }

  PhysicalRenameColumn::PhysicalRenameColumn(Statements::TableName *table, Statements::RenameColumn *column)
  : table(table), column(column){}

  PhysicalRenameColumn::~PhysicalRenameColumn(){
    delete this->table;
    delete this->column;
  }

  PhysicalPlanResult * PhysicalRenameColumn::Execute(const int& batchSize){
    auto* result = new PhysicalPlanResult();

    const auto* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

    const auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    const std::vector<Value> updates = {
      Value(this->column->newName.name, 2),
    };

    Server::ServerInstance::Get().UpdateColumnById(this->column->columnId, updates);

    tablePtr->UpdateColumnName(this->column->ordinalPosition, this->column->newName.name);

    return result;
  }

  PhysicalAlterColumn::PhysicalAlterColumn(Statements::TableName *table, Statements::AlterColumn *column)
    : table(table), column(column){}

  PhysicalAlterColumn::~PhysicalAlterColumn(){
    delete this->table;
    delete this->column;
  }

  PhysicalPlanResult * PhysicalAlterColumn::Execute(const int& batchSize){
    auto* result = new PhysicalPlanResult();

    const std::vector<Value> updates = {
      Value(this->column->type.size, 4)
    };

    Server::ServerInstance::Get().UpdateColumnById(this->column->columnId, updates);

    return result;
  }
};