#include "PhysicalPlan.h"
#include "../../Server/MasterDbColumns.h"
#include "../../Server/Server.h"
#include "../../Systemic/Functions/StringFunctions.h"

namespace QueryPipeline::PhysicalPlan{

  PhysicalAddColumn::PhysicalAddColumn(const DataTypes::Guid& sessionId, Statements::TableName *table, Statements::NewColumn *column)
    : PhysicalOperator(sessionId), table(table), column(column){}

  PhysicalAddColumn::~PhysicalAddColumn(){
    delete this->table;
    delete this->column;
  }

  PhysicalPlanResult * PhysicalAddColumn::Execute(const PhysicalPlanExecutionProperties& properties){
    const auto columnType = ColumnTypesDictionary.Get(Functions::String::NormalizeString(this->column->type.name));

    auto& server = Server::ServerInstance::Get();

    const auto* session = server.GetSession(this->sessionId);

    if (session == nullptr || session->user == nullptr)
      return new PhysicalPlanResult{
        Errors::RuntimeError::Error,
        "Failed to retrieve user session"
      };

    //if add occurs in a different index pos chaos ensues
    const auto columnResult =
        server.InsertColumnToMasterDb(
          properties.transactionId,
          this->table->tableId,
          this->column->name.name,
          columnType,
          this->column->type.size,
          this->column->type.decimal.precision,
          this->column->type.decimal.scale,
          this->column->isNullable,
          this->column->index,
          false,
          session->user->name
          );

      if (columnResult.code != Errors::RuntimeError::Ok) {
        auto* result = new PhysicalPlanResult();
        result->code = columnResult.code;
        result->message = columnResult.message;
        return result;
      }

    if (!this->column->defaultValue.GetIsNull()) {
      const auto value = this->column->defaultValue.GetString();
      const auto defaultValueResult = server.InsertDefaultValuesToMasterDb(properties.transactionId, columnResult.primaryKey.GetKeyAsInt(), this->column->defaultValue);
    }

    const auto* db = server.UseDatabase(this->table->databaseId);

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

  PhysicalDropColumn::PhysicalDropColumn(const DataTypes::Guid& sessionId, Statements::TableName *table, Statements::DropColumn *column)
    : PhysicalOperator(sessionId), table(table), column(column){}

  PhysicalDropColumn::~PhysicalDropColumn(){
    delete this->table;
    delete this->column;
  }

  PhysicalPlanResult * PhysicalDropColumn::Execute(const PhysicalPlanExecutionProperties& properties){
    auto* result = new PhysicalPlanResult();

    auto& server = Server::ServerInstance::Get();

    const auto* session = server.GetSession(this->sessionId);

    if (session == nullptr || session->user == nullptr)
      return new PhysicalPlanResult{
        Errors::RuntimeError::Error,
        "Failed to retrieve user session"
      };

    //update master db set isDeleted to 1
    //remove it from table, remove it from rows. Adjust column indexes if need be.
    const auto* db = Server::ServerInstance::Get().UseDatabase(this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->RemoveColumn(this->column->index);

    return result;
  }

  PhysicalRenameColumn::PhysicalRenameColumn(const DataTypes::Guid& sessionId, Statements::TableName *table, Statements::RenameColumn *column)
  : PhysicalOperator(sessionId), table(table), column(column){}

  PhysicalRenameColumn::~PhysicalRenameColumn(){
    delete this->table;
    delete this->column;
  }

  PhysicalPlanResult * PhysicalRenameColumn::Execute(const PhysicalPlanExecutionProperties& properties){
    auto* result = new PhysicalPlanResult();

    auto& server = Server::ServerInstance::Get();

    const auto* session = server.GetSession(this->sessionId);

    if (session == nullptr || session->user == nullptr)
      return new PhysicalPlanResult{
        Errors::RuntimeError::Error,
        "Failed to retrieve user session"
      };

    const auto* db = server.UseDatabase(this->table->databaseId);

    const auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    const std::vector<Value> updates = {
      Value(this->column->newName.name, static_cast<Constants::column_index_t>(Server::SysColumns::Name)),
      Value(DataTypes::DateTime::Now(), static_cast<Constants::column_index_t>(Server::SysColumns::LastModifiedAt)),
      Value(session->user->name, static_cast<Constants::column_index_t>(Server::SysColumns::LastModifiedBy)),
    };

    server.UpdateColumnById(this->column->columnId, updates);

    tablePtr->UpdateColumnName(this->column->ordinalPosition, this->column->newName.name);

    return result;
  }

  PhysicalAlterColumn::PhysicalAlterColumn(const DataTypes::Guid& sessionId, Statements::TableName *table, Statements::AlterColumn *column)
    : PhysicalOperator(sessionId), table(table), column(column){}

  PhysicalAlterColumn::~PhysicalAlterColumn(){
    delete this->table;
    delete this->column;
  }

  PhysicalPlanResult * PhysicalAlterColumn::Execute(const PhysicalPlanExecutionProperties& properties){
    auto* result = new PhysicalPlanResult();

    auto& server = Server::ServerInstance::Get();

    const auto* session = server.GetSession(this->sessionId);

    if (session == nullptr || session->user == nullptr)
      return new PhysicalPlanResult{
        Errors::RuntimeError::Error,
        "Failed to retrieve user session"
      };

    const std::vector<Value> updates = {
      Value(this->column->type.size, static_cast<Constants::column_index_t>(Server::SysColumns::RecordSize)),
      Value(DataTypes::DateTime::Now(), static_cast<Constants::column_index_t>(Server::SysColumns::LastModifiedAt)),
      Value(session->user->name, static_cast<Constants::column_index_t>(Server::SysColumns::LastModifiedBy)),
    };

    server.UpdateColumnById(this->column->columnId, updates);

    return result;
  }
};