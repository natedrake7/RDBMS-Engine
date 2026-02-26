#include "ValidationMessages.h"
#include "../../../DatabaseEngine/include/SystemDatabases/CatalogSchema.h"
#include "../../include/PhysicalPlan.h"
#include "../../../Server/include/Server.h"
#include "../../../Systemic/include/Functions/StringFunctions.h"
#include "../../../DatabaseEngine/include/SystemDatabases/SystemCatalog.h"
#include "../../../DatabaseEngine/include/DataStorage/Table.h"

namespace QueryPipeline::PhysicalPlan{

  PhysicalAddColumn::PhysicalAddColumn(const DataTypes::Guid& sessionId, Statements::DataSource *table, Statements::NewColumn *column)
    : ExecutionNode(sessionId), table(table), column(column){}

  PhysicalAddColumn::~PhysicalAddColumn() = default;

  ExecutionResult PhysicalAddColumn::Execute(const DatabaseEngine::ExecutionContext& context){
    const auto columnType = ColumnTypesDictionary.Get(Functions::String::NormalizeString(this->column->type.name));

    auto result = ExecutionResult();

    if (this->session == nullptr || this->session->user == nullptr){
        result.status = Errors::RuntimeStatus(Errors::RuntimeError::Error, Messages::FAILED_TO_RETRIEVE_USER_SESSION);
        return result;
    }

      //if add occurs in a different index pos chaos ensues
    result.status =
        this->catalog->InsertColumnToMasterDb(
          context,
          this->table->tableId,
          this->column->name.name,
          columnType,
          this->column->type.size,
          this->column->type.decimal.precision,
          this->column->type.decimal.scale,
          this->column->isNullable,
          this->column->index,
          false,
          this->session->user->name
          );

      if (!result.status.IsOk())
        return result;

      const auto columnId = result.status.primaryKey.AsInt(1);

      if (!this->column->defaultValue.IsNull()) {
        const auto value = this->column->defaultValue.AsString();

        const auto defaultValueResult =
            this->catalog->InsertDefaultValuesToMasterDb(
              context,
              columnId,
              this->column->defaultValue
            );
     }

    const auto* db = this->server->UseDatabase(context, this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    auto* columnPtr = new DatabaseEngine::StorageTypes::Column(
        this->column->name.name,
        columnType,
        this->column->type.size,
        this->column->index,
        this->column->isNullable
    );

    columnPtr->SetColumnId(columnId);

    tablePtr->AddColumn(columnPtr);
    tablePtr->RetrieveIdentityColumnById(context.GetAllocator(), columnId);

    tablePtr->PopulateColumn(this->column->index, this->column->defaultValue);
    tablePtr->RetrieveDefaultValuesFromCatalog(context.GetAllocator());

    return ExecutionResult();
  }

  PhysicalDropColumn::PhysicalDropColumn(const DataTypes::Guid& sessionId, Statements::DataSource *table, Statements::DropColumn *column)
    : ExecutionNode(sessionId), table(table), column(column){}

  PhysicalDropColumn::~PhysicalDropColumn() = default;

  ExecutionResult PhysicalDropColumn::Execute(const DatabaseEngine::ExecutionContext& context){
    auto result = ExecutionResult();

    if (this->session == nullptr || this->session->user == nullptr)
      return ExecutionResult(
        Errors::RuntimeError::Error,
        Messages::FAILED_TO_RETRIEVE_USER_SESSION
      );

    //update master db set isDeleted to 1
    //remove it from table, remove it from rows. Adjust column indexes if need be.
    const auto* db = this->server->UseDatabase(context, this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->RemoveColumn(context, this->column->index);

    return result;
  }

  PhysicalRenameColumn::PhysicalRenameColumn(const DataTypes::Guid& sessionId, Statements::DataSource *table, Statements::RenameColumn *column)
  : ExecutionNode(sessionId), table(table), column(column){}

  PhysicalRenameColumn::~PhysicalRenameColumn() = default;

  ExecutionResult PhysicalRenameColumn::Execute(const DatabaseEngine::ExecutionContext& context){
    auto result = ExecutionResult();

    if (this->session == nullptr || this->session->user == nullptr)
      return ExecutionResult(
        Errors::RuntimeError::Error,
        Messages::FAILED_TO_RETRIEVE_USER_SESSION
      );

    const auto* db = this->server->UseDatabase(context, this->table->databaseId);

    const auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    const std::vector updates = {
      Value(this->column->newName.name, context.GetAllocator(), static_cast<column_index_t>(DatabaseEngine::SysColumns::Name)),
      Value(DataTypes::DateTime::Now(), context.GetAllocator(), static_cast<column_index_t>(DatabaseEngine::SysColumns::LastModifiedAt)),
      Value(this->session->user->name, context.GetAllocator(), static_cast<column_index_t>(DatabaseEngine::SysColumns::LastModifiedBy)),
    };

    const auto _ = this->catalog->UpdateColumnById(context.GetAllocator(), this->column->columnId, updates);

    tablePtr->UpdateColumnName(this->column->ordinalPosition, this->column->newName.name);

    return result;
  }

  PhysicalAlterColumn::PhysicalAlterColumn(const DataTypes::Guid& sessionId, Statements::DataSource *table, Statements::AlterColumn *column)
    : ExecutionNode(sessionId), table(table), column(column){}

  PhysicalAlterColumn::~PhysicalAlterColumn() = default;

  ExecutionResult PhysicalAlterColumn::Execute(const DatabaseEngine::ExecutionContext& context){
    auto result = ExecutionResult();

    if (this->session == nullptr || this->session->user == nullptr)
      return ExecutionResult(
        Errors::RuntimeError::Error,
        Messages::FAILED_TO_RETRIEVE_USER_SESSION
      );

    const std::vector updates = {
      Value(this->column->type.size, context.GetAllocator(), static_cast<column_index_t>(DatabaseEngine::SysColumns::RecordSize)),
      Value(DataTypes::DateTime::Now(), context.GetAllocator(), static_cast<column_index_t>(DatabaseEngine::SysColumns::LastModifiedAt)),
      Value(this->session->user->name, context.GetAllocator(), static_cast<column_index_t>(DatabaseEngine::SysColumns::LastModifiedBy)),
    };

    const auto _ = this->catalog->UpdateColumnById(context.GetAllocator(), this->column->columnId, updates);

    return result;
  }
};