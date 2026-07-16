#include "ValidationMessages.h"
#include "../../../CoreEngine/include/SystemDatabases/CatalogSchema.h"
#include "../../include/PhysicalPlan.h"
#include "../../../Server/include/Server.h"
#include "../../../CoreEngine/include/SystemDatabases/SystemCatalog.h"
#include "../../../CoreEngine/include/DataStorage/Table.h"
#include "../../../Systemic/include/DataTypes/DataTypes.StaticData.h"

namespace QueryPipeline::PhysicalPlan{

  PhysicalAddColumn::PhysicalAddColumn(const DataTypes::Guid& sessionId, Statements::DataSource *table, Statements::NewColumn *column)
    : PlanNode(sessionId), table(table), column(column){}

  ExecutionResult PhysicalAddColumn::Execute(CoreEngine::ExecutionContext& context){
    this->column->type.name.ToLowerInPlace();
    const auto columnType = COLUMN_TYPENAMES_TO_ENUMS.Get(this->column->type.name.ToView());

    auto result = ExecutionResult(context);

    if (this->session == nullptr || this->session->user == nullptr){
        result.status = Errors::RuntimeStatus(
            Errors::RuntimeError::Error,
            Messages::FAILED_TO_RETRIEVE_USER_SESSION,
            context.GetAllocator()
        );
        return result;
    }

      //if add occurs in a different index pos chaos ensues
    result.status =
        this->catalog->InsertColumnToMasterDb(
          context,
          this->table->_tableId,
          this->column->name.name.ToView(),
          columnType,
          this->column->type.size,
          this->column->type.decimal.precision,
          this->column->type.decimal.scale,
          this->column->isNullable,
          this->column->index,
          false,
          this->session->user->name.ToView()
          );

      if (!result.status.IsOk())
        return result;

      const auto columnId = result.status.primaryKey.AsInt<Int>(1);

      if (!this->column->defaultValue.IsNull()) {
        const auto value = this->column->defaultValue.AsString();

        const auto defaultValueResult =
            this->catalog->InsertDefaultValuesToMasterDb(
              context,
              columnId,
              this->column->defaultValue
            );
     }

    const auto* db = this->server->UseDatabase(context, this->table->_databaseId);

    auto* tablePtr = db->OpenTable(this->table->_ordinalPosition);

    auto* columnPtr = tablePtr->AddColumn(
        this->column->name.name.ToView(),
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

    return ExecutionResult(context);
  }

  PhysicalDropColumn::PhysicalDropColumn(const DataTypes::Guid& sessionId, Statements::DataSource *table, Statements::DropColumn *column)
    : PlanNode(sessionId), table(table), column(column){}

  ExecutionResult PhysicalDropColumn::Execute(CoreEngine::ExecutionContext& context){
    auto result = ExecutionResult(context);

    if (this->session == nullptr || this->session->user == nullptr)
      return ExecutionResult(
        Errors::RuntimeError::Error,
        Messages::FAILED_TO_RETRIEVE_USER_SESSION,
        context.GetAllocator()
      );

    //update master db set isDeleted to 1
    //remove it from table, remove it from rows. Adjust column indexes if need be.
    const auto* db = this->server->UseDatabase(context, this->table->_databaseId);

    auto* tablePtr = db->OpenTable(this->table->_ordinalPosition);

    tablePtr->RemoveColumn(context, this->column->index);

    return result;
  }

  PhysicalRenameColumn::PhysicalRenameColumn(const DataTypes::Guid& sessionId, Statements::DataSource *table, Statements::RenameColumn *column)
  : PlanNode(sessionId), table(table), column(column){}

  ExecutionResult PhysicalRenameColumn::Execute(CoreEngine::ExecutionContext& context){
    auto result = ExecutionResult(context);

    if (this->session == nullptr || this->session->user == nullptr)
      return ExecutionResult(
        Errors::RuntimeError::Error,
        Messages::FAILED_TO_RETRIEVE_USER_SESSION,
        context.GetAllocator()
      );

    const auto* db = this->server->UseDatabase(context, this->table->_databaseId);

    const auto* tablePtr = db->OpenTable(this->table->_ordinalPosition);

    const auto updates = DataStructures::PolymorphicArray<Value>::From(
        context.GetAllocator(),
        Value(this->column->newName.name, context.GetAllocator(), static_cast<column_index_t>(CoreEngine::SysColumns::Name)),
        Value(DataTypes::DateTime::Now(), context.GetAllocator(), static_cast<column_index_t>(CoreEngine::SysColumns::LastModifiedAt)),
        Value(this->session->user->name, context.GetAllocator(), static_cast<column_index_t>(CoreEngine::SysColumns::LastModifiedBy))
    );

    const auto _ = this->catalog->UpdateColumnById(context.GetAllocator(), this->column->columnId, updates);

    tablePtr->UpdateColumnName(this->column->ordinalPosition, this->column->newName.name);

    return result;
  }

  PhysicalAlterColumn::PhysicalAlterColumn(const DataTypes::Guid& sessionId, Statements::DataSource *table, Statements::AlterColumn *column)
    : PlanNode(sessionId), table(table), column(column){}

  ExecutionResult PhysicalAlterColumn::Execute(CoreEngine::ExecutionContext& context){
    auto result = ExecutionResult(context);

    if (this->session == nullptr || this->session->user == nullptr)
      return ExecutionResult(
        Errors::RuntimeError::Error,
        Messages::FAILED_TO_RETRIEVE_USER_SESSION,
        context.GetAllocator()
      );

    const auto updates = DataStructures::PolymorphicArray<Value>::From(
      context.GetAllocator(),
      Value(this->column->type.size, context.GetAllocator(), static_cast<column_index_t>(CoreEngine::SysColumns::RecordSize)),
      Value(DataTypes::DateTime::Now(), context.GetAllocator(), static_cast<column_index_t>(CoreEngine::SysColumns::LastModifiedAt)),
      Value(this->session->user->name, context.GetAllocator(), static_cast<column_index_t>(CoreEngine::SysColumns::LastModifiedBy))
    );

    const auto _ = this->catalog->UpdateColumnById(context.GetAllocator(), this->column->columnId, updates);

    return result;
  }
};