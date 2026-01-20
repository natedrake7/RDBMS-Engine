#include "../../../DatabaseEngine/include/SystemDatabases/CatalogSchema.h"
#include "../../include/PhysicalPlan.h"
#include "../../../Server/include/Server.h"
#include "../../../Systemic/include/Functions/StringFunctions.h"
#include "../../../DatabaseEngine/include/SystemDatabases/SystemCatalog.h"

namespace QueryPipeline::PhysicalPlan{

  PhysicalAddColumn::PhysicalAddColumn(const DataTypes::Guid& sessionId, Statements::DataSource *table, Statements::NewColumn *column)
    : ExecutionNode(sessionId), table(table), column(column){}

  PhysicalAddColumn::~PhysicalAddColumn(){
    delete this->table;
    delete this->column;
  }

  ExecutionResult * PhysicalAddColumn::Execute(const DatabaseEngine::ExecutionProperties& properties){
    const auto columnType = ColumnTypesDictionary.Get(Functions::String::NormalizeString(this->column->type.name));

    if (this->session == nullptr || this->session->user == nullptr)
      return new ExecutionResult{
        Errors::RuntimeError::Error,
        "Failed to retrieve user session"
      };

    //if add occurs in a different index pos chaos ensues
    const auto columnResult =
        this->catalog->InsertColumnToMasterDb(
          properties,
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

      if (columnResult.code != Errors::RuntimeError::Ok) {
        auto* result = new ExecutionResult();
        result->code = columnResult.code;
        result->message = columnResult.message;
        return result;
      }

      const auto columnId = columnResult.primaryKey.AsInt(1);

      if (!this->column->defaultValue.IsNull()) {
        const auto value = this->column->defaultValue.AsString();

        const auto defaultValueResult =
            this->catalog->InsertDefaultValuesToMasterDb(
              properties,
              columnId,
              this->column->defaultValue
            );
     }

    const auto* db = this->server->UseDatabase(this->table->databaseId);

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
    tablePtr->RetrieveIdentityColumnById(columnId);

    tablePtr->PopulateColumn(this->column->index, this->column->defaultValue);
    tablePtr->RetrieveDefaultValuesFromCatalog();

    return nullptr;
  }

  PhysicalDropColumn::PhysicalDropColumn(const DataTypes::Guid& sessionId, Statements::DataSource *table, Statements::DropColumn *column)
    : ExecutionNode(sessionId), table(table), column(column){}

  PhysicalDropColumn::~PhysicalDropColumn(){
    delete this->table;
    delete this->column;
  }

  ExecutionResult * PhysicalDropColumn::Execute(const DatabaseEngine::ExecutionProperties& properties){
    auto* result = new ExecutionResult();

    if (this->session == nullptr || this->session->user == nullptr)
      return new ExecutionResult{
        Errors::RuntimeError::Error,
        "Failed to retrieve user session"
      };

    //update master db set isDeleted to 1
    //remove it from table, remove it from rows. Adjust column indexes if need be.
    const auto* db = this->server->UseDatabase(this->table->databaseId);

    auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    tablePtr->RemoveColumn(this->column->index);

    return result;
  }

  PhysicalRenameColumn::PhysicalRenameColumn(const DataTypes::Guid& sessionId, Statements::DataSource *table, Statements::RenameColumn *column)
  : ExecutionNode(sessionId), table(table), column(column){}

  PhysicalRenameColumn::~PhysicalRenameColumn(){
    delete this->table;
    delete this->column;
  }

  ExecutionResult * PhysicalRenameColumn::Execute(const DatabaseEngine::ExecutionProperties& properties){
    auto* result = new ExecutionResult();

    if (this->session == nullptr || this->session->user == nullptr)
      return new ExecutionResult{
        Errors::RuntimeError::Error,
        "Failed to retrieve user session"
      };

    const auto* db = this->server->UseDatabase(this->table->databaseId);

    const auto* tablePtr = db->OpenTable(this->table->ordinalPosition);

    const std::vector<Value> updates = {
      Value(this->column->newName.name, static_cast<column_index_t>(DatabaseEngine::SysColumns::Name)),
      Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(DatabaseEngine::SysColumns::LastModifiedAt)),
      Value(this->session->user->name, static_cast<column_index_t>(DatabaseEngine::SysColumns::LastModifiedBy)),
    };

    const auto _ = this->catalog->UpdateColumnById(this->column->columnId, updates);

    tablePtr->UpdateColumnName(this->column->ordinalPosition, this->column->newName.name);

    return result;
  }

  PhysicalAlterColumn::PhysicalAlterColumn(const DataTypes::Guid& sessionId, Statements::DataSource *table, Statements::AlterColumn *column)
    : ExecutionNode(sessionId), table(table), column(column){}

  PhysicalAlterColumn::~PhysicalAlterColumn(){
    delete this->table;
    delete this->column;
  }

  ExecutionResult * PhysicalAlterColumn::Execute(const DatabaseEngine::ExecutionProperties& properties){
    auto* result = new ExecutionResult();

    if (this->session == nullptr || this->session->user == nullptr)
      return new ExecutionResult{
        Errors::RuntimeError::Error,
        "Failed to retrieve user session"
      };

    const std::vector<Value> updates = {
      Value(this->column->type.size, static_cast<column_index_t>(DatabaseEngine::SysColumns::RecordSize)),
      Value(DataTypes::DateTime::Now(), static_cast<column_index_t>(DatabaseEngine::SysColumns::LastModifiedAt)),
      Value(this->session->user->name, static_cast<column_index_t>(DatabaseEngine::SysColumns::LastModifiedBy)),
    };

    const auto _ = this->catalog->UpdateColumnById(this->column->columnId, updates);

    return result;
  }
};