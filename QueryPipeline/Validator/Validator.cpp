#include "Validator.h"
#include "../Visitor.h"
#include "../../Server/Server.h"

namespace QueryPipeline {

  void Validator::Validate(const CreateDbStatement& statement){
    if (!Server::ServerInstance::Get().DatabaseExists(statement.name))
      return;

    throw runtime_error("Database " + statement.name + " already exists");
  }

  void Validator::Validate(const DropDbStatement& statement){
    const auto database = Server::ServerInstance::Get().SelectDatabases(statement.name);

    if (database.name.empty())
      throw runtime_error("Cannot drop: " + statement.name + ". Database" + statement.name + " does not exist");

    if (database.isSystem)
      throw runtime_error("Cannot drop: a system database");
  }

  void  Validator::Validate(const SelectStatement& statement) {
    const auto tables = Server::ServerInstance::Get().SelectTables("masterDb");

    const Server::TableHeader* headerPtr = nullptr;
    for (const auto& table : tables) {
      if (table.name == statement.table) {
        headerPtr = &table;
        break;
      }
    }

    if (headerPtr == nullptr)
      throw runtime_error("Table " + statement.table + " does not exist");

    const auto columns = Server::ServerInstance::Get().SelectColumns("masterDb", headerPtr->name);

    vector<const Server::ColumnHeader*> columnsPtrs;
    for (const auto& selectColumn : statement.columns) {
      for (const auto& column : columns) {
          if (selectColumn == column.name) {
            columnsPtrs.push_back(&column);
            break;
          }
      }
    }

    if (columnsPtrs.empty())
      throw runtime_error("Columns not found");

    if (columnsPtrs.size() != statement.columns.size())
      throw runtime_error("Number of specified columns does not match actual number of columns");
  }

}