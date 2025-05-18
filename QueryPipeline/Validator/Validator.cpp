#include "Validator.h"
#include "../Visitor/Visitor.h"
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
    //get only table needed. if joins occur get them all
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

    const auto columnsDict = Server::ServerInstance::Get().SelectColumnsToDictionary("masterDb", headerPtr->name);

    for (const auto& selectColumn : statement.columns) {
      if (columnsDict.Contains(selectColumn))
        continue;
      
      throw runtime_error("Column " + selectColumn + " does not exist");
    }

    if (statement.where.expression == nullptr)
      return;

    Validator::Validate(statement.where.expression, columnsDict);
  }

  void Validator::Validate(Expression *expression, const Dictionary<string, Server::ColumnHeader>& columnsDictionary){

    if (expression->type != ExpressionType::Predicate
      && expression->left != nullptr
      && expression->right != nullptr) {
      Validator::Validate(expression->left, columnsDictionary);
      Validator::Validate(expression->right, columnsDictionary);

      return;
    }

    Server::ColumnHeader header;

    if (!columnsDictionary.TryGetValue(expression->column, header))
      throw runtime_error("Column " + expression->column + " does not exist");

    expression->columnIndex = header.tablePosition;
  }

}