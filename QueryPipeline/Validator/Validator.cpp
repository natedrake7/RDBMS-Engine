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

}