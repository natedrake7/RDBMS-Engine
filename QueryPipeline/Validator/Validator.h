#pragma once
#include "../Visitor/Visitor.h"
#include "../../Server/Server.h"

namespace QueryPipeline {
  class Validator {

    static void Validate(Expression* expression, const Dictionary<std::string, Server::ColumnHeader>& columnsDictionary);

    public:
      static Validator& Get() {
        static Validator validator;

        return validator;
      }

      static void Validate(const CreateDbStatement& statement);
      static void Validate(const DropDbStatement& statement);
      static void Validate(const SelectStatement& statement);
  };
}
