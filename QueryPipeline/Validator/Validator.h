#pragma once
#include "../Visitor/Visitor.h"
#include "../../Server/Server.h"

namespace QueryPipeline {
  class Validator {

    static void Validate(Expression* expression, const Dictionary<std::string, Headers::ColumnHeader>& columnsDictionary);
    static void Validate(Field* field, const Headers::ColumnHeader& header);
    
    public:
      static Validator& Get() {
        static Validator validator;

        return validator;
      }

      static void Validate(const CreateDbStatement& statement);
      static void Validate(const DropDbStatement& statement);
      static void Validate(SelectStatement& statement);
      static void Validate(InsertStatement& statement);
  };
}
