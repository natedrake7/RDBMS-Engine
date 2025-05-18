#pragma once
#include "../Visitor/Visitor.h"

namespace QueryPipeline {
  class Validator {

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
