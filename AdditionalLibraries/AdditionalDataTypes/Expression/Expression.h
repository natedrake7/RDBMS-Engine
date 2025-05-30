#pragma once
#include <string>
#include "../Field/Field.h"
#include "../../HashSet/HashSet.h"

namespace Expressions{
  enum class ExpressionType {
    And = 0,
    Or = 1,
    Predicate = 2
  };

  struct Expression {
    ExpressionType type;

    Expression* left;
    Expression* right;

    std::string column;
    std::string operation;
    Field value;

    Constants::column_index_t columnIndex;

    static Expression Predicate(
      const std::string& column,
      const std::string& operation,
      const Field& value);

    static Expression Predicate(
      const column_index_t & column,
      const std::string& operation,
      const Field& value);

    static Expression Logical(
      const ExpressionType& type,
      Expression* leftExpression,
      Expression* RightExpression);

    ~Expression();
    bool Validate(const Dictionary<string, Headers::ColumnHeader>& columnsDictionary);
    [[nodiscard]] bool IsComplex() const;
    void GetColumns(HashSet<column_index_t>& columnsSet)const;
  };
}