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

  enum class ExpressionOperator {
    Equal = 0,
    NotEqual = 1,
    Greater = 2,
    GreaterEqual = 3,
    Less = 4,
    LessEqual = 5,
  };

  static Dictionary<std::string, ExpressionOperator> ExpressionOperatorsDictionary{
  { "=", ExpressionOperator::Equal },
  { "!=", ExpressionOperator::NotEqual },
  { "<>", ExpressionOperator::NotEqual },
  { ">", ExpressionOperator::Greater },
  { ">=", ExpressionOperator::GreaterEqual },
  { "<", ExpressionOperator::Less },
  { "<=", ExpressionOperator::LessEqual },
  };

  struct Expression {
    ExpressionType type;

    Expression* left;
    Expression* right;

    std::string column;
    ExpressionOperator operation;
    Field value;

    Constants::column_index_t columnIndex;

    static Expression Predicate(
      const std::string& column,
      const ExpressionOperator& operation,
      const Field& value);

    static Expression Predicate(
      const column_index_t & column,
      const ExpressionOperator& operation,
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