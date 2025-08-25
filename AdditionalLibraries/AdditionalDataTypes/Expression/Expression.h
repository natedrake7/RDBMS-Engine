#pragma once
#include <string>
#include "../../Dictionary/Dictionary.h"
#include "../../HashSet/HashSet.h"
#include "../Field/Field.h"

namespace DatabaseEngine::StorageTypes {
  class Row;
}

namespace QueryPipeline::Statements {
  struct ColumnName {
    std::string name;
    std::string alias;

    int32_t tableId;
    int32_t columnId;
  };
}

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
    Add = 6,
    Subtract = 7,
    Multiply = 8,
    Divide = 9,
    Modulo = 10,
  };

  static Dictionary<std::string, ExpressionOperator> ExpressionOperatorsDictionary{
    { "=", ExpressionOperator::Equal },
    { "!=", ExpressionOperator::NotEqual },
    { "<>", ExpressionOperator::NotEqual },
    { ">", ExpressionOperator::Greater },
    { ">=", ExpressionOperator::GreaterEqual },
    { "<", ExpressionOperator::Less },
    { "<=", ExpressionOperator::LessEqual },
    { "+", ExpressionOperator::Add },
    { "-", ExpressionOperator::Subtract },
    { "*", ExpressionOperator::Multiply },
    { "/", ExpressionOperator::Divide },
    { "%", ExpressionOperator::Modulo },
  };

static Dictionary<std::string, Constants::FunctionType> FunctionTypeDictionary{
      {"getdate",   FunctionType::GetDate},
      {"newid",     FunctionType::NewGuid},
      {"concat",    FunctionType::Concat},
      {"length",    FunctionType::Length},
      {"ascii",     FunctionType::AsciiValue},
      {"char",      FunctionType::Char},
      {"charindex", FunctionType::CharIndex},
      {"lower",     FunctionType::Lower},
      {"upper",     FunctionType::Upper},
      {"trim",      FunctionType::Trim},
      {"trimleft",     FunctionType::TrimLeft},
      {"trimright",     FunctionType::TrimRight},
      {"replace",   FunctionType::Replace},
      {"substr",    FunctionType::Substr},
      {"left",      FunctionType::Left},
      {"right",     FunctionType::Right}
};

  class Expression {
    public:
      virtual ~Expression() = default;
      Expression() = default;

      [[nodiscard]] virtual Field Evaluate(const DatabaseEngine::StorageTypes::Row* row) const = 0;
  };

  class ColumnExpression final : public Expression {
    public:
      std::string name;
      std::string alias;

      int32_t tableId;
      int32_t columnId;

      column_index_t columnIndex;

      ColumnExpression(const std::string& name, const std::string& alias);
      ~ColumnExpression()override = default;

      [[nodiscard]] Field Evaluate(const DatabaseEngine::StorageTypes::Row* row) const override;
  };

  class LiteralExpression final : public Expression {
    public:
      Field value;

      explicit LiteralExpression(const Field& value);
      ~LiteralExpression()override = default;

      [[nodiscard]] Field Evaluate(const DatabaseEngine::StorageTypes::Row* row) const override;
  };

  class BinaryExpression final : public Expression {
    public:
      Expression* left;
      Expression* right;

      ExpressionOperator operation;

      BinaryExpression(Expression* left, Expression* right, const ExpressionOperator& operation);
      ~BinaryExpression()override;

    //TODO : Implement Evaluate for BinaryExpression where left and rig*  are evaluated and Field Addition is implemented with data type coercion.
      [[nodiscard]] Field Evaluate(const DatabaseEngine::StorageTypes::Row* row) const override;
  };

  class FunctionExpression final : public Expression {

    public:
      std::vector<Expression*> arguments;

      Constants::FunctionType type;

      FunctionExpression(const Constants::FunctionType& type, std::vector<Expression*>& arguments);
      ~FunctionExpression()override;

      [[nodiscard]] Field Evaluate(const DatabaseEngine::StorageTypes::Row* row) const override;

      //String Function

      [[nodiscard]] static Field Concat(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Field Length(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Field TrimLeft(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Field TrimRight(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Field Trim(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Field AsciiValue(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Field Char(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Field CharIndex(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Field Lower(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Field Upper(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Field Replace(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Field Substr(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Field Left(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Field Right(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);


      //DateTime Functions
      [[nodiscard]] static Field GetDate(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row *row);

      //Guid Functions
      [[nodiscard]] static Field NewGuid(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row *row);
  };

  class LogicalExpression final : public Expression{
    public:

    ExpressionType type;

    LogicalExpression* left;
    LogicalExpression* right;

    QueryPipeline::Statements::ColumnName column;

    ExpressionOperator operation;
    Field value;

    Constants::column_index_t columnIndex;

      LogicalExpression(
        const std::string& alias,
        const std::string& column,
        const ExpressionOperator& operation,
        const Field& value
        );

      LogicalExpression(
        const column_index_t & column,
        const ExpressionOperator& operation,
        const Field& value
        );

      LogicalExpression(
        const ExpressionType& type,
        LogicalExpression* leftExpression,
        LogicalExpression* RightExpression
      );

      static LogicalExpression* Predicate(
        const std::string& alias,
        const std::string& column,
        const ExpressionOperator& operation,
        const Field& value
      );

      static LogicalExpression* Predicate(
        const column_index_t & column,
        const ExpressionOperator& operation,
        const Field& value
        );

      static LogicalExpression* Logical(
        const ExpressionType& type,
        LogicalExpression* leftExpression,
        LogicalExpression* RightExpression
        );

      LogicalExpression() = default;
      ~LogicalExpression()override;

      bool Validate(const Dictionary<string, Headers::ColumnHeader>& columnsDictionary);
      [[nodiscard]] bool IsComplex() const;
      void GetColumns(HashSet<column_index_t>& columnsSet)const;

    [[nodiscard]] LogicalExpression* GetLeft()const;

    [[nodiscard]] LogicalExpression* GetRight() const;

    [[nodiscard]] Field Evaluate(const DatabaseEngine::StorageTypes::Row* row) const override;

    // [[nodiscard]] ExpressionType GetType() const;
    //
    // [[nodiscard]] QueryPipeline::Statements::ColumnName GetColumn() const;
    //
    // [[nodiscard]] ExpressionOperator GetOperation() const;
    //
    // [[nodiscard]] Field GetValue() const;
    //
    // [[nodiscard]] Constants::column_index_t GetColumnIndex() const;
};
}