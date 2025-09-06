#pragma once
#include <string>
#include "Expressions.Additional.h"
#include "../DataTypes/Value/Value.h"

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
  class Expression {
    public:
      std::string alias;
      virtual ~Expression() = default;
      Expression() = default;

      [[nodiscard]] virtual Value Evaluate(const DatabaseEngine::StorageTypes::Row* row) const = 0;
      [[nodiscard]] virtual DataType GetReturnType() const = 0;
  };

  class ColumnExpression final : public Expression {
    public:
      std::string name;
      std::string tableAlias;

      int32_t tableId;
      int32_t columnId;

      column_index_t columnIndex;
      DataType returnType;

      ColumnExpression(const std::string& name, const std::string& tableAlias);
      explicit ColumnExpression(const column_index_t& index);
      ~ColumnExpression()override = default;

      [[nodiscard]] Value Evaluate(const DatabaseEngine::StorageTypes::Row* row) const override;
      [[nodiscard]] DataType GetReturnType() const override;
  };

  class LiteralExpression final : public Expression {
    public:
      Value value;

      explicit LiteralExpression(const Value& value);
      ~LiteralExpression()override = default;

      [[nodiscard]] Value Evaluate(const DatabaseEngine::StorageTypes::Row* row) const override;
      [[nodiscard]] DataType GetReturnType() const override;
  };

  class BinaryExpression final : public Expression {
    public:
      Expression* left;
      Expression* right;

      ExpressionOperator operation;

      BinaryExpression(Expression* left, Expression* right, const ExpressionOperator& operation);
      ~BinaryExpression()override;

    //TODO : Implement Evaluate for BinaryExpression where left and rig*  are evaluated and Field Addition is implemented with data type coercion.
      [[nodiscard]] Value Evaluate(const DatabaseEngine::StorageTypes::Row* row) const override;
      [[nodiscard]]DataType GetReturnType() const override;
  };

  class FunctionExpression final : public Expression {

    public:
      std::vector<Expression*> arguments;

      Constants::FunctionType type;

      FunctionExpression(const Constants::FunctionType& type, std::vector<Expression*>& arguments);
      ~FunctionExpression()override;

      [[nodiscard]] Value Evaluate(const DatabaseEngine::StorageTypes::Row* row) const override;

      //String Function

      [[nodiscard]] static Value Concat(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Value Length(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Value TrimLeft(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Value TrimRight(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Value Trim(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Value AsciiValue(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Value Char(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Value CharIndex(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Value Lower(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Value Upper(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Value Replace(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Value Substr(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Value Left(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Value Right(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Value Reverse(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);
      [[nodiscard]] static Value Space(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row* row);


      //DateTime Functions
      [[nodiscard]] static Value GetDate(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row *row);

      //Guid Functions
      [[nodiscard]] static Value NewGuid(const FunctionExpression* expression, const DatabaseEngine::StorageTypes::Row *row);

      [[nodiscard]] bool ValidateNumberOfArguments(std::string& errorMessage)const;
      [[nodiscard]]DataType GetReturnType() const override;
  };

  class LogicalExpression final : public Expression{
      public:
      ExpressionType type;

      Expression* left;
      Expression* right;

      LogicalExpression(
        Expression *leftExpression,
        Expression *RightExpression,
        const ExpressionType &type
      );
      LogicalExpression();
      ~LogicalExpression()override;

    [[nodiscard]] Value Evaluate(const DatabaseEngine::StorageTypes::Row* row) const override;

    [[nodiscard]] DataType GetReturnType() const override;
};
}