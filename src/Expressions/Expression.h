#pragma once
#include <string>
#include "Expressions.Additional.h"
#include "../Systemic/QueryResult/QueryResult.h"
#include "../Systemic/DataTypes/Value/Value.h"
#include "../Systemic/DataTypes/Variable/Variable.h"

namespace DatabaseEngine::StorageTypes {
  class Row;
}

namespace QueryPipeline::Statements {
  struct ColumnName {
    std::string name;
    std::string alias;

    int32_t tableId;
    int32_t columnId;
    Constants::column_index_t index;
    Constants::DataType returnType;
  };
}

namespace Expressions{
  struct EvaluationContext {
    enum class EvaluationContextType {
      Constant = 0,
      SingleRow = 1,
      MaterializedRow = 2,
      Join = 3,
      Aggregate = 4,
      Window = 5,
    };

    EvaluationContextType type;

    const DatabaseEngine::StorageTypes::Row* row;
    const DatabaseEngine::StorageTypes::Row* outerRow;
    const DatabaseEngine::StorageTypes::Row* innerRow;

    QueryResult materializedRow;

    const Dictionary<std::string, Variable>* variables;

    EvaluationContext();
    explicit EvaluationContext(const EvaluationContextType& type, const Dictionary<std::string, Variable>* variables);
    explicit EvaluationContext(const DatabaseEngine::StorageTypes::Row* row);
    explicit EvaluationContext(const QueryResult& row);
    EvaluationContext(const DatabaseEngine::StorageTypes::Row* outerRow, const DatabaseEngine::StorageTypes::Row* innerRow);
  };

  class Expression {
    public:
      std::string name;
      virtual ~Expression() = default;
      Expression() = default;

      [[nodiscard]] virtual Value Evaluate(const EvaluationContext& context) const = 0;
      [[nodiscard]] virtual DataType GetReturnType() const = 0;
  };

  class ColumnExpression final : public Expression {
    public:
      std::string alias;
      std::string tableAlias;

      int32_t tableId;
      int32_t columnId;

      column_index_t index;
      DataType returnType;
      block_size_t size;

      ColumnExpression(const std::string& name, const std::string& tableAlias);
      explicit ColumnExpression(const column_index_t& index);
      ~ColumnExpression()override = default;

      [[nodiscard]] Value Evaluate(const EvaluationContext& context)const override;
      [[nodiscard]] DataType GetReturnType() const override;
      [[nodiscard]] bool HasTableAlias() const;
  };

  class LiteralExpression final : public Expression {
    public:
      Value value;

      explicit LiteralExpression(const Value& value);
      explicit LiteralExpression(Value& value);
      ~LiteralExpression()override = default;

      [[nodiscard]] Value Evaluate(const EvaluationContext& context)const override;
      [[nodiscard]] DataType GetReturnType() const override;
  };

  class BinaryExpression final : public Expression {
    public:
      Expressions::Expression* left;
      Expression* right;

      ExpressionOperator operation;

      BinaryExpression(Expression* left, Expression* right, const ExpressionOperator& operation);
      ~BinaryExpression()override;

      [[nodiscard]] Value Evaluate(const EvaluationContext& context)const override;
      [[nodiscard]] DataType GetReturnType() const override;
  };

  class FunctionExpression final : public Expression {

    [[nodiscard]] bool ValidateUnlimitedArgumentTypes(const FunctionInfo& info, std::string& errorMessage)const;
    [[nodiscard]] bool ValidateArgumentTypes(const FunctionInfo& info, std::string& errorMessage)const;
    [[nodiscard]] static bool ValidateReturnType(
      const FunctionInfo& info,
      std::string& errorMessage,
      const DataType& expectedType,
      const DataType& returnType,
      const int& index
    );

    public:
      std::vector<Expression*> arguments;

      Constants::FunctionType type;

      FunctionExpression(const Constants::FunctionType& type, std::vector<Expression*>& arguments);
      ~FunctionExpression()override;

      [[nodiscard]] Value Evaluate(const EvaluationContext& context)const override;

      //String Function

      [[nodiscard]] static Value Concat(const std::vector<Value>& arguments);
      [[nodiscard]] static Value Length(const std::vector<Value>& arguments);
      [[nodiscard]] static Value TrimLeft(const std::vector<Value>& arguments);
      [[nodiscard]] static Value TrimRight(const std::vector<Value>& arguments);
      [[nodiscard]] static Value Trim(const std::vector<Value>& arguments);
      [[nodiscard]] static Value AsciiValue(const std::vector<Value>& arguments);
      [[nodiscard]] static Value Char(const std::vector<Value>& arguments);
      [[nodiscard]] static Value CharIndex(const std::vector<Value>& arguments);
      [[nodiscard]] static Value Lower(const std::vector<Value>& arguments);
      [[nodiscard]] static Value Upper(const std::vector<Value>& arguments);
      [[nodiscard]] static Value Replace(const std::vector<Value>& arguments);
      [[nodiscard]] static Value Substr(const std::vector<Value>& arguments);
      [[nodiscard]] static Value Left(const std::vector<Value>& arguments);
      [[nodiscard]] static Value Right(const std::vector<Value>& arguments);
      [[nodiscard]] static Value Reverse(const std::vector<Value>& arguments);
      [[nodiscard]] static Value Space(const std::vector<Value>& arguments);

      //DateTime Functions
      [[nodiscard]] static Value GetDate(const std::vector<Value>& arguments);

      //Guid Functions
      [[nodiscard]] static Value NewGuid(const std::vector<Value>& arguments);

      [[nodiscard]] bool ValidateNumberOfArguments(std::string& errorMessage)const;
      [[nodiscard]] DataType GetReturnType() const override;
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

        [[nodiscard]] Value Evaluate(const EvaluationContext& context)const override;
        [[nodiscard]] DataType GetReturnType() const override;
  };

  class VariableExpression final : public Expression {

    public:
      std::string name;
      std::string normalizedName;
      DataType type;

      explicit VariableExpression(const std::string& name);

      [[nodiscard]]Value Evaluate(const EvaluationContext &context) const override;
      [[nodiscard]]DataType GetReturnType() const override;

  };
}