#pragma once
#include <string>

class Expression {
  public:
    virtual ~Expression() = default;
};

class BinaryExpression final : public Expression {
  public:
    std::string operation;
    Expression* left;
    Expression* right;
    BinaryExpression(Expression* left, const std::string& operation, Expression* right) : left(left), operation(operation), right(right) {}
};

class IdentifierExpression final : public Expression {
  public:
    std::string name;
    explicit IdentifierExpression(const std::string& name) : name(name) {}
};

class StringExpression final : public Expression {
  public:
    std::string value;
    explicit StringExpression(const std::string& value) : value(value) {}
};

class NumberExpression final : public Expression {
public:
  std::string value;
  explicit NumberExpression(const std::string& value) : value(value) {}
};