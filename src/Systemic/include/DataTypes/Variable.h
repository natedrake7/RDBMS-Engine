#pragma once
#include "../Functions/StringFunctions.h"
#include "../../include/DataTypes/Value.h"

class Variable {
  Value value;
  DataType type;

  std::string name;
  std::string normalizedName;

  public:
    Variable() {
      this->type = DataType::Unknown;
    }

    Variable(Value& value, const DataType& type, std::string& name)
      : value(std::move(value)), type(type), name(std::move(name)) {
      this->normalizedName = Functions::String::NormalizeString(this->name);
    }

    Variable(Variable&& other)noexcept {
      this->value = std::move(other.value);
      this->type = other.type;
      this->name = std::move(other.name);
      this->normalizedName = std::move(other.normalizedName);

      other.value = Value::Null();
      other.type = DataType::Unknown;
      other.name = {};
      other.normalizedName = {};
    }

    Variable(const Variable& other) {
      this->value = other.value;
      this->type = other.type;
      this->name = other.name;
      this->normalizedName = other.normalizedName;
    }

    Variable& operator=(Variable&& other)noexcept {
      if (this == &other)
        return *this;

      this->value = std::move(other.value);
      this->type = other.type;
      this->name = std::move(other.name);
      this->normalizedName = std::move(other.normalizedName);

      other.value = Value::Null();
      other.name = {};
      other.normalizedName = {};
      other.type = DataType::Unknown;

      return *this;
    }

    Variable& operator=(const Variable& other) {
      this->value = other.value;
      this->type = other.type;
      this->name = other.name;

      return *this;
    }

    [[nodiscard]] const Value& GetValue() const { return this->value; }

    [[nodiscard]] Value& GetValue(){return this->value;}
    [[nodiscard]] const DataType& GetType() const { return this->type; }
    [[nodiscard]] const std::string& GetName() const { return this->name; }
    [[nodiscard]] const std::string& GetNormalizedName() const { return this->normalizedName; }

    void SetValue(Value& other) { this->value = std::move(other); }
    void SetValue(const Value& other) { this->value = other; }

    void SetType(const DataType& other) { this->type = other; }
    void SetName(std::string& other) {
      this->name = std::move(other);
      this->normalizedName = Functions::String::NormalizeString(this->name);
    }
};
