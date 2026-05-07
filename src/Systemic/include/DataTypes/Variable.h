#pragma once
#include "String.h"
#include "../../include/DataTypes/Value.h"

class Variable {
  Value value;
  DataType type;

  DataTypes::String name;
  DataTypes::String normalizedName;

  public:
    Variable()
        : value(nullptr), type(DataType::Null){}

    Variable(Value& value, const DataType& type, DataTypes::String& name)
      : value(std::move(value)), type(type), name(std::move(name)) {
      this->normalizedName = DataTypes::String::Normalize(this->name);
    }

    Variable(Variable&& other)noexcept
    :   value(std::move(other.value)),
        type(other.type),
        name(std::move(other.name)),
        normalizedName(std::move(other.normalizedName)){

      other.value = Value::Null(nullptr);
      other.type = DataType::Null;
      other.name = DataTypes::String::Null();
      other.normalizedName = DataTypes::String::Null();
    }

    Variable(const Variable& other)
        :   value(other.value),
            type(other.type),
            name(other.name),
            normalizedName(other.normalizedName){}

    Variable& operator=(Variable&& other)noexcept{
      if (this == &other)
        return *this;

      this->value = std::move(other.value);
      this->type = other.type;
      this->name = std::move(other.name);
      this->normalizedName = std::move(other.normalizedName);

      other.value = Value::Null(nullptr);
      other.name = DataTypes::String::Null();
      other.normalizedName = DataTypes::String::Null();
      other.type = DataType::Null;

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
    [[nodiscard]] const DataTypes::String& GetName() const { return this->name; }
    [[nodiscard]] const DataTypes::String& GetNormalizedName() const { return this->normalizedName; }

    void SetValue(Value& other) { this->value = std::move(other); }
    void SetValue(const Value& other) { this->value = other; }

    void SetType(const DataType& other) { this->type = other; }
    void SetName(DataTypes::String& other) {
      this->name = std::move(other);
      this->normalizedName = DataTypes::String::Normalize(this->name);
    }
};
