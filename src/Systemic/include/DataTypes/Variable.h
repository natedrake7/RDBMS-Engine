#pragma once
#include "String.h"
#include "../../include/DataTypes/Value.h"

class Variable {
    Value value;

    DataTypes::String name;
    DataTypes::String normalizedName;

    public:
        Variable()
            :   name(DataTypes::String::Null()),
                normalizedName(DataTypes::String::Null()){}

        Variable(Value& value, DataTypes::String& name)
            : value(std::move(value)), name(std::move(name)) {
            this->normalizedName = DataTypes::String::Normalize(this->name, this->name.GetAllocator());
        }

        Variable(Variable&& other)noexcept
        :   value(std::move(other.value)),
            name(std::move(other.name)),
            normalizedName(std::move(other.normalizedName)){

            other.value = Value::Null();
            other.name = DataTypes::String::Null();
            other.normalizedName = DataTypes::String::Null();
        }

        Variable(const Variable& other) = default;

        Variable& operator=(Variable&& other)noexcept{
            if (this == &other)
                return *this;

            this->value = std::move(other.value);
            this->name = std::move(other.name);
            this->normalizedName = std::move(other.normalizedName);

            other.value = Value::Null();
            other.name = DataTypes::String::Null();
            other.normalizedName = DataTypes::String::Null();

            return *this;
        }

        Variable& operator=(const Variable& other) = default;

        [[nodiscard]] const Value& GetValue() const { return this->value; }
        [[nodiscard]] Value& GetValue(){return this->value;}

        [[nodiscard]] const DataTypes::String& GetName() const { return this->name; }
        [[nodiscard]] const DataTypes::String& GetNormalizedName() const { return this->normalizedName; }

        [[nodiscard]] DataType GetType() const { return this->value.GetType(); }
        void SetType(const DataType type) { this->value.SetType(type); }

        void SetValue(Value& other) { this->value = std::move(other); }
        void SetValue(const Value& other) { this->value = other; }

        void SetName(DataTypes::String& other) {
            this->name = std::move(other);
            this->normalizedName = DataTypes::String::Normalize(this->name, this->name.GetAllocator());
        }
};
