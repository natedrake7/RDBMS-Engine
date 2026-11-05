#pragma once
#include <Systemic/DataTypes/Value.h>
#include <Systemic/DataTypes/Variable.h>

class BoundVariable{
    Value _inlineValue;

    std::string _name;
    std::string _normalizedName;
    std::vector<object_t> _externalData;

    DataTypes::TypeInfo _typeInfo;

    public:
        explicit BoundVariable(const Variable& variable){
            this->SetValue(variable);

            const auto& name = variable.GetName();
            const auto& normalizedName = variable.GetNormalizedName();

            this->_name = std::string(name.Data(), name.Size());
            this->_normalizedName = std::string(normalizedName.Data(), normalizedName.Size());

            this->_typeInfo = variable.GetType();
        }

        BoundVariable() = default;
        BoundVariable(const BoundVariable& other) = delete;
        BoundVariable(BoundVariable&& other)noexcept = default;
        BoundVariable& operator=(const BoundVariable& other) = delete;
        BoundVariable& operator=(BoundVariable&& other)noexcept = default;

        void SetValue(const Variable& variable){
            const auto& value = variable.GetValue();

            if (value.IsInline())
                std::memcpy(&this->_inlineValue, &value, sizeof(Value));
            else{
                this->_externalData.resize(value.Size());
                std::memcpy(this->_externalData.data(), value.Data(), value.Size());
            }

            this->_typeInfo = value.GetType();
        }

        [[nodiscard]] Value GetValue() const{
            if (Value::IsInline(this->_typeInfo._type)){
                Value value;
                std::memcpy(&value, &this->_inlineValue, sizeof(value));
                return value;
            }

            return Value::SessionValue(this->_externalData.data(), this->_externalData.size(), this->_typeInfo);
        }

        [[nodiscard]] const std::string& GetNormalizedName()const{
            return this->_normalizedName;
        }

        [[nodiscard]] DataType GetType() const{
            return this->_typeInfo;
        }
};
