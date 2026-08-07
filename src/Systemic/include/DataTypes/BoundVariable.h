#pragma once
#include "Value.h"
#include "Variable.h"

class BoundVariable{
    Value _inlineValue;

    std::string _name;
    std::string _normalizedName;
    std::vector<object_t> _externalData;

    DataType _type;

    public:
        explicit BoundVariable(const Variable& variable){
            this->SetValue(variable);

            const auto& name = variable.GetName();
            const auto& normalizedName = variable.GetNormalizedName();

            this->_name = std::string(name.Data(), name.Size());
            this->_normalizedName = std::string(normalizedName.Data(), normalizedName.Size());
        }

        BoundVariable() = default;
        BoundVariable(const BoundVariable& other) = default;
        BoundVariable(BoundVariable&& other)noexcept = default;
        BoundVariable& operator=(const BoundVariable& other) = default;
        BoundVariable& operator=(BoundVariable&& other)noexcept = default;

        void SetValue(const Variable& variable){
            const auto& value = variable.GetValue();
            if (value.IsInline())
                this->_inlineValue = value;
            else{
                this->_externalData.resize(value.Size());
                std::memcpy(this->_externalData.data(), value.Data(), value.Size());
            }

            this->_type = value.GetType();
        }

        [[nodiscard]] Value GetValue() const{
            if (Value::IsInline(this->_type))
                return this->_inlineValue;

            return Value::SessionValue(this->_externalData.data(), this->_externalData.size(), this->_type);
        }

        [[nodiscard]] const std::string& GetNormalizedName()const{
            return this->_normalizedName;
        }

        [[nodiscard]] DataType GetType() const{
            return this->_type;
        }
};
