#pragma once
#include <Systemic/DataTypes/DataTypes.h>
#include <Systemic/DataTypes/Decimal.h>

namespace DataTypes{
    struct TypeInfo{
        data_size_t _maxLength;

        DataType _type;
        DecimalType _decimal;

        constexpr TypeInfo()
            : _maxLength(0), _type(DataType::Null), _decimal(){}

        explicit constexpr TypeInfo(
            const data_size_t maxLength,
            const DataType type,
            const DecimalType decimal = DecimalType()
        ):  _maxLength(maxLength), _type(type),
            _decimal(decimal){}

        [[nodiscard]] constexpr bool SameRepresentation(const TypeInfo& other) const{
            return this->_type == other._type && this->_decimal == other._decimal;
        }

        [[nodiscard]] friend constexpr bool operator==(const TypeInfo&, const TypeInfo&) = default;
    };
}
